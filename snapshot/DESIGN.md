# Snapshot Package Design

## Purpose

The `snapshot` package implements rqlite's custom Raft snapshot store. It satisfies the `raft.SnapshotStore` interface from [hashicorp/raft](https://github.com/hashicorp/raft) and is responsible for persisting, organizing, and serving the database state that Raft needs for log truncation, node recovery, and cluster membership changes (bringing new nodes up to speed).

In a Raft-based system the snapshot store is the ground truth. If the Raft log is truncated up to index N, the snapshot store must be able to produce a complete database state at index N. Everything in this package exists to maintain that invariant.

## Background: Why Incremental Snapshots?

Prior to rqlite 8.0, every Raft snapshot was a full copy of the SQLite database. This worked well for small databases but became progressively more expensive as data grew — snapshot cost scaled with total database size, not with the volume of recent writes. For multi-gigabyte databases, the memory and I/O overhead became prohibitive.

rqlite 8.0 introduced **incremental snapshots** based on SQLite's Write-Ahead Log (WAL). Instead of copying the entire database, rqlite snapshots only the WAL file — a record of changes since the last snapshot. The cost of snapshotting now depends only on how much data was written since the last snapshot, independent of total database size.

This means the snapshot store must manage two kinds of snapshots and be able to combine them to reconstruct database state at any snapshotted point in time.

## Core Concepts

### Snapshot Types

A snapshot is a directory under the store root. The directory name is the snapshot ID (derived from Raft term, index, and a timestamp). Each directory contains a `meta.json` file with Raft metadata and one or more data files.

The snapshot type is determined by what files are present:

- **Full snapshot**: Contains `data.db` (a valid SQLite database). May also contain zero or more `.wal` files. A full snapshot is always the base from which database state is reconstructed.

- **Incremental snapshot**: Contains one or more `.wal` files but no `data.db`. An incremental snapshot is meaningful only in combination with an earlier full snapshot.

Every data file (DB or WAL) has a `.crc32` sidecar file containing its CRC32 checksum. These sidecars are used for integrity verification without recomputing checksums.

### Snapshot Ordering and Resolution

Snapshots are ordered by `(Term, Index, ID)` from oldest to newest. The `SnapshotSet` type encapsulates this ordering and provides query methods for selecting, filtering, and partitioning snapshots.

The key operation is **resolution**: given a snapshot ID, determine the complete set of files needed to reconstruct database state at that point. `ResolveFiles` implements this:

- For a **full snapshot**, it returns the full's DB file and any WAL files within that snapshot directory.
- For an **incremental snapshot**, it walks backward to find the nearest full snapshot, then collects the full's DB file plus all WAL files from the full snapshot and every incremental snapshot up to and including the requested one.

The invariant is: after the newest full snapshot, all subsequent snapshots must be incremental. `ValidateIncrementalChain` enforces this.

### ChecksummedFile

The `ChecksummedFile` type pairs a file path with its CRC32 checksum, loaded from the sidecar file on disk. It avoids redundant checksum computation when opening existing snapshots — the checksum was already verified and written when the snapshot was created.

## Architecture

The package is organized around four main operations: **creating** snapshots (sinks), **reading** snapshots (streamers), **maintaining** the store (reaping), and **restoring** from snapshots.

### Creating Snapshots: Sink

The `Sink` type implements `raft.SnapshotSink`. It is the entry point for Raft when persisting a new snapshot. The sink is **adaptive** — it handles both full and incremental snapshots through a single interface.

When Raft calls `Write`, the sink first accumulates bytes until it can decode a protobuf `SnapshotHeader` (a 4-byte big-endian length prefix followed by the marshaled header). The header's payload type determines what happens next:

- **FullSnapshot header**: The sink delegates to `FullSink`, which streams the incoming bytes into `data.db` and any WAL files described by the header. On `Close`, it verifies each file's CRC32 against the header (catching network corruption) and writes `.crc32` sidecar files.

- **IncrementalFileSnapshot header**: No data follows the header. Instead, the header contains a local filesystem path to a WAL staging directory. On `Close`, the sink atomically moves that directory's contents into the snapshot directory. This is an optimization for the local-node case — WAL data is already on disk, so there is no need to stream it through Raft's snapshot transport.

The sink writes to a temporary directory (`<id>.tmp`) and atomically renames it into place on successful `Close`. If `Close` fails during an incremental snapshot, the sink calls a fatal function that terminates the process. This is a deliberate design choice: an interrupted incremental snapshot breaks the WAL chain, and a hard restart (which recovers from the last good snapshot) is preferable to requiring a full snapshot, which would pause write traffic for longer.

### Reading Snapshots: Streamer

Two streamer types produce the byte stream that Raft reads when it needs to send a snapshot to another node or restore local state:

- **SnapshotStreamer**: Implements `io.ReadCloser`. Concatenates the framed header, the DB file, and any WAL files into a single sequential byte stream using `io.MultiReader`. Two constructors exist:
  - `NewSnapshotStreamer` computes CRC32 checksums on the fly (used when creating a new full snapshot from the live database).
  - `NewChecksummedSnapshotStreamer` uses pre-loaded checksums from sidecar files (used when opening an existing snapshot from the store — avoids redundant I/O).

- **SnapshotPathStreamer**: Streams only the framed header (no file data follows). Used for incremental snapshots where the WAL directory path is embedded in the header and the receiver moves the files directly.

### Maintaining the Store: Reaping

Over time, incremental snapshots accumulate. **Reaping** consolidates them into a single up-to-date full snapshot and removes obsolete snapshots. It is the only operation that deletes snapshot data.

Reaping is triggered automatically by a background goroutine (`reapLoop`) when the snapshot count exceeds a configurable threshold, or manually via `Store.Reap()`. The process:

1. Find the newest full snapshot and partition the store around it.
2. Delete all snapshots older than the full snapshot (they are superseded).
3. If there are WAL files (from the full snapshot itself or from subsequent incrementals), checkpoint them all into the full snapshot's database file.
4. Recompute the DB file's CRC32 sidecar.
5. Remove incremental snapshot directories.
6. Write new metadata reflecting the newest snapshot's term and index.
7. Rename the full snapshot directory to a new ID.

#### Crash Safety: Plan-Execute Pattern

Reaping is destructive and must run to completion. The package uses a **plan-execute pattern**: the entire sequence of operations is serialized as a JSON plan file (`REAP_PLAN`) before any mutations begin. If the process crashes mid-execution, `Store.check()` on startup detects the plan file and re-executes it. Every operation in the plan (rename, remove, checkpoint, etc.) is idempotent, so re-execution is safe.

The `plan` sub-package implements this pattern with a `Plan` type (ordered list of `Operation` values), a `Visitor` interface, and an `Executor` that performs the actual filesystem and SQLite operations.

#### Concurrency: MRSW Lock

The store uses a multi-reader single-writer (MRSW) lock. Creating or reading snapshots requires only a read lock — multiple snapshots can be created and read concurrently. Reaping requires the write lock and waits for all active readers to finish. The `LockingSink` and `LockingStreamer` wrappers ensure the lock is held for the duration of the operation and released on `Close`.

### Restoring from Snapshots

`Restore` reads a protobuf-framed snapshot stream (the same format produced by `SnapshotStreamer`) and writes the resulting SQLite database to a destination path. If the stream contains WAL files, they are extracted to temporary files and checkpointed into the database. This is used when a node receives a snapshot from the leader and needs to rebuild its local state.

## WAL Staging

The `StagingDir` type manages a temporary directory where WAL files are staged before being packaged into a snapshot. The store's FSM writes compacted WAL data into the staging directory via `WALWriter`, which computes a running CRC32 checksum and writes the `.crc32` sidecar on `Close`.

A key property of the staging directory is **persistence across failed snapshots**. If `Persist` fails after the WAL has been written and closed (but before the snapshot is finalized), the staging directory retains the WAL files. The next snapshot attempt can pick them up rather than requiring a full snapshot. This significantly reduces the frequency of full snapshots compared to earlier versions of rqlite.

## Wire Format

Snapshot data is framed using a simple protocol defined in `snapshot/proto/snapshot.proto`:

```
[4 bytes: header length (big-endian uint32)]
[N bytes: marshaled SnapshotHeader protobuf]
[DB file bytes (if FullSnapshot)]
[WAL file bytes, in order (if FullSnapshot with WALs)]
```

The `SnapshotHeader` protobuf uses a `oneof` payload:

- **FullSnapshot**: Contains a `Header` for the DB file (size + CRC32) and repeated `Header` entries for WAL files.
- **IncrementalFileSnapshot**: Contains only a `wal_dir_path` string — no file data follows.

## Store Lifecycle

1. **Startup** (`NewStore`): Creates the store directory, runs `check()` to clean up temporary directories and resume interrupted reap plans, and starts the reaper goroutine.

2. **Creating snapshots** (`Create`): Acquires a read lock, creates a `Sink` in a temporary directory, returns a `LockingSink` to Raft.

3. **Opening snapshots** (`Open`): Acquires a read lock, resolves the requested snapshot ID to its constituent files via `ResolveFiles`, creates a `SnapshotStreamer`, and returns a `LockingStreamer`. The Raft metadata's `Size` field is set to the total stream length so Raft can track transfer progress.

4. **Listing snapshots** (`List`): Returns only the most recent snapshot's metadata (per the `raft.SnapshotStore` contract). `ListAll` returns all snapshots.

5. **Reaping**: Triggered by the reaper goroutine after a successful snapshot persist (`LockingSink.Close` signals the reaper). Also available as `Store.Reap()` for manual invocation.

6. **Shutdown** (`Close`): Stops the reaper goroutine and waits for it to exit.

## Full Snapshot Trigger

The `FULL_NEEDED` flag file signals that the next snapshot must be a full snapshot rather than an incremental one. This is set when no snapshots exist (first snapshot), or when the system detects the incremental chain is broken and cannot be continued. The `Sink` checks this flag when it receives an `IncrementalFileSnapshot` header and rejects the snapshot if a full is needed.

## Upgrade Support

The package includes upgrade paths from older snapshot formats (`Upgrade7To8` and `Upgrade8To10`). These use the same plan-execute pattern as reaping for crash safety. The v7 format stored the database as a gzip-compressed blob; v8 stored it as a bare `.db` file at the store root; v10 stores it inside the snapshot directory as `data.db` with a CRC32 sidecar. Upgrades are run once on startup and remove the old format directory after successful migration.

## Key Design Decisions and Trade-offs

- **Incremental snapshots trade simplicity for efficiency.** A full-only system would be simpler but impractical for large databases. The incremental approach adds complexity (chain validation, resolution, reaping) but makes snapshot cost proportional to write volume rather than database size.

- **Hard exit on incremental sink failure.** Rather than setting `FULL_NEEDED` and continuing (which would pause writes during the next full snapshot), the process exits. On restart, the node recovers from its last good snapshot and rejoins the cluster. This minimizes write disruption at the cost of a brief node outage.

- **CRC32 sidecars over embedded checksums.** Storing checksums in separate files allows verification without parsing snapshot metadata and enables the `ChecksummedFile` abstraction for avoiding redundant computation. The trade-off is additional filesystem entries.

- **Plan-execute for destructive operations.** Serializing the operation sequence to disk before execution adds I/O overhead but guarantees crash-safe, idempotent recovery. This is essential because a partially-reaped store could leave the system in an unrecoverable state.

- **Local file moves for incremental snapshots.** The `IncrementalFileSnapshot` path avoids serializing WAL data through Raft's snapshot transport when the data is already on the local filesystem. This is a significant performance optimization for the common case.

- **WAL staging directory for resilience.** Keeping staged WAL files across failed snapshot attempts avoids falling back to full snapshots unnecessarily. This was a key improvement in v10 that reduces the frequency of expensive full snapshots.
