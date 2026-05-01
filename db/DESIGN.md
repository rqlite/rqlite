# DB Package Design

## Purpose

The `db` package is rqlite's wrapper around SQLite. It owns the SQLite database file, the WAL, the connection pool, and the lifecycle of every PRAGMA that affects how rqlite coordinates with SQLite. Above this package the rest of rqlite is largely SQLite-agnostic — the Store talks to a `SwappableDB`, the snapshot subsystem talks to a `CheckpointManager`, CDC consumers receive normalized `CDCEvent` protobufs, and so on.

The package's job is to take SQLite's defaults — which assume a single application has full control over the file — and reshape them so that SQLite is safe to run *under Raft consensus*: writes are serialized, the WAL never disappears unexpectedly, and the checkpoint cadence is dictated by the snapshot subsystem rather than by SQLite's autocheckpointer.

## Background: How rqlite Uses SQLite

rqlite runs SQLite in WAL journal mode with `SYNCHRONOUS=OFF`. This is much faster than the SQLite default but normally unsafe — a crash mid-write can leave the file inconsistent. rqlite is comfortable with this because every committed write has already been durably stored in the Raft log on a quorum of nodes. If a node crashes and its SQLite file is corrupt, the cluster has the data and the node can be rebuilt from the snapshot store and the log.

This shifts a lot of responsibility into the `db` package. SQLite's normal "I'll checkpoint when the WAL grows too big" and "I'll checkpoint when you close the database" behaviors become actively *harmful*: the snapshot store needs to know exactly what is in the WAL at any given moment, and a process restart must leave the WAL intact so the rest of rqlite can decide whether to replay or discard it. The package disables both behaviors and exposes its own checkpoint primitives instead.

## Two Connections, One Writer

`Open` creates two `*sql.DB` handles for the same file:

- `rwDB` — read/write, with `MaxOpenConns(1)`.
- `roDB` — read-only DSN (`mode=ro`), with the standard idle pool.

The two-handle split exists because SQLite's WAL mode allows readers to run concurrently with a writer. Sending all reads through `roDB` lets `Query` and `RequestWithContext` execute alongside an in-flight write without serializing against the writer.

`MaxOpenConns(1)` on `rwDB` is the load-bearing detail. Right after opening, the package executes `PRAGMA wal_autocheckpoint=0` to disable SQLite's automatic checkpointer. That PRAGMA is per-connection. If `database/sql` were free to open additional read/write connections from its pool, each fresh connection would arrive with `wal_autocheckpoint=1000` re-enabled, and SQLite would silently start checkpointing behind the snapshot subsystem's back. Holding the writer pool to exactly one connection is what guarantees the disable-autocheckpoint setting is real and permanent. The struct comment "Key to ensure a new connection doesn't enable checkpointing" marks this on the field.

`Open` also forces the WAL files into existence at open time when WAL mode is requested. SQLite normally creates `-wal` and `-shm` lazily on the first write; the package executes `BEGIN IMMEDIATE; ROLLBACK` on a single pinned connection to materialize them, then runs a `wal_checkpoint(TRUNCATE)`. This matters because external read-only connections (and startup checks elsewhere in rqlite) need to be able to see the WAL files even on a brand-new database.

`Close` closes `roDB` first and `rwDB` second. The order matters in the rare case where a driver does want close-time cleanup (see `CheckpointDriver` below) — that cleanup must run on the writer connection, which has to outlive any reader.

## The Driver Factory

SQLite's Go binding requires a driver to be registered with `database/sql` by name; once registered, the same name cannot be reused. rqlite needs different driver instances for different scenarios — a steady-state driver that disables checkpoint-on-close, a cleanup driver that *enables* it (used by `CheckpointRemove`), one that turns on foreign keys, and dynamically-named drivers for each set of loaded extensions. The `Driver` type and the `DefaultDriver` / `CheckpointDriver` / `ForeignKeyDriver` / `NewDriver` factory functions wrap that registration, each guarded by a `sync.Once` so repeated calls are safe.

The drivers differ only in their `ConnectHook`. The default and checkpoint hooks set or skip `DBConfigNoCkptOnClose`, a method exposed by rqlite's go-sqlite3 fork (`github.com/rqlite/go-sqlite3`) that turns off SQLite's "checkpoint when the last connection closes" behavior for the lifetime of the connection. The default driver disables it because of v9's design rule:

> When rqlite closes the database — including on hard process exit — the WAL must remain intact.

If SQLite checkpointed and removed the WAL on close, a crash between "last write" and "next snapshot" would erase data that has not yet made it into the SQLite file. By keeping the WAL around, restart logic can replay or discard it deliberately rather than have the choice forced by SQLite. `CheckpointDriver` exists for the rare case (`CheckpointRemove`, used during WAL replay) where rqlite *does* want SQLite to do its normal close-time cleanup.

The fork also adds the preupdate, update, and commit hook registration methods (`RegisterPreUpdateHook`, `RegisterUpdateHook`, `RegisterCommitHook`) that the CDC integration depends on. Without the fork, neither the close-time control nor the preupdate hook would be available.

## SwappableDB

`SwappableDB` is a thin synchronization wrapper around `*DB`. Every method takes an `RWMutex` read lock, delegates to the inner `*DB`, and unlocks. The single method that takes the write lock is `Swap`, which closes the current database, removes the on-disk files, renames a replacement file into place, reopens the database, and recreates the `CheckpointManager` (which has to start over because the new database has fresh WAL salt values).

The wrapper exists because callers — the FSM, the HTTP layer, the auto-backup uploader, the CDC streamer — all hold a long-lived reference to "the database" and must not see a closed `*DB` while a swap is in flight. `Swap` happens during Restore (after a snapshot is loaded), Load (operator-initiated database replacement), and recovery flows. Without the wrapper, every caller would need its own coordination, or the Store would have to broadcast a "stop using the DB" signal across the codebase.

## Checkpointing

Checkpointing — moving committed WAL frames into the database file and (in TRUNCATE mode) shrinking the WAL back to zero — is the single most coordination-sensitive operation in the package. Three things happen during a checkpoint:

1. The WAL is read and frames are written into the database file. This requires no readers to be holding a snapshot of the WAL state past the frames being moved. SQLite's busy timeout governs how long the checkpoint will wait for readers to release.
2. The WAL file is truncated (in TRUNCATE mode). This requires *exclusive* access to the WAL.
3. The checkpoint may run partially and stop, leaving a coherent intermediate state that the next attempt has to reason about.

Both the `DB.Checkpoint*` family of methods and the `CheckpointManager` switch SQLite to `SYNCHRONOUS=FULL` for the duration of the checkpoint, then restore the prior mode. This is the same fsync strategy described on rqlite.io/docs/design: pages that have just landed in the database file must be durable on disk before the snapshot subsystem treats them as committed.

### CheckpointManager (the v10 motivation)

Prior to v10, checkpointing was effectively "wait forever for readers to release, then truncate." A persistently-blocked reader could stall checkpointing indefinitely, which would in turn stall Raft's log truncation and bring write throughput to a halt across the cluster. The `CheckpointManager`, introduced in v10, replaces the wait-forever loop with a bounded, recoverable strategy:

1. **Compact the WAL into the snapshot writer first.** `wal.NewCompactingFrameScanner` reads the WAL and emits only the latest version of each page (subject to transaction boundaries), so the bytes that go into the snapshot are the minimum needed to bring a follower to the same state. This work is pure read — no checkpoint lock is held — so a slow reader does not block it.
2. **Then attempt a TRUNCATE checkpoint with a bounded timeout.** Whatever happens, the manager updates its bookkeeping based on the three possible outcomes encoded in `CheckpointMeta`:
   - **Truncated** (`Code == 0`): the WAL is reset and the manager clears its state.
   - **Pages partially moved** (`pnCkpt < pnLog`): readers are still holding old frames. The next attempt can resume from the same WAL position; the manager returns `ErrDatabaseCheckpointBusy` (a `RetryableError`) and stays in its previous state.
   - **All pages moved but file not truncated** (`pnCkpt == pnLog`): SQLite is between "I moved everything" and "I freed the file." The next attempt has to handle two possible futures, and which future occurred is only knowable from outside.

The third case is the subtle one. When the next checkpoint runs, SQLite may have either kept appending new frames at the *end* of the WAL (so the saved `nextFrameIdx` is still the right place to resume) or *reset* the WAL and started writing new frames at the beginning (so `nextFrameIdx` is now nonsense). The manager can tell which happened by reading the salt values from the WAL header before it scans: if the salt has changed, the WAL was reset, the bookkeeping is discarded, and the scan starts at frame zero. The `WALReset` field on `CheckpointManagerMeta` exposes this transition for callers that care (mostly stats and tests).

The net effect is that no reader, no matter how slow, can prevent forward progress. Each checkpoint either truncates, makes partial progress that the next attempt continues from, or returns retryable-busy without touching state. Snapshot creation can keep up with write traffic.

## The wal/ Subpackage

`db/wal` is a byte-level reader and writer for the SQLite WAL file format. It exists because `CheckpointManager.Checkpoint` needs to *compact* the WAL — produce a smaller, equivalent WAL containing only the latest committed version of each page — before handing it to the snapshot subsystem. SQLite's own checkpoint primitive moves frames into the database file, not into a new WAL file, so this work has to be done outside SQLite.

The reader (`wal.Reader`, derived from the LiteFS reader) walks WAL frames, verifies salt and (optionally) checksum, and stops at the first invalid frame — which is also how it detects the end of the valid prefix in a WAL that may contain trailing garbage from interrupted writes.

Two iterators sit on top of the reader:

- **`FullScanner`** emits every frame in order, used when an exact copy is needed.
- **`CompactingFrameScanner`** scans frames into a `(pgno → latest committed frame)` map, then emits the survivors in file-offset order. It honors transaction boundaries: frames between a non-committing frame and its commit are buffered in a temporary map and only merged into the survivor set once the commit lands. An open transaction at the end of the WAL produces `ErrOpenTransaction` rather than silently dropping the in-progress frames.

The compacting scanner has a `fullScan` mode that verifies every frame's checksum (required when starting from frame zero) and a fast mode that skips the page-data read entirely and trusts SQLite's salt for validity. The fast mode is what the `CheckpointManager` uses on the live WAL — SQLite has just produced it, so it is trusted. Frame-data buffers are reused across `Next()` calls (`pageBuf`), so scanning a multi-gigabyte WAL does not allocate per frame.

`wal.Writer` consumes any `WALIterator` and writes a freshly-checksummed WAL stream to an `io.Writer`. The compaction round-trip — `CompactingFrameScanner` into `Writer` into the snapshot's staging directory — is the entire incremental-snapshot WAL pipeline.

## CDC Integration

Change Data Capture is wired through SQLite's preupdate and commit hooks. The hooks are registered on the single `rwDB` connection — the same one that does all writes — via `RegisterPreUpdateHook` and `RegisterCommitHook`. Hook installation goes through `conn.Raw` because the registration methods are on the SQLite-specific connection type, not on the abstract `database/sql` connection.

`CDCStreamer` (in `cdc.go`) is the in-package hook target. The preupdate hook appends an event to a pending group; the commit hook stamps the group with the current time, looks up column names per table (cached for the duration of the group), and non-blockingly sends the group to a channel. If the channel is full the event is dropped and a stat increments — by design, CDC is at-least-once on the receiver side, not back-pressuring the writer. The `CDCStreamer` lives in `db/` rather than in `cdc/` because its hook installation, its per-statement reset, and its lifecycle are all driven from inside the database write path; it is tightly bound to the SQLite hook plumbing and would only become harder to reason about if moved further away.

The Store owns the streamer's lifecycle but delegates the row-data conversion (`normalizeCDCValues`) to this package, which is also where the SQLite-to-rqlite type mapping for query results lives.

## Boundary Checks

A few small utilities in `state.go` exist to catch user input that would break invariants the rest of the package depends on:

- **`BreakingPragmas`** — a regex set covering `journal_mode`, `wal_autocheckpoint`, `wal_checkpoint`, and `synchronous`. The Store rejects any user statement matching one of these before it reaches Raft. The list is deliberately narrow: only PRAGMAs that would invalidate rqlite's coordination with SQLite. Tuning PRAGMAs like `cache_size` are fine and pass through unchanged.
- **`IsValidSQLiteFile` / `IsValidSQLiteData`** — magic-byte checks on file headers, used before swapping a file in or accepting a restore payload.
- **`IsValidSQLiteWALFile` / `IsValidSQLiteWALData`** — magic and version checks for WAL files, used before WAL replay.
- **`IsWALModeEnabled` / `IsDELETEModeEnabled`** — read bytes 18–19 of the SQLite header to detect the journal mode without opening the file.

These all guard against operator mistakes (wrong file passed to `/db/load`, mismatched format from a restore source) without the noise of trying to open the file and parse the resulting SQLite error.

## Backup, Serialize, Dump, ReplayWAL

Several utilities expose SQLite's data in different shapes for callers that need it:

- **`Backup`** uses SQLite's online backup API (`copyDatabaseConnection` → `sqlite.Backup.Step`) to copy a consistent snapshot to a new file while writes remain in flight. The destination is set to `journal_mode=DELETE` so the result is a portable, single-file SQLite database. With `vacuum=true`, the destination is vacuumed afterwards.
- **`Serialize`** returns the database as a byte slice — for WAL-mode databases it first round-trips through `Backup` to a temporary file, since serializing a live WAL would produce inconsistent bytes.
- **`Dump`** writes a SQL text rendering of the schema and data, suitable for restore through `/db/load?fmt=sql`.
- **`ReplayWAL`** (in `state.go`) takes one or more WAL files, renames each into place, and runs `CheckpointRemove` to fold it into the database. Core to compaction processes that rqlite runs.

## Extensions

Loaded extensions are baked into a per-set driver via `NewDriver(name, extensions, chkpt)`. Each unique combination of extensions registers its own driver name, and the registration is *not* `sync.Once`-guarded — `NewDriver` deliberately panics if the name is reused, since two drivers with the same name would conflict. The Store builds a unique driver name per process invocation. `ValidateExtension` is a sanity check: it registers a throwaway driver, opens an in-memory database, and tries to load the extension, returning an error if the extension is incompatible. The extensions themselves are managed by the `db/extensions` subpackage, which is just an on-disk store of extension `.so` files.

## Key Design Decisions and Trade-offs

- **rqlite owns the WAL lifecycle.** SQLite's autocheckpointer is disabled (`PRAGMA wal_autocheckpoint=0`), close-time checkpointing is disabled (`DBConfigNoCkptOnClose`), and `MaxOpenConns(1)` on the writer pool prevents either from being silently re-enabled by a fresh connection. This is what lets the snapshot subsystem reason about what is in the WAL at any moment.

- **Synchronous=OFF in steady state, FULL during checkpoint.** Performance comes from `SYNCHRONOUS=OFF` (Raft provides durability across the cluster), but the checkpoint is the moment when SQLite assumes "what is in the file is durable" — so the checkpoint path temporarily switches to FULL and switches back.

- **No checkpoint on close, even on graceful shutdown.** Surviving the WAL is more important than a tidy on-disk file. On the next start, the rest of rqlite decides what to do with the leftover WAL. A tidy-shutdown variant (`CheckpointDriver`, used by `CheckpointRemove`) exists for the few code paths that actually want SQLite's old behavior.

- **Bounded checkpointing replaces wait-forever (v10).** The v10 `CheckpointManager` ensures a slow reader cannot stall checkpointing — and therefore cannot stall Raft log truncation. The cost is the bookkeeping for the partial-checkpoint case, including the WAL-salt comparison that distinguishes "WAL was reset" from "WAL was appended" between attempts. The benefit is that write throughput no longer collapses when one node has a slow reader.

- **Two connection pools, one writer.** Read concurrency comes from the read-only pool; write coordination (and PRAGMA persistence) comes from holding the writer pool to one connection. This mirrors SQLite's underlying single-writer model.

- **CDC streamer lives next to the SQLite hooks.** The streamer's lifecycle is driven by the same write-path code that registers and tears down the preupdate and commit hooks. Separating it from the hook plumbing would create an awkward two-way dependency for no real benefit.

- **WAL parsing is byte-level, not via SQLite.** Compacting the WAL requires reading frames and selecting the latest per page across transaction boundaries, which SQLite does not expose. The `wal/` subpackage reimplements just enough of the WAL format (derived from LiteFS) to do this, with checksum and salt validation.

- **SwappableDB instead of broadcasting "stop".** A central RW-mutex around the database handle is simpler than coordinating every caller during a swap, and the `RLock`-on-each-call cost is negligible relative to SQLite work.
