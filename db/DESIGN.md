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

`MaxOpenConns(1)` on `rwDB` serializes writes on a single connection, but it does not guarantee that connection lives forever: `database/sql` can discard it — for example, after the context of a transaction is cancelled or times out — and open a replacement. `PRAGMA wal_autocheckpoint=0`, which disables SQLite's automatic checkpointer, is per-connection, so a replacement would otherwise arrive with `wal_autocheckpoint=1000` and SQLite would silently start checkpointing behind the snapshot subsystem's back. The PRAGMA is therefore executed by the `ConnectionFactory` which opens every connection (see below), so every connection — including any replacement — has autocheckpointing disabled.

`Open` also forces the WAL files into existence at open time when WAL mode is requested. SQLite normally creates `-wal` and `-shm` lazily on the first write; the package executes `BEGIN IMMEDIATE; ROLLBACK` on a single pinned connection to materialize them, then runs a `wal_checkpoint(TRUNCATE)`. This matters because external read-only connections (and startup checks elsewhere in rqlite) need to be able to see the WAL files even on a brand-new database.

`Close` closes `roDB` first and `rwDB` second. The order matters in the rare case where a driver does want close-time cleanup (see `CheckpointDriver` below) — that cleanup must run on the writer connection, which has to outlive any reader.

## The Driver Factory

rqlite needs connections configured differently for different scenarios — a steady-state configuration that disables checkpoint-on-close, a cleanup configuration that *enables* it (used by `CheckpointRemove`), one that turns on foreign keys, and one for each set of loaded extensions. A `Driver` is a plain description of that configuration (`DriverConfig`), built by the `DefaultDriver` / `CheckpointDriver` / `ForeignKeyDriver` / `NewDriver` / `NewDriverFromConfig` factory functions. Nothing is registered with `database/sql`, so drivers have no names and can be created freely.

Each connection pool is given its own `ConnectionFactory` (`connection.go`), built from a DSN and the `Driver`'s configuration, and the pools are opened with `sql.OpenDB`. A `ConnectionFactory` implements the standard library's `driver.Connector`: `database/sql` calls its `Connect` method every time it needs a connection — the first one, an extra read-only one, or a replacement for one it discarded — so `Connect` is the single place where a connection is opened and configured. It opens the connection with the DSN, then sets or skips `DBConfigNoCkptOnClose`, disables autocheckpointing, optionally enables foreign keys, optionally installs query tracing, and finally applies the factory's settings (see below). `DBConfigNoCkptOnClose` is a method exposed by rqlite's go-sqlite3 fork (`github.com/rqlite/go-sqlite3`) that turns off SQLite's "checkpoint when the last connection closes" behavior for the lifetime of the connection. The default driver disables it because of v9's design rule:

> When rqlite closes the database — including on hard process exit — the WAL must remain intact.

If SQLite checkpointed and removed the WAL on close, a crash between "last write" and "next snapshot" would erase data that has not yet made it into the SQLite file. By keeping the WAL around, restart logic can replay or discard it deliberately rather than have the choice forced by SQLite. `CheckpointDriver` exists for the rare case (`CheckpointRemove`, used during WAL replay) where rqlite *does* want SQLite to do its normal close-time cleanup.

A connection's configuration therefore comes from three places, all applied by the factory: the DSN and the `Driver` configuration, which are fixed when the factory is created, and the factory's *settings*, which can change while the database is open. The settings are the busy timeout, the synchronous mode, and the preupdate, update, commit, and rollback hooks. Changing a setting records it in the factory and applies it to every connection the factory has opened that is still open; because the factory holds the settings, every connection opened afterwards — including a replacement — is configured identically. To know which connections are open, the factory hands out `Connection` values: a `Connection` is a SQLite connection that holds a pointer back to its factory, and its `Close` removes it from the factory's set before closing the underlying connection. Code that reaches the driver connection through `conn.Raw` therefore receives a `*Connection`.

A `ConnectionFactory` is agnostic of whether its connections are read-write or read-only; that is decided by the DSN alone. `DB` holds one factory per pool (`rwFactory`, `roFactory`) and never configures a connection directly: `SetBusyTimeout`, `SetSynchronousMode`, and the `Register*Hook` methods all call the relevant factory. A setting is applied to a connection wherever it is in its lifecycle, without borrowing it from the pool. The one consequence is that SQLite refuses to change the synchronous mode of a connection which has a transaction open; in that case `SetSynchronousMode` returns an error and that connection keeps its previous mode.

The fork also adds the preupdate, update, and commit hook registration methods (`RegisterPreUpdateHook`, `RegisterUpdateHook`, `RegisterCommitHook`) that the CDC integration depends on. Without the fork, neither the close-time control nor the preupdate hook would be available.

## SwappableDB

`SwappableDB` is a thin synchronization wrapper around `*DB`. Every method takes an `RWMutex` read lock, delegates to the inner `*DB`, and unlocks. The single method that takes the write lock is `Swap`, which closes the current database, removes the on-disk files, renames a replacement file into place, reopens the database, and recreates the `CheckpointManager` (which has to start over because the new database has fresh WAL salt values).

The wrapper exists because callers — the FSM, the HTTP layer, the auto-backup uploader, the CDC collator — all hold a long-lived reference to "the database" and must not see a closed `*DB` while a swap is in flight. `Swap` happens during Restore (after a snapshot is loaded), Load (operator-initiated database replacement), and recovery flows. Without the wrapper, every caller would need its own coordination, or the Store would have to broadcast a "stop using the DB" signal across the codebase.

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

Change Data Capture is wired through SQLite's preupdate, commit, and rollback hooks. The hooks are registered on the single `rwDB` connection — the same one that does all writes — via `RegisterPreUpdateHook`, `RegisterCommitHook`, and `RegisterRollbackHook`. Hooks are per-connection, so they are settings of the read-write `ConnectionFactory` (see The Driver Factory): registering, replacing, or removing a hook updates the factory, which installs it on the current read-write connection and on every read-write connection it opens afterwards. A replacement connection therefore keeps delivering events. `RegisterPreUpdateHook` is also where raw SQLite preupdate data is converted into a `CDCEvent` protobuf (including `normalizeCDCValues`, the row-data conversion), so hook targets only ever see normalized events.

`CDCCollator` (in `cdc_collator.go`) is the in-package hook target. It does no I/O and has no channel: it simply gathers the events produced by one database change entry — in practice, one Raft log entry — so the caller can retrieve them once the entry has been fully applied. It keeps two lists:

- **`pending`** — events from the transaction currently in progress. The preupdate hook appends to it.
- **`events`** — events from transactions that have committed since the last `Reset`.

The commit hook moves `pending` onto the end of `events`; the rollback hook discards `pending` and leaves `events` alone. The split exists because a single change entry can contain several autocommit transactions — a bulk request without an enclosing transaction commits once per statement — so one entry can see many commits, and a rollback of a later statement must not throw away the events of earlier statements that did commit. An earlier design delivered events from inside the commit hook, one group per commit; collecting across commits and handing over the whole set afterwards is what lets every request in a bulk request contribute its events to the entry's single event group.

The caller's protocol is `Reset` before applying the entry, then `Events` after execution has finished. `Reset` drops both lists by releasing the storage rather than truncating it, so a slice previously returned by `Events` is never modified and can be handed off without copying. The collator is not safe for concurrent use and does not need to be: the hooks run on the goroutine executing the SQL, and the caller reads the result only after that execution returns.

Column names are resolved in the commit hook, through the `ColumnsNameProvider` passed to `NewCDCCollator` (the Store passes its `SwappableDB`), and cached per table for the duration of that one commit. They are resolved at commit rather than when `Events` is called because a later transaction in the same entry may alter the table, and each event must carry the column names that were in effect when its transaction committed. A failed lookup is recorded in the event's `Error` field rather than failing the commit. The commit hook always returns true: CDC bookkeeping must never cause a transaction to be rolled back.

The rollback hook fires when SQLite rolls back an autocommit statement or a transaction managed by the DB API. Events must not be discarded merely because a statement returns an error: SQLite's `FAIL` conflict resolution can retain and commit changes made before the error. SQLite does not notify this hook about statement rollback within an explicitly opened SQL transaction or `ROLLBACK TO` a savepoint; CDC does not yet account for those partial rollbacks.

The Store owns the collator's lifecycle and everything downstream of it. It creates the collator when it opens, registers the three hooks lazily on the first log entry applied with CDC enabled, calls `Reset` before applying each entry, and after applying it wraps any collected events in a group stamped with the entry's Raft index and sends that group to the CDC service. Because the hooks live on the writer connection, a `Swap` silently removes them along with the old connection; the Store re-registers them on the next entry after a Load.

## Boundary Checks

A few small utilities in `state.go` exist to catch user input that would break invariants the rest of the package depends on:

- **`IsBreakingPragma`** — a regex-free check covering `journal_mode`, `wal_autocheckpoint`, `wal_checkpoint`, `synchronous`, and `query_only`. The Store rejects any user statement matching one of these before it reaches Raft. The list is deliberately narrow: only PRAGMAs that would invalidate rqlite's coordination with SQLite. Tuning PRAGMAs like `cache_size` are fine and pass through unchanged.
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

Loaded extensions are part of the driver configuration (`NewDriver(extensions, chkpt)` or `DriverConfig.Extensions`), and are loaded into every connection the resulting factories open. `ValidateExtension` is a sanity check: it opens an in-memory database using the stock SQLite driver and tries to load the extension, returning an error if the extension is incompatible. The extensions themselves are managed by the `db/extensions` subpackage, which is just an on-disk store of extension `.so` files.

## Key Design Decisions and Trade-offs

- **rqlite owns the WAL lifecycle.** SQLite's autocheckpointer is disabled (`PRAGMA wal_autocheckpoint=0`), close-time checkpointing is disabled (`DBConfigNoCkptOnClose`), and both are applied by the `ConnectionFactory` as it opens each connection, so neither can be silently re-enabled by a fresh connection. This is what lets the snapshot subsystem reason about what is in the WAL at any moment.

- **Synchronous=OFF in steady state, FULL during checkpoint.** Performance comes from `SYNCHRONOUS=OFF` (Raft provides durability across the cluster), but the checkpoint is the moment when SQLite assumes "what is in the file is durable" — so the checkpoint path temporarily switches to FULL and switches back.

- **No checkpoint on close, even on graceful shutdown.** Surviving the WAL is more important than a tidy on-disk file. On the next start, the rest of rqlite decides what to do with the leftover WAL. A tidy-shutdown variant (`CheckpointDriver`, used by `CheckpointRemove`) exists for the few code paths that actually want SQLite's old behavior.

- **Bounded checkpointing replaces wait-forever (v10).** The v10 `CheckpointManager` ensures a slow reader cannot stall checkpointing — and therefore cannot stall Raft log truncation. The cost is the bookkeeping for the partial-checkpoint case, including the WAL-salt comparison that distinguishes "WAL was reset" from "WAL was appended" between attempts. The benefit is that write throughput no longer collapses when one node has a slow reader.

- **Two connection pools, one writer.** Read concurrency comes from the read-only pool; write coordination comes from holding the writer pool to one connection, and everything set on that connection is held by its `ConnectionFactory` so that it survives the connection being replaced. This mirrors SQLite's underlying single-writer model.

- **CDC events are collected per change entry, not delivered per commit.** The `CDCCollator` accumulates events across every commit in an entry and the Store retrieves them once the entry is applied. This keeps delivery, back-pressure, and Raft-index stamping in the Store, leaves the hook path free of I/O, and means a multi-statement entry yields one complete event group. The collator stays in `db/` because it is defined entirely by SQLite's hook semantics — what commits, what rolls back, and when column names are valid.

- **WAL parsing is byte-level, not via SQLite.** Compacting the WAL requires reading frames and selecting the latest per page across transaction boundaries, which SQLite does not expose. The `wal/` subpackage reimplements just enough of the WAL format (derived from LiteFS) to do this, with checksum and salt validation.

- **SwappableDB instead of broadcasting "stop".** A central RW-mutex around the database handle is simpler than coordinating every caller during a swap, and the `RLock`-on-each-call cost is negligible relative to SQLite work.
