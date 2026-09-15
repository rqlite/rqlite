# Store Package Design

## Purpose

The `store` package is the central glue of rqlite. It owns the Raft instance, the SQLite database, the snapshot store, the Bolt log store, the network transport, and all the bookkeeping that ties those subsystems together. Every external entry point — HTTP, the cluster service, CDC, the auto-backup uploader — eventually goes through `*store.Store`.

The package's job is to take the simple-sounding contract "a SQL request goes in, a result comes out, replicated and durable" and make it actually true on top of Raft consensus, SQLite, a custom snapshot store, and a SQLite driver with hooks. Most of the complexity in the package is coordinating those subsystems — making them interact without stepping on each other.

## The FSM Bridge

Hashicorp Raft talks to the application through the `raft.FSM` interface (`Apply`, `Snapshot`, `Restore`). rqlite's `FSM` type (in `fsm.go`) is intentionally a thin wrapper that delegates each method directly to the parent `Store`:

```go
func (f *FSM) Apply(l *raft.Log) any              { return f.s.fsmApply(l) }
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) { return f.s.fsmSnapshot() }
func (f *FSM) Restore(rc io.ReadCloser) error      { return f.s.fsmRestore(rc) }
```

Why not put the FSM logic in its own type with its own state? Because all three FSM operations need to coordinate with things the Store already owns. `Apply` touches the CDC streamer, the throttler, and several index/term variables. `Snapshot` drives the SQLite checkpointer, the WAL staging directory, and the auto-vacuum/optimize machinery. `Restore` swaps the live SQLite database and re-primes the CDC hooks. Splitting these into a separate type would just produce a back-reference to the Store anyway. The thin-wrapper pattern makes the coupling explicit rather than awkwardly hidden.

### CommandProcessor

The actual log-entry-to-database dispatch lives in `CommandProcessor` (`command_processor.go`), not in the Store. It is a small stateless object that takes a serialized `proto.Command`, picks the right SQLite operation (Execute / Query / ExecuteQuery / Load / LoadChunk / Noop), and runs it against a `*sql.SwappableDB`. The Store's `fsmApply` calls into it; nothing else in the steady state does.

The reason it is its own type is `RecoverNode` (`state.go`). When manual quorum recovery runs, it needs to replay log entries against a temporary database without spinning up a full Store, so the dispatch logic has to be reusable. Keeping `CommandProcessor` decoupled from `Store` is what makes that possible. This is the one place in the package where decoupling actually pays off — and it pays off because there is a real second consumer.

## Request Path

Three public entry points handle SQL requests:

- **`Execute`** — write-only path. Always goes through Raft consensus. Leader-only. Returns the row counts and the Raft index of the committed entry.
- **`Query`** — read-only path. Behavior depends on the consistency level (below).
- **`Request`** — unified path that accepts a mix of reads and writes. If the request contains *any* writes, or asks for `STRONG`, it goes through Raft. If it is all reads at a non-strong level, it falls through to a direct SQLite query — the same code path as `Query`.

Each entry point checks readiness, runs the throttler (`s.throttler.Delay(ctx)`), validates that no disallowed pragmas are present, and then either calls `s.raft.Apply` with a marshaled `proto.Command` or hits SQLite directly. Commands are marshaled by `RequestMarshaler`, which compresses the payload above a configurable threshold; the compressed flag rides in the `proto.Command` envelope and `CommandProcessor` decompresses transparently.

### Consistency levels

`Query` and `Request` accept a consistency level via the `proto.ConsistencyLevel` enum. From cheapest to most expensive:

- **`NONE`** — direct local read, no leader check. Optionally bounded by a `freshness` window: if the leader has not been heard from in too long (or, with `strict`, the local FSM is behind the leader's known commit index), the read fails with `ErrStaleRead`. The freshness check is in `IsStaleRead` (`state.go`), and depends on `appendedAtTime` — the leader-clock time captured from `AppendEntries` RPCs by `NodeTransport`.
- **`WEAK`** — direct local read, but the node must be the leader. Cheap and the default for most reads. Non-leaders return `ErrNotLeader`.
- **`LINEARIZABLE`** — guaranteed to reflect everything committed before the request started. Implemented by `waitForLinearizableRead` using the technique from §6.4 of the Raft dissertation: capture the commit index, verify leadership via a quorum heartbeat round-trip, confirm the term has not changed, then wait for the local FSM to reach that commit index. There is one wrinkle: if no Strong read has yet gone through the Raft log in the current term, the linearizable read upgrades itself to a Strong read so the leader can establish that it has actually committed something in this term. Without that upgrade, a freshly-elected leader could return a result based on a stale committed index. The code comment links to the dissertation thread.
- **`STRONG`** — the read is itself sent through `raft.Apply` as a `COMMAND_TYPE_QUERY` entry. Slowest, but the read is processed by every node's FSM in log order, which makes it trivially linearizable. Mostly used by tests and as the upgrade target for the linearizable path.

`AUTO` resolves to `WEAK` on voters and `NONE` on non-voters.

## Snapshot Orchestration

> This section assumes familiarity with `snapshot/DESIGN.md`. Review that doc first if any of `Sink`, `StagingDir`, `WALWriter`, `FULL_NEEDED`, or the `snapshotTypeController` interface are unfamiliar.

`fsmSnapshot` is the most coordination-heavy function in the package. Raft calls it when it has decided to snapshot — either time-triggered, log-entry-count triggered, WAL-size triggered, or via an explicit `Store.Snapshot` call. Its job is to produce a `raft.FSMSnapshot` that, when its `Persist` method is later invoked, will write a coherent snapshot into the snapshot store. A lot has to happen in between.

The high-level flow:

1. **Notify snapshot-sync subscribers.** Anything registered via `RegisterSnapshotSync` (CDC is the current real consumer) gets a chance to flush in-flight state before the snapshot starts. The Store sends each subscriber a sync channel and waits for them to close it.
2. **Acquire the snapshot CAS gate** (`snapshotCAS`). This is mutually exclusive with backups and with the startup clean-snapshot CRC verification — only one of {snapshot, backup, CRC verify} runs at a time, system-wide.
3. **Switch SQLite to `synchronous=FULL`.** Snapshots assume that every page that has reached the database file is durable; FULL synchronous mode guarantees that. The deferred cleanup restores `synchronous=OFF` so steady-state writes do not pay the cost.
4. **Run auto-vacuum and auto-optimize if their interval has elapsed.** This is deliberately scheduled inside the snapshot path because it is the one moment the database is guaranteed not to be receiving writes.
5. **Decide full vs incremental.** `snapshotDueNext()` combines two signals: what `snapshotStore.DueNext()` says (the `FULL_NEEDED` flag), and `dbModified()` — whether the SQLite file has been touched outside the Store's control since the last snapshot. Either signal forces a full.
6. **Build the FSM snapshot.**
   - **Full path**: checkpoint any outstanding WAL into the database file, then construct a `snapshot.SnapshotStreamer` that streams the live `db.sqlite` file framed by a `FullSnapshot` header.
   - **Incremental path**: write a compacted WAL into the staging directory via `StagingDir.CreateWAL` and `WALWriter`, run the SQLite checkpointer with that writer as its sink, mark the WAL valid by closing the writer, and construct a `SnapshotPathStreamer` that emits only an `IncrementalFileSnapshot` header pointing at the staging directory. The actual WAL bytes never travel through Raft's snapshot transport — the snapshot Sink moves the directory directly when it receives the header.
7. **Wrap in `FSMSnapshot`** (the local type, not the snapshot package's). This wrapper carries:
   - `Type` — Full or Incremental, for logging and stats.
   - `Finalizer` — runs after a successful `Persist`. The finalizer is `createSnapshotFingerprint`, which writes the clean-snapshot file (see the next section).
   - `OnRelease` — called after `Release` with `(invoked, succeeded)` flags. This is where the `FULL_NEEDED` recovery logic lives. If the snapshot was invoked, *failed*, and the WAL staging directory is no longer present (so the WAL data has been moved into the snapshot tmp dir and lost when that was cleaned up), then the chain is broken and the next snapshot must be a full one — `OnRelease` calls `snapshotStore.SetDueNext(Full)`. If the snapshot was never invoked at all (Raft decided it did not need it), the staging directory is left intact and the next snapshot picks up where this one left off, packaging the leftover WAL together with whatever is newly compacted.

A subtle point worth knowing: a full snapshot's `Persist` does the actual streaming of the SQLite file via `StateReader` (which wraps the streamer with a progress monitor). An incremental's `Persist` writes only a few hundred bytes — the framed header — and the snapshot Sink does the rest of the work asynchronously as part of its own `Close`. So the cost of an incremental on the Store side is dominated by the WAL compaction in step 6, not by `Persist`.

## Clean-Snapshot Fingerprint

When a node restarts, the obvious behavior would be: "let Raft replay the latest snapshot through `FSM.Restore`, which extracts the database file and swaps it in". For multi-gigabyte databases this is slow and unnecessary — the SQLite file is already on disk from the previous run, and if it is byte-identical to what the snapshot would produce, there is no point in extracting anything.

The clean-snapshot fingerprint is the optimization that makes this skip safe. After every successful snapshot, `createSnapshotFingerprint` writes a small JSON file (`clean_snapshot`) to the Raft directory containing:

- The SQLite file's modification time
- Its size
- Its CRC32

On `Open`, the Store checks for this file. If it exists, the SQLite file's mtime and size still match, and the snapshot store has at least one snapshot, the Store sets `raftConfig.NoSnapshotRestoreOnStart = true` — telling Raft to bring up the FSM without re-restoring from the snapshot. The local SQLite file is reused as-is.

The CRC32 check is more expensive (it has to read the whole file), so it runs asynchronously in a goroutine after `Open` returns. While that goroutine is running, snapshotting is blocked by `snapshotCAS` so the file is not modified mid-CRC. If the CRC mismatches, the process logs a fatal error and exits — the node is in an unrecoverable state and the cluster's redundancy is supposed to provide the fault tolerance.

Any code path that invalidates the SQLite file relative to the snapshot store — `fsmRestore`, `RecoverNode`, `Load`, manual swap — removes the fingerprint file before swapping. On the next restart, the absence of the fingerprint forces a normal snapshot-restore. The optimization is opt-in by design: only a *clean* shutdown after a successful snapshot enables the fast path.

## State and Locks

The Store tracks a fair amount of fine-grained state — Raft index variants for the FSM and the database, commit-index targets for `WaitFor*` callers, term tracking for the linearizable-read protocol, last-modification time for the SQLite file, leader-clock time for stale-read detection, and so on. Each variable exists because some external query or internal handshake needs it. The details are best read from the `Store` struct definition rather than enumerated here.

Similarly there are several mutex / CAS gates with overlapping responsibilities (`snapshotCAS` for snapshot/backup/CRC mutual exclusion, `snapshotSync` for snapshot pre-notification, `readerMu` for the rare cases where reads must be blocked, plus per-feature mutexes for CDC, leader observers, and notification state). Each lock is documented inline at its point of use; the names make their scopes clear in context.

## Leader Changes and Observers

A long-running goroutine (`observe()`, started in `Open`) drains events from a `raft.Observer` channel and routes them. Two event types matter:

- **`LeaderObservation`** — fired by Raft whenever leadership changes. The Store fans this out to every channel registered via `RegisterLeaderChange` (the http and cluster layers use this) and also calls `selfLeaderChange` for the auto-restore logic.
- **`FailedHeartbeatObservation`** — fired when a follower is not responding. If the failing node has been silent for longer than `ReapTimeout` (or `ReapReadOnlyTimeout` for non-voters), and the timeouts are non-zero, the leader removes it from the configuration. This is how rqlite handles "node went away and is never coming back" without manual intervention.

`selfLeaderChange` is also where **auto-restore** happens. If `SetRestorePath` was called before `Open`, the Store remembers a SQLite file to be restored on first leader transition. The first time this node becomes leader, `installRestore` reads the file and routes it through the Load path (so the restore goes through Raft and lands on every node). It is one-shot: subsequent leader transitions do not trigger it again, and the restore file is removed from disk after the attempt — successful or not. If a different node becomes leader first, this node's restore is silently skipped. Auto-restore is meant to seed a new cluster, not to recover an existing one.

## Cluster Formation

A node can become part of a cluster through four distinct paths, and the choice has implications for what state is on disk before `Open` is called.

- **`Bootstrap(servers...)`** — single shot. Calls `raft.BootstrapCluster` with an explicit configuration. Used when the operator knows the full cluster up front and wants to start it as a unit. The configuration must be identical on every node, and only one node should call `Bootstrap` (or the call must converge to identical configurations across all nodes).

- **`Notify(nr)` with `BootstrapExpect`** — the discovery-driven path. Each node calls `Notify` on every other node it learns about (via DNS, Consul, etcd, or a static list). The Store accumulates the notifying set in `notifyingNodes` and, when the count reaches `BootstrapExpect`, triggers a single `BootstrapCluster` call with the full set. `notifyMu` and the `bootstrapped` flag ensure exactly one node performs the bootstrap. Read-only nodes deliberately disable this path (they require `BootstrapExpect=0`) so they never accidentally bootstrap a cluster. `Notify` also resolves the remote address before accepting the notification — DNS races during cluster startup can otherwise cause a node to be added to the configuration with an unresolvable address.

- **`Join(jr)`** — sent by a new node to a current leader, asking to be added to the existing configuration as either a voter or a non-voter. Before adding, the leader checks whether a server with the same ID *or* address already exists; if so, the existing entry is removed first. This handles the case where a node is restarted with a new ID at the same address, or a new node reuses an old ID. As with `Notify`, the address is resolved before the join is accepted.

- **`RecoverNode` via `peers.json`** — manual quorum recovery, the escape hatch for when the cluster has lost quorum and cannot heal itself. If a `peers.json` file exists in the Raft directory at `Open` time, the Store reads the configuration from it, replays the existing snapshot store and Raft log into a temporary database via `CommandProcessor`, writes a fresh recovery snapshot containing that configuration, compacts the log so any leftover configuration entries do not interfere, and renames `peers.json` → `peers.info` to mark the recovery done. It is destructive: any pre-existing SQLite file is invalidated by clearing the clean-snapshot fingerprint, and the next normal startup will restore from the recovery snapshot. This is operator-triggered, not automatic.

## Other Background Activity

A handful of smaller pieces run alongside the main request and snapshot paths:

- **WAL-size snapshot ticker** (`runWALSnapshotting`) — periodic check (jittered) of the SQLite WAL file. If it exceeds `SnapshotThresholdWALSize`, the ticker triggers a snapshot. This is in addition to Raft's own snapshot triggering, which is based on log-entry count. WAL-size triggering catches the case of many small writes that do not produce enough log entries to hit Raft's threshold but do bloat the WAL.
- **Throttler** — `throttler.Throttler` (in the `throttler/` sub-package) gates write requests by sleeping callers for an increasing delay when the system is under pressure. External code can `Signal()` to ramp up and `Release()` to wind down; without signals, it idles back to zero delay after a timeout. `Execute` and `Request` call `s.throttler.Delay(ctx)` before doing any expensive work.
- **CDC integration** — `EnableCDC` / `DisableCDC` set up the SQLite preupdate and commit hooks for change capture. The hooks are owned by the `cdc` package; the Store only registers them lazily on the first `fsmApply` after CDC is enabled, and re-registers them after a database swap (Restore, Load, ReadFrom). See `cdc/DESIGN.md` for the rest of the story.
- **Compressed snapshot transport** — `NodeTransport` wraps `raft.NetworkTransport` and, if `CompressSnapTransport` is set, wraps the snapshot byte stream in a zstd compressor when shipping it to a follower (and in a decompressor on receive). This is invisible to the snapshot store on either end.

## Key Design Decisions and Trade-offs

- **Single struct, many concerns.** The `Store` has ~80 fields, deliberately. Splitting the FSM, the snapshot orchestration, the cluster membership logic, and the index tracking into separate types would each require a back-reference to the Store, because they all need to coordinate with each other. The thin-wrapper FSM and `CommandProcessor` are the two places where decoupling actually pays off — the latter because `RecoverNode` reuses it.

- **Fast restart over conservative restart.** The clean-snapshot fingerprint optimization adds complexity (a fingerprint file, an async CRC check, a snapshot-CAS dance during verification, and several invalidation points scattered across the code) but turns multi-gigabyte node restarts from "extract and re-write the whole database" into "open the existing file and go". For large databases this is the difference between minutes and seconds.

- **Snapshotting drives auto-vacuum and auto-optimize.** SQLite maintenance happens inside the snapshot path because that is the only point where the database is guaranteed quiescent. The trade-off is that maintenance latency is bounded by snapshot frequency, which is usually fine.

- **Strong-read upgrade for linearizable.** The first linearizable read in any new term is upgraded to a strong read so the leader can establish that it has committed something in the current term. This costs a Raft round-trip on term changes but is required for correctness — without it, a freshly-elected leader could return a result based on a stale committed index.

- **CDC hooks registered lazily.** Rather than registering preupdate/commit hooks at `EnableCDC` time, the Store waits until the first `fsmApply` call in which CDC is enabled. This avoids any race between hook registration and database swap, since `fsmApply` is the only place that knows for sure the database is in steady state.

- **One-shot auto-restore on first leadership.** The auto-restore path is gated by `selfLeaderChange` and removes its source file after the attempt — successful or not. There is no retry, and if a different node becomes leader first, the restore is silently skipped. Auto-restore is for seeding a new cluster, not recovering an existing one.

- **Notify-with-bootstrap-expect for discovery-driven startup.** Rather than requiring an operator to pick one node and call `Bootstrap`, the discovery-driven path lets every node call `Notify` on every other node and resolves the race internally — exactly one notifier ends up doing the bootstrap. The cost is the `notifyMu`/`bootstrapped` machinery plus a deliberate exclusion for read-only nodes; the benefit is that orchestrators (Kubernetes, Nomad, systemd) can start every node identically.
