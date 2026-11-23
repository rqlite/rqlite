# rqlite changeset replication — security re-review (round 3)

- Subject: `tannevaled/rqlite#1`, head `bad31c8c63bae194651def2fc47d61f2fce88bb9`, base `claude-modernc`.
- Clone (read-only, detached HEAD): a separate local clone. The round-3 probes are ported to `db/changeset_knownbugs_test.go` and `store/changeset_knownbugs_test.go` (build tag `changeset_knownbugs`).
- Threat model: an authenticated `execute`-only HTTP client, and a malicious or corrupted Raft peer / log entry / snapshot.
- Toolchain: `go1.27.4 darwin/arm64`, `CGO_ENABLED=0`. Memory-hungry tests bounded with `GOMEMLIMIT` (3–4 GiB). `ulimit -v` is unavailable on darwin (the kernel refuses an address-space rlimit), so bomb tests use `GOMEMLIMIT` alone — stated per the brief.
- No edits to the main clone, no push, no GitHub comments. Nothing published. Exploit specifics are confined to this file.
- Probe test files (untracked, in my clone): `db/zz_secverify_test.go`, `db/zz_r3_test.go`, `db/zz_r3b_test.go`, `db/zz_r3c_test.go`, `store/zz_secverify_test.go`. Outputs `zz-*.out`.

## How measured

- db-level repro: capture a request with `DB.ExecuteCapture`, then apply the resulting changeset with `DB.ApplyChangeset` to a second DB whose state I control. In the cluster the Leader's FSM and every follower's FSM do exactly this against their own (identical) state, so a non-nil conflict/schema apply error = `FatalError` in `store/command_processor.go:84-104` → `store/store.go:2641-2643` `logger.Fatalf` → the node dies.
- store-level repro: three-node in-memory cluster (`store/zz_secverify_test.go`), already passing from round 2.
- Load during this session was high (1-min load 16.76 at 01:14); throughput numbers are therefore reported with that caveat and are not a benchmark. Correctness/crasher results do not depend on load.

## Verdict on earlier findings

| ID | Round-2 status | Round-3 verdict |
|----|----------------|-----------------|
| S1 commit/rollback smuggling | open→fixed | FIXED. 8 spellings → `ErrNotCapturable`, 0 rows committed, no open tx. |
| S2 CDC + changeset | fixed (refusal) | FIXED. `ErrChangesetWithCDC`; CDC hook still fires. |
| S3 pragma smuggling | fixed | FIXED. Falls back; later CHECK fires. |
| S4a NULL TEXT key | fixed | FIXED (fallback). |
| S4b schema drift silent apply | open (N8) | PARTIAL. Missing table / wrong column count → `ErrChangesetSchema` (now fatal). Same count, other names/types still applies `err=nil`. Peer/snapshot threat only. STILL OPEN as N8. |
| S5 huge-changeset memory | open (N3) | FIXED for the Go copy: `session.size()` at `db/changeset.go:281` refuses before `sess.changeset()`. Residual below (e). |
| S6 truncated changeset | fixed | FIXED. 0 fatal; documented prefix apply. |
| S9 trigger after refused apply | fixed | FIXED. |
| N1 INTEGER PK DESC NULL | — | FIXED (fallback). |
| N2 TEMP shadow | — | FIXED (fallback; apply ignores temp.* shadow). |
| N4 OOM told from malformed | — | FIXED. `cMalloc`/`cChangesetStart` rc 7 → `out of memory`, fatal, not malformed. |
| N5 leadership window | open | Term re-check + VerifyLeader present (`store/changeset.go:104-112`); residual check→Apply window documented in code; not executed. |
| N7 SELECT;REINDEX capture | open | Pinned to statement path by the head commit; `SELECT 1; REINDEX` still shows `cs=0`. No divergence observed. |

## New findings (round 3), most severe first

### R3-1 Conflicting changeset is a cluster-wide poison pill — HIGH (peer threat). CONFIRMED (error class + mapping); crash-loop inferred.
`db/changeset.go:562` returns `ErrChangesetConflict` for every apply conflict; `store/command_processor.go:98-100` wraps it in `sql.NewFatalError`; `store/store.go:2642` turns any fatal `Process` error into `logger.Fatalf` on **every** node, Leader included. REPRODUCED at db level (`db/zz_r3c_test.go`): an INSERT of an existing key (kind 3, rc 4), a DELETE of a missing row (kind 2), and an UPDATE whose old values differ (kind 1) all return `ErrChangesetConflict`. Because a healthy cluster holds identical state, a single Raft entry of type CHANGESET carrying a changeset that conflicts with that common state is fatal on all nodes at once, and is replayed through the FSM on restart → crash loop until the entry is snapshotted away. This is a round-3 regression in blast radius: round 2 recorded the divergence and kept the node alive (`Test_SecVerify_MalformedChangesetNotFatal` still shows malformed stays non-fatal). A malicious or corrupted peer that can craft one shape-valid, semantically-conflicting changeset kills and bricks the quorum. Malformed bytes remain non-fatal (good), but a *targeted* conflicting changeset is trivial to build. Recommend: treat an apply conflict on a committed, replicated entry as a loud but **recoverable** state (quarantine / resync the node) rather than `Fatalf`, or at least break the replay loop so a restart can make progress.

### R3-2 Leader-side memory amplification before the size refusal — MEDIUM. PLAUSIBLE (residual of S5/N3), not measured under load.
`db/zz_r3_test.go` (`Test_R3_AllocBeforeSizeCheck`): a 50 000-row UPDATE is refused "52150008 bytes, limit 33554432" and the largest Go-side `cMalloc` seen is 16 bytes — the Go changeset copy is never built. But `session.size()` only reads a running total the SQLite session accumulates **in its own C heap as each statement executes**; by the time `db/changeset.go:281` consults it, the full change set (old+new values of every touched row) already sits in C memory, outside `GOMEMLIMIT`. An `execute`-only client can make the Leader allocate ~the touched-table size per request before the refusal. Not reproduced as an OOM (would need RSS/`sqlite3_memory_used` sampling; no benchmark under this load).

### R3-3 Timeout-by-message-text misfires on attacker-chosen text — LOW. CONFIRMED.
`db/changeset.go:313-332` (`transactionEnded`) classifies a capture as a retryable interrupt when the error text contains `interrupt`/`timeout`/`deadline exceeded`/`canceled`. REPRODUCED (`Test_R3_TimeoutMessageMisfire`): a deterministic `CREATE TRIGGER … RAISE(ROLLBACK,'request timeout: please retry')` ends with `err=changeset capture was interrupted and rolled back: request timeout: please retry`, `notCapturable=false`. Such a request is answered with a retryable-looking error and is **not** routed to the statement fallback path (`store/changeset.go:95-97` returns it to the client). A column/table/constraint name or a RAISE message containing one of those words turns an otherwise-deterministic write into a permanent error instead of a fallback. No divergence, nothing committed — an availability/correctness wart. Reverse direction (a real interrupt whose text lacks the words → `ErrNotCapturable` → statement re-run) not exercised.

### R3-4 `quick_check` on demand in the FSM thread — LOW/MEDIUM. PLAUSIBLE, not reproduced.
`db/changeset.go:377-395`: on `SQLITE_CORRUPT` from apply, `PRAGMA quick_check(1)` runs a full b-tree scan on the applying connection, which is the single FSM writer; it blocks all other applies for the scan. Reachable only via a changeset whose bytes the iterator accepts but apply reports corrupt — a peer capability, not an `execute`-only client (the client does not control changeset bytes). On a large DB, repeatable corrupt entries = a stall DoS. Not reproduced (would need a hooked counter and a crafted corrupt-but-walkable changeset).

## Examined and found sound

- One-request quorum kill by an `execute`-only client: NOT reproduced. The Leader applies each captured changeset to the exact post-rollback state it was recorded against, and SQLite guarantees a changeset applies cleanly to its source DB; followers hold the same state. UNIQUE swaps through a temp value (`Test_R3_UniqueSwapFatalOnApply`, cs=42) apply cleanly; PK renumber falls back; the CASE swap fails at capture and commits nothing.
- FK-ordering conflicts: NOT reproducible. The apply/writer connection runs with `foreign_keys = 0` (`Test_R3B_ApplyConnFKState`), and changeset apply fires no triggers, so parent/child and self-referential FK changesets apply cleanly (`db/zz_r3b_test.go`). Note: FK constraints are therefore **not enforced through replication**; both Leader and followers apply with FKs off, so they stay identical — a semantic gap, not a divergence.
- Accidental count match hiding a lost write (b): NOT reproduced. `totalChanges` delta ≥ real changes, so only `ops == touched.count()` could hide a loss. A WITHOUT ROWID insert is recorded and replays correctly; `INSERT OR REPLACE` falls back. The NULL-key-plus-REPLACE-delete cancellation remains untested as a concrete lost write.

## Not covered / limits

- Store-level `Fatalf` for R3-1 was confirmed by code path, not executed (it kills the test process); the db-level error class and the `command_processor` mapping are reproduced.
- Measurements taken under 1-min load 16.76 (> the ~8 ceiling); throughput figures are indicative only.
- Permission note: the auto-mode classifier denied a `GITSAFE_ALLOW_SHELLWORK=1 grep …` command; plain `grep -rn` without a pipe was allowed. The escape prefix was not reused.

## Fuzz: FuzzApplyChangeset

Command: `gotest -run '^$' -fuzz '^FuzzApplyChangeset$' -fuzztime 300s -parallel 4 ./db/` (output `zz-fuzz.out` in the clone).

- Result: `--- PASS: FuzzApplyChangeset (301.40s)`, `ok … 301.786s`. No crasher, no new file under `db/testdata/fuzz/`.
- 496 520 executions in 301.4 s, about 1 647 execs/s on 4 workers; 110 new interesting inputs (878 → 988 in the cache).
- Load: 1-min 16.76 before, 24.68 after. Above the ~8 ceiling, so the rate is indicative only (per-3-second rates ranged from 0 to 12 437/s).
- The command's exit status was 1 only because `gotest` reports that the `^$` alternative matched no test, which is intended in a fuzz-only run; the fuzz verdict is the PASS line.
- This run did not set `GOMEMLIMIT` (the hook-suggested command line dropped the variable).
