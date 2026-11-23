# rqlite changeset replication: security re-review of the fixes

- Date: 2026-11-22
- Subject: `tannevaled/rqlite#1` (state OPEN), head `04a19b97ffc6d35f195e7eb88c2af56358fa1377`, base `claude-modernc`
- Previous review: `_upstream-reports/rqlite-changeset-review-security.md` (S1 to S10)
- Threat model: an authenticated `execute`-only HTTP client; a malicious or corrupted Raft peer, log entry or snapshot.
- Status of this document: local, not published. Nothing was pushed, no GitHub comment was written.

## How this was measured

- Clone: a separate local clone, detached at the head above (`git rev-parse HEAD`).
- Reproductions (untracked, in that clone): `db/zz_secverify_test.go` (the old `Test_SecReview_*` repros carried over, plus new `Test_SecVerify_*` probes) and `store/zz_secverify_test.go` (three-node clusters).
- Commands:
  - `GOMEMLIMIT=3GiB CGO_ENABLED=0 gotest -run 'Test_SecReview_|Test_SecVerify_' -count=1 ./db/` (output `zz-db.out`, `zz-db2.out`)
  - `GOMEMLIMIT=3GiB CGO_ENABLED=0 gotest -run 'Test_SecVerify_|Test_Review_Store|Test_Changeset_RefusalIsCounted' -count=1 ./store/` (output `zz-store.out`)
  - `GOMEMLIMIT=4GiB CGO_ENABLED=0 gotest -run '^FuzzApplyChangeset$' -fuzz '^FuzzApplyChangeset$' -fuzztime 300s -parallel 4 ./db/` (output `zz-fuzz.out`)
- Toolchain `go1.27.4 darwin/arm64`.
- Limits of the measurement:
  - `ulimit -v` is refused on darwin (`setrlimit failed: invalid argument`); only `GOMEMLIMIT` bounded the runs. `GOMEMLIMIT` does not bound memory allocated by the transpiled C allocator.
  - Machine load average was about 157 throughout. No benchmark was run. No timing in this document is a performance figure.
  - The e2e suite was not run. Leadership-change scenarios (N4, N5) were read, not executed.
  - "REPRODUCED" below means a test in the files above shows it at this head; "INFERRED" means it follows from the code read and was not executed.

## Part 1: verdicts on S1 to S10

| # | Finding | Verdict | Evidence |
|---|---------|---------|----------|
| S1 | Transaction control smuggled into a request (`OR ROLLBACK`, `; COMMIT`, `END`, `RELEASE`) commits on the Leader only | FIXED | REPRODUCED. 7 spellings all return `ErrNotCapturable`, 0 rows committed, no transaction left open (`db/changeset.go:228-232`, authorizer `db/changeset_auth.go:75-118`). Three-node repro `Test_SecVerify_ClusterDivergence`: all nodes identical. |
| S2 | Session pre-update hook clobbers the CDC hook | FIXED (by refusal) | REPRODUCED. `ErrChangesetWithCDC` from both capture entry points; the CDC hook still fires. Store refuses to open with both (`store/store.go:807-809`, `cmd/rqlited/flags.go:183-188`). The PR body states "refused, not chained": accurate. |
| S3 | `PRAGMA` smuggled in a request changes Leader-only state | FIXED | REPRODUCED. Pragma requests fall back; 42 statements compared against 15 pragmas, the schema, a dump and `database_list`: no state change, no open transaction. |
| S4 | Acknowledged writes silently not replicated: (a) NULL primary key, (b) schema mismatch, (c) follower refusal invisible to the client, (d) no-change request counted as a strong read | NOT FIXED in general | (a) fixed for a nullable TEXT key, but two new spellings reach the same loss (N1, N2 below, REPRODUCED on three nodes). (b) missing table and wrong column count are now `ErrChangesetSchema`; a table with the same column count and key flags but other names and types still applies with a nil error (REPRODUCED, `db/changeset_schema.go:214-233`). (c) DOCUMENTED: refusal is a log line and a counter, the client still gets success (`store/store.go:2652-2663`). (d) partly fixed: `strongReadTerm` is no longer moved (`Test_Changeset_NoChangeIsNotAStrongRead` passes), the request itself still skips the log (N4). |
| S5 | Small request, huge changeset in the log | FIXED for replication; Leader-side cost remains | REPRODUCED. 200 000-row request refused with `ErrChangesetTooLarge` ("208600008 bytes, limit 33554432"). The whole C buffer is built before the check (N3). |
| S6 | Malformed changeset in the log is fatal to every node | FIXED | REPRODUCED. The 11-byte entry of the old repro: node survives, `numChangesetDivergences` is 1, the next write works. Truncations: 0 fatal. A truncation that ends on a change boundary applies its prefix with a nil error: DOCUMENTED accurately in the PR body. |
| S7 | Tables without an explicit key addressed by rowid, renumbered by `VACUUM` on one node | FIXED | REPRODUCED. `Test_Review_StoreVacuumRowidConverges` passes; such tables fall back to the statement path (`db/changeset_schema.go:84-151`). See N2 for a way around the check. |
| S8 | Write lock held across `raft.Apply` | DOCUMENTED | INFERRED from the code. `s.changesetMu.Lock()` at `store/store.go:1517-1524` and `:1745-1755`, held across capture, `raft.Apply` and the fallback; `Load` takes it at `:1989-1990`. The PR body lists it as open: accurate, with two additions in N6. |
| S9 | Triggers left disabled after a failed apply | FIXED; unconditional restore DOCUMENTED | REPRODUCED. Triggers are on after a conflict; a failed restore is a `FatalError` (`db/changeset.go:445-493`). Restore to 1 regardless of the earlier value is in the PR body's open list. |
| S10 | Stray dotfile in `db/`, mixed-version clusters, `rawHandles` reflection | Dotfile FIXED; the rest DOCUMENTED | `git ls-files "db/.*"` prints nothing. Mixed versions and `rawHandles` fragility are both in the PR body's open list. |

Fuzzing: `FuzzApplyChangeset` (`db/changeset_diff_test.go:179`) ran 303.68 s, 23 289 executions, baseline 819 inputs, 14 new interesting inputs, no crash, no failure, `PASS`. The execution rate is the loaded machine's, not the target's: the PR body reports about 984 000 executions in 60 s, and this run reached about 2 % of that in five times the duration, with long stretches at 0/sec. Coverage of this run is therefore shallow; it is a no-regression signal, not a strong negative. The seed corpus is committed: `git ls-files db/testdata/fuzz` lists 16 files under `db/testdata/fuzz/FuzzApplyChangeset/` (`371f778d45b5421c`, `cut-00..04`, `flip-00..04`, `seed-00..04`).

## Part 2: new findings, most severe first

### N1. HIGH, CONFIRMED: `INTEGER PRIMARY KEY DESC` is taken for a rowid alias; an acknowledged write is on no node

- Where: `db/changeset_schema.go:140` (`len(pkCols) == 1 && strings.EqualFold(pkCols[0].typ, "INTEGER")`).
- Cause: SQLite documents that `INTEGER PRIMARY KEY DESC` is not an alias for the rowid. The column is an ordinary nullable key column; the session extension does not record a row whose key is NULL. The recordability check accepts the table, the changeset is empty, and the empty-changeset path returns success (`store/changeset.go:81-85`).
- Scenario: `CREATE TABLE t (id INTEGER PRIMARY KEY DESC, v TEXT)`, then `INSERT INTO t VALUES(NULL,'x')`. The client receives `{"last_insert_id":1,"rows_affected":1}`.
- REPRODUCED: `Test_SecVerify_IntegerPKDescNull` (db) and `Test_SecVerify_ClusterIntegerPKDescLost` (three nodes): every node holds 0 rows.
- Reach: needs such a table to exist. An `execute`-only client can create it; a legitimate schema may already have one. No attacker is required: this is silent data loss on a legal schema.
- Direction for a fix: decide rowid aliasing the way SQLite does, not from the declared type. Independently, refuse to acknowledge when the statements reported `rows_affected > 0` and the changeset is empty: that single invariant would have caught N1, N2 and the original S4a.

### N2. HIGH, CONFIRMED: a TEMP table shadows the main table in the recordability check; writes are acknowledged and dropped on every node

- Where: `db/changeset_schema.go:37` (`pragma_table_xinfo(?)` with an unqualified name, which resolves to `temp` first), used by `tablesRecordable` (`:84-151`) and by `checkChangesetShape` (`:214-233`).
- Cause: the authorizer records the table as written in `main`; the check then reads the description of the `temp` table of the same name. If that one has a proper key, a `main` table without a key (or with a nullable key) is declared recordable. The session records nothing for it.
- Scenario, one statement from an `execute`-only client: `CREATE TEMP TABLE audit (id INTEGER PRIMARY KEY, what TEXT)`. It takes the statement path and is replicated, so it exists on every node's write connection. From then on, any write to `main.audit (what TEXT)` made by a trigger body (trigger bodies resolve names in the trigger's own schema) or by a schema-qualified statement is acknowledged and stored nowhere, while the triggering row itself replicates.
- REPRODUCED: `Test_SecVerify_TempShadowDefeatsRecordability` (db) and `Test_SecVerify_ClusterTempShadowLosesTriggerWrites` (three nodes): after the temp table, `INSERT INTO acct VALUES(2,20)` (audit trigger) and `INSERT INTO main.audit VALUES('direct')` are both answered `rows_affected: 1`; every node holds `acct=2, audit=1` where 3 audit rows are expected.
- Why it matters: this is a suppression primitive for keyless tables, which are typically log and audit tables. It persists until the write connection is reopened, and that happens at different times on different nodes.
- Second effect, same root: in the apply direction a `temp` table makes the shape check read the wrong table, so a valid changeset is refused on that node (`Test_SecVerify_TempShadow`: "has 1 columns, the changeset 2"). After a restart of one node the temp table exists on some nodes only; the outcome is a per-node refusal, that is a divergence alarm.
- Direction for a fix: qualify every schema read with `main` (the table-valued pragma takes a schema argument), and consider refusing `CREATE TEMP` while the feature is on.

### N3. MEDIUM, CONFIRMED: the size limit is enforced after the changeset has been built twice

- Where: `db/changeset.go:263` (`sess.changeset()`) then `:266` (limit). `session.size()` at `:418` exists and is not used.
- Scenario: one small request (`INSERT ... SELECT` over a recursive CTE, or an `UPDATE` of a large table) makes the Leader allocate the full changeset in the C heap and copy it to Go before refusing. Measured: a 208 600 008-byte buffer built for a request refused at a 33 554 432-byte limit. The C allocation is outside `GOMEMLIMIT`.
- REPRODUCED: `Test_SecReview_Amplification` with `cSessionChangeset` hooked.
- Related, INFERRED: `-changeset-max-size` has no upper cap (`cmd/rqlited/config_flags.go:122,232`); the apply side has no limit other than `math.MaxInt32-64`. The refusal has no fallback, so a legitimate bulk write above the limit now fails outright: documented, by design.

### N4. MEDIUM, CONFIRMED for one code path, PLAUSIBLE for the other: node-local failures are reported as "malformed changeset" and are not fatal

- Where: `db/changeset_schema.go:176-178` and `:205-207` (any non-OK code from the iterator), `db/changeset.go:308-311` (returned without `classifyError`), `db/changeset.go:485-490` (any `SQLITE_CORRUPT` from apply).
- Scenario: one node runs out of memory while walking a changeset, or its database file is corrupt. Both are in `nodeLocalErrorCodes` (`db/db_errors.go:174-189`) and would stop the node on the statement path. Here the entry is skipped, the node logs an alarm and keeps serving reads from a database that has missed a committed write. A peer that can send large entries can provoke the out-of-memory case on the smallest node.
- REPRODUCED for the iterator: `Test_SecVerify_NoMemInWalkIsNotFatal` (`cChangesetStart` returning 7 yields "malformed changeset (rc 7)", not fatal). INFERRED for `SQLITE_CORRUPT` in apply. Not affected: a failed `malloc` of the input buffer is fatal.
- Direction for a fix: map only the codes the format check can produce to "malformed"; pass every other code through `classifyError`. After the walk has validated the bytes, a `SQLITE_CORRUPT` from apply is about the database, not the changeset.

### N5. MEDIUM, PLAUSIBLE: no leadership proof on the empty-changeset path, and none between capture and `raft.Apply`

- Where: `store/changeset.go:81-85` and `:65-95`; `changesetCaughtUp` (`:109-119`) checks the term once, before capture.
- Scenarios, not executed: (1) a deposed or partitioned Leader answers a `/db/request` whose writes match no rows with success and with read results from its stale state, without a log round trip; (2) leadership is lost and regained between capture and apply, so a changeset computed from a stale read (`INSERT ... SELECT count(*)`) is committed on top of entries it never saw. Conflict handling does not catch a value computed from another table.

### N6. LOW, INFERRED: additions to S8

- `changesetMu.Lock()` ignores the request context: a client that disconnects keeps its place in the queue, and each queued write can hold the lock up to `ApplyTimeout` when quorum is lost.
- A request that cannot be captured is executed twice on the Leader (once under capture and rolled back, once on the statement path), both under the lock. An `execute`-only client can double the Leader cost of any expensive non-capturable request.

### N7. LOW, CONFIRMED: behaviour changes for maintenance statements inside a request

- `SELECT 1; REINDEX` is answered with an empty success and no changeset: the `REINDEX` is not performed and not replicated. `SELECT 1; VACUUM` returns "cannot VACUUM from within a transaction" instead of running on the statement path.
- REPRODUCED in `Test_SecVerify_AuthorizerBypass`. No state change, no divergence; an acknowledged no-op.

### N8. LOW, CONFIRMED: the shape check compares column count and key flags only

- Where: `db/changeset_schema.go:214-233`. A changeset recorded for `foo(id, v TEXT)` applies with a nil error to `foo(k INTEGER PRIMARY KEY, other BLOB NOT NULL DEFAULT x'00')`. Only reachable when schemas already differ between nodes (peer or snapshot threat); it turns a detectable divergence into a silent one.

## Examined and found sound

- Authorizer: trigger bodies, views with `INSTEAD OF` triggers, foreign-key cascades into triggers, `RAISE(ROLLBACK)`, `sqlite_master` writes with `writable_schema`, `ATTACH`, all DDL, `SAVEPOINT`/`BEGIN`/`END`/`COMMIT` all fall back with no state change. `load_extension(...)` is "not authorized". `pragma_table_info()` in a `SELECT` is allowed and harmless; `pragma_user_version`, `pragma_optimize` fall back; pragma functions given arguments error out.
- 32-bit lengths at the C boundary: `checkChangesetLen` (`db/changeset.go:285-290`) refuses `n > math.MaxInt32-64`; at the largest accepted `n = 2147483583` the two allocations are 2 147 483 608 and 2 147 483 632 bytes, both below `MaxInt32`. No wrap (`Test_SecVerify_LenGuard`).
- Apply-error policy: no request was found that a client can send and that every node then refuses or crashes on. Generated columns (virtual and stored), `INSERT OR REPLACE` over a unique conflict and a three-step unique swap all capture and apply cleanly. Applying a changeset on a connection with a CDC hook works.

## Not covered

- e2e suite, leadership-change scenarios under real partitions, snapshot restore with temp state, mixed-version clusters, any measurement of throughput.
- `ulimit -v` bounding, unavailable on this platform.
