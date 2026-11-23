# Correctness re-review: rqlite changeset replication prototype, head 04a19b97

- Reviewed: tannevaled/rqlite#1 at `04a19b97ffc6d35f195e7eb88c2af56358fa1377` (base `claude-modernc`). The diff since the first review is `97290e31..04a19b97`: 4 commits, 39 files.
- Worktree: a separate detached local worktree of this branch (nothing committed or pushed). The probes were untracked files there (the round-3 ones are ported to `db/changeset_knownbugs_test.go` and `store/changeset_knownbugs_test.go`, build tag `changeset_knownbugs`):
  - `db/zz_review_probe_test.go` and `store/zz_review_probe_test.go`: the first review's probes, copied verbatim.
  - `db/zz_verify_probe_test.go` and `store/zz_verify_probe_test.go`: new probes.
- How the probes were run: `CGO_ENABLED=0 gotest -v -count=1 -run …`, modernc.org/sqlite, go1.27.4 darwin/arm64. The PR's own `Test_Review_*|Test_Changeset*` in `./db/ ./store/` pass at this head.
- Date: 2026-11-22.

## Verdict

The fixes close the four CONFIRMED divergences of the first review. The transaction-escape class is closed by an authorizer plus a per-statement autocommit check. CDC is refused. The VACUUM/rowid divergence and the TEMP lost write are gone. The invariant still does **not** hold:

1. One new lost-write path is CONFIRMED end-to-end: `INTEGER PRIMARY KEY DESC`. The cause is the static schema check that replaced the suggested count comparison.
2. Deferred foreign keys change the semantics of a request and raise a false divergence alarm.
3. A STRONG read can still be served without quorum.
4. The "non-fatal divergence" policy means a follower that does not match the Leader keeps a write the client was told succeeded. The PR states this policy openly.

## Part 1: earlier findings, re-run at 04a19b97

| # | Finding | Verdict | Evidence |
|---|---|---|---|
| 1 | Capture escapes (COMMIT smuggle, OR ROLLBACK, RAISE(ROLLBACK)) leave the Leader diverged | **FIXED** | `Test_Probe_StoreMultiStatementDiverges`: Leader and follower both `[[1,"a"]]`; client gets the COMMIT error. `Test_Probe_MultiStatementCommit` and `Test_Probe_OrRollback`: `ErrNotCapturable`, the DB is unchanged after the capture. Mechanism: the authorizer denies transaction control/savepoints/DDL/PRAGMA and anything prepared once autocommit is back (`db/changeset_auth.go:85-109`). Statements run one at a time, with `auth.refused() \|\| raw.autocommit()` checked after each one (`db/changeset.go:220-232`). modernc prepares a multi-statement string incrementally, so a tail statement is authorized after the earlier one ran. The PR's `Test_Review_OrRollbackDoesNotLeak` covers the single-string case. |
| 2 | CDC + capture SIGSEGV | **FIXED** | `Test_Probe_CDCHook`: `capture err=changeset replication cannot be combined with CDC`, no crash, and the hook still fires afterwards. Refused at `Store.Open` (`store/store.go:807-808`) and per capture (`db/changeset.go:142-147`). Chaining the two is listed as open. |
| 3 | Follower conflict swallowed; VACUUM renumbers rowids | **FIXED** (VACUUM trigger) / **DOCUMENTED** (policy) | Tables without an explicit PK are no longer captured (`db/changeset_schema.go:136-137`), so they take the statement path. In `Test_Probe_StoreVacuumRowidDiverges` the values converge (`b,z,e` on both nodes). The rowids still differ (`2,4,5` vs `1,2,3`), but that is base-rqlite behaviour of the statement path after a node-local VACUUM, not a regression of this PR. A refused apply now logs `CHANGESET DIVERGENCE ALARM` and counts a stat (`store/store.go:2654-2662`), but stays non-fatal (`store/command_processor.go:94`). The PR body says so accurately ("Divergence stays non-fatal"). See N4 for what that still allows. |
| 4 | Mixed-version clusters skip changesets | **DOCUMENTED (NOT FIXED)** | The PR body's "Open" list names it. No version gate was added; `default:` in the command processor is unchanged. |
| 5 | NULL PK / TEMP / attached writes acknowledged but lost | **PARTLY FIXED** | `Test_Probe_NullPK` (`k TEXT PRIMARY KEY`) is now `ErrNotCapturable`, so the statement path stores the row. `Test_Probe_Temp`: `ErrNotCapturable` (the probe then panics indexing nil results, which is a probe artefact). Non-main writes are denied (`changeset_auth.go:110-114`). **But** the static check misclassifies `INTEGER PRIMARY KEY DESC` (new finding N1): the same loss, CONFIRMED. |
| 6 | Load between capture and apply | **FIXED** | `Store.Load` takes `changesetMu` (`store/store.go:1986-1991`). No chunked-load or other DB-swapping path reachable on a cluster leader was found (`git grep LoadChunk`: none; `ReadFrom` is the single-node boot). |
| 7a | No-op write + read → "strong" read without quorum | **NOT FIXED (half)** | Only the `strongReadTerm` side effect was fixed (`store/store.go:1750`). When the capture yields an empty changeset, `changesetApplyLogged` still returns the locally read rows with ok=true and no log entry (`store/changeset.go:79-85`). `Request` returns them to a `level=strong` client (`store/store.go:1748-1753`). Scenario: a deposed Leader in its old term gets `UPDATE t SET x=1 WHERE 0; SELECT * FROM t` with level=strong. It answers from its stale DB, whereas the statement path would have gone through Raft and failed with ErrNotLeader. The once-per-term barrier (`changesetCaughtUp`) does not prove current leadership. CONFIRMED by code. |
| 7b | Raw `raft.ErrNotLeader` returned | **FIXED** | `mapNotLeader` (`store/changeset.go:50-55`) is applied to the barrier and `af.Error()`. |
| 8 | Fallback replicates non-deterministic SQL | **DOCUMENTED** | Listed under "Open". Still true for `rollback_on_error`, explicit BEGIN, DDL-led requests, no-PK tables, virtual tables and FK-reachable children. The new refusals widen this fallback set. |
| 9 | No changeset size bound | **PARTLY FIXED** | A 32 MiB default and `-changeset-max-size` now fail the request with `ErrChangesetTooLarge` (`db/changeset.go:259-268`). The check runs only **after** `sess.changeset()` has materialised the whole buffer, plus a Go copy. `SQLITE_SESSION_OBJCONFIG_SIZE` is enabled (`:397-401`) and `session.size()` exists (`:418-420`), but nothing calls it, so Leader memory is still unbounded during a capture. The "refused before its buffer is built" comment at `:394-396` is false. |
| 10 | Stray `.changeset.go.atomicwrite-*` | **FIXED** | Deleted in this range (`git diff --stat`). |

## Part 2: new findings, ranked

### N1. HIGH: `INTEGER PRIMARY KEY DESC` with a NULL key: acknowledged, lost on every node. CONFIRMED

`tablesRecordable` treats any single PK column whose declared type is `INTEGER` as the rowid (`db/changeset_schema.go:140-141`). SQLite's documented quirk is that `INTEGER PRIMARY KEY DESC` as a column constraint is **not** a rowid alias. It is an ordinary nullable key column (rowid-table legacy NULL-PK behaviour), and the session extension does not record rows whose PK is NULL.

- `Test_Verify_IntegerPKDescNull` (db level): the capture returns `rows_affected:1 last_insert_id:1`, changeset length 0, both nodes `foo: []`. The statement path stores `[[1,null,"x"]]`.
- `Test_Verify_StoreIntegerPKDescNull` (2-node cluster, flag on): the client gets `rows_affected:1`, err=nil. Leader and follower are both empty. No Raft entry is written (`store/changeset.go:79-85`, empty changeset).

This is the exact failure class of old #5, reached through the static schema check that the PR chose instead of a count check. Fix: decide rowid-alias status from SQLite itself, not from the type name. For example, a non-alias PK column shows in `pragma_index_list` as an `origin='pk'` autoindex, while an alias has none. Alternatively, require `notnull` for every PK column that is not provably the rowid. As a backstop, refuse when the session is empty but `sqlite3_total_changes` moved during the capture (coalescing cannot make a non-empty change set empty except by insert+delete of the same row, which N3 shows also matters).

### N2. MEDIUM: Deferred foreign keys: the capture never commits, so deferred constraints are checked only at apply. The request fails as a whole and every node raises a false divergence alarm. CONFIRMED

The capture runs in `BEGIN IMMEDIATE … ROLLBACK` (`db/changeset.go:189-200`). A `DEFERRABLE INITIALLY DEFERRED` violation is therefore never checked on the Leader. `sqlite3changeset_apply_v2` checks it at the end of the apply, and `abortOnConflict` aborts (`:512-515`).

`Test_Verify_DeferredFK` (FK on; request `INSERT INTO other …`, `INSERT INTO c VALUES(1, 99)`, no transaction):
- Capture: both statements report success, changeset 53 bytes. The apply on **both** nodes returns `changeset conflicts … (kind 5 = FOREIGN_KEY)`, and both are left empty.
- Statement path: statement 1 committed (`other: [[1,"kept"]]`), statement 2 fails with `FOREIGN KEY constraint failed`.

Consequences:
- (a) In a non-transactional request, valid earlier statements are lost. The client does get an error ("changeset apply: …"), so the invariant holds, but the per-statement results contract of non-transactional requests is broken.
- (b) Every node logs `CHANGESET DIVERGENCE ALARM` and counts a divergence (`store/store.go:2654-2662`) for a deterministic, uniform refusal. That trains operators to ignore the alarm that is now the only signal for N4.
- (c) `-fk` is a per-node flag. With fk on the Leader and off on a follower, the follower applies what the Leader refused, which is divergence plus a write the client was told failed. The statement path has the same misconfiguration hazard, so this is PLAUSIBLE and pre-existing in kind.

Fix: check `PRAGMA foreign_key_check` (or `sqlite3_db_status(SQLITE_DBSTATUS_DEFERRED_FKS)`) at the end of each statement when not in a transaction, and at the end of the capture. Mark FK-kind refusals as deterministic, not as divergence.

### N3. LOW: AUTOINCREMENT high-water mark lost when a request inserts and then deletes the same row. CONFIRMED

`Test_Verify_AutoincInsertDelete` (`INSERT …; DELETE FROM foo`): the session coalesces the insert and delete to nothing, so the changeset is empty, and `sqlite_sequence` stays `[]` on every node. The statement path leaves `seq=1`. The nodes agree with each other, but the next insert reuses id 1, which breaks AUTOINCREMENT's documented never-reuse guarantee and differs from the statement path. The first review's probe (INSERT/DELETE/INSERT) masked this because the second insert re-raised the sequence. Fix: treat writes to AUTOINCREMENT tables as non-capturable, or capture `sqlite_sequence` explicitly.

### N4. MEDIUM (design, documented): A non-fatal refusal leaves an acknowledged write missing on that node. CONFIRMED by code

When a follower refuses (conflict, schema), it logs and moves on (`store/command_processor.go:94`). The Leader's apply succeeded, so the client got success, and that node lacks the write for good. Later changesets that touch other rows keep applying, so the node serves divergent reads indefinitely. The reverse also holds: if the Leader refuses and a diverged follower accepts, the follower holds a write the client was told failed. The PR documents the choice. It contradicts the stated invariant "an acknowledged write is on every node or the request errors", which should be reworded or enforced, for example by removing the refusing node from the read path or stopping it.

### N5. LOW: Timeout or interrupt during capture silently re-routes to the statement path. PLAUSIBLE

When `DbTimeout` or a context cancel interrupts a write inside the capture transaction, SQLite rolls back the whole transaction. `raw.autocommit()` then becomes true and the request is returned as `ErrNotCapturable` (`db/changeset.go:228-231`). `Store.Execute` re-runs it as SQL text through Raft (`store/store.go:1520-1525`). The non-deterministic SQL that changesets exist to avoid is replicated exactly when the Leader was slow. This was not run; inferred from SQLite's documented `sqlite3_interrupt` behaviour inside explicit transactions.

### N6. LOW: Size limit does not bound Leader memory. CONFIRMED by code

See row 9: the limit is enforced only after the full buffer and its Go copy exist.

## Classic escapes probed, no defect found

Each item lists the probe and its outcome:
- `SAVEPOINT …; …; RELEASE` and a trailing `SAVEPOINT`: refused → statement path; nothing committed by the capture.
- `WITH … INSERT`, `REPLACE` through a UNIQUE index, UPSERT that rewrites the PK, `INSERT … RETURNING`, leading comments, two INSERTs in one string: the changeset path equals the statement path on both nodes (`Test_Verify_Escapes`).
- FTS5 written directly or through a trigger, and rtree: `ErrNotCapturable` (`Test_Verify_VirtualTables`).
- FK `ON DELETE CASCADE` into a child whose trigger writes a table with no PK: refused. The authorizer sees the trigger's write (`Test_Verify_FKCascadeTrigger`).
- `sqlite_dbpage`/shadow tables: they are not `type='table'` in `pragma_table_list`, so they are refused (code reading, `changeset_schema.go:122-128`).

## Not examined

- Restart/replay when the on-disk DB is newer than the log.
- Non-voter nodes.
- The race detector.
- e2e scripts.
- A deposed-leader STRONG-read reproduction (7a is by code only).
