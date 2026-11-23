# Correctness re-review, round 3: rqlite changeset replication, head bad31c8c

- Reviewed: tannevaled/rqlite#1 at `bad31c8c63bae194651def2fc47d61f2fce88bb9` (base `claude-modernc`). The range since round 2 is `04a19b97..bad31c8c`, 4 commits.
- Clone: a separate local clone (detached; nothing committed or pushed). The round-3 probes below are ported to `db/changeset_knownbugs_test.go` and `store/changeset_knownbugs_test.go` (build tag `changeset_knownbugs`). The probes are untracked files there:
  - copies of the earlier probes: `db|store/zz_review_probe_test.go` and `db|store/zz_verify_probe_test.go`
  - new probes: `db/zz_verify3_probe_test.go` and `store/zz_verify3_probe_test.go`
- Run with `CGO_ENABLED=0 gotest -count=1 -v -run …` (modernc.org/sqlite, go1.27.4 darwin/arm64). Raw output is in `_upstream-reports/v3-*.out`.
- SQLite documentation opened: <https://www.sqlite.org/c3ref/total_changes.html> and <https://www.sqlite.org/c3ref/update_hook.html>.
- Date: 2026-11-23.

## Verdict

**Yes, acknowledged writes are still lost. CONFIRMED end-to-end on a 2-node cluster.** A single client `INSERT` gets `rows_affected:1, err=nil`, and the row is on no node.

The completeness check (`db/changeset.go:300`) compares two counts with the changeset's op count. It accepts when **either** count equals the op count, so it does not prove that no row was lost. Some changes add an op that neither counter sees:
- a REPLACE delete;
- a write to a WITHOUT ROWID table;
- an UPDATE that moves the primary key.

Any of these offsets one row the session dropped.

`tablesRecordable` still treats `INTEGER PRIMARY KEY DESC` as the rowid (`db/changeset_schema.go:140`), so the dropped row is easy to produce.

Separately, making apply conflicts fatal lets one client with execute permission stop every follower.

## Part 1: earlier findings at bad31c8c

| Earlier finding | Verdict | Evidence |
|---|---|---|
| R1 #1 capture escapes (COMMIT smuggle, OR ROLLBACK) | FIXED | Probes `MultiStatementCommit`, `OrRollback`, `StoreMultiStatementDiverges`: NotCapturable, DB unchanged, both nodes `[[1,"a"]]` |
| R1 #2 CDC SIGSEGV | FIXED | `Test_Probe_CDCHook`: refused, hook intact |
| R1 #3 VACUUM rowids | FIXED (values) | Both nodes `b,z,e`; only rowids differ, which is base behaviour |
| R1 #4 mixed versions | DOCUMENTED | PR body "Open" list |
| R1 #5 NULL PK / TEMP | FIXED alone | `Test_Probe_NullPK`: NotCapturable. `Test_Probe_Temp`: NotCapturable (the probe then panics on nil results; probe artefact) |
| R1 #6 Load race | FIXED | unchanged since round 2 |
| R1/R2 7a no-op write + read without quorum | FIXED (as linearizable) | `verifyLeader` before returning local rows, `store/changeset.go:100-108`. The PR says plainly this is not STRONG |
| R1 #8 fallback replicates non-deterministic SQL | DOCUMENTED | It is now the trigger of N2 below |
| R1 #9 size bound | FIXED by code | `sess.size()` is checked before `sess.changeset()` (`db/changeset.go:281`). The session's own memory still grows unbounded during statement execution |
| R2 N1 `INTEGER PRIMARY KEY DESC` NULL | **NOT FIXED** | Caught alone (`Test_Verify_IntegerPKDescNull` → NotCapturable; the store probe now stores the row). Lost when combined with an offset: N1 below |
| R2 N2 deferred FK | FIXED | NotCapturable; statement path keeps `other` and fails statement 2 |
| R2 N3 AUTOINCREMENT insert+delete | **NOT FIXED** | Caught alone; bypassed by an offset (N3 below) |
| R2 N4 non-fatal divergence | FIXED, but see N2 | Conflict and schema errors are now fatal (`store/command_processor.go:98-99`) |
| R2 N5 timeout re-routes | FIXED by code | `transactionEnded` returns an error, not ErrNotCapturable, for interrupt/timeout (`db/changeset.go:326-329`) |
| R2 N6 size not bounding memory | PARTLY FIXED | see R1 #9 |

## Part 2: new findings, ranked

### N1. CRITICAL: an acknowledged write is lost on every node because the count check is satisfied by an offset. CONFIRMED (db and store)

The check is `ops != Δtotal_changes && ops != touched` → fall back (`db/changeset.go:300`). A row the session does not record lowers `ops` by 1. It still raises Δtotal and touched by 1.

Three things raise `ops` by 1 without raising one of the counters. Each is measured in `Test_V3_CountSemantics`, and each matches the SQLite docs:

| Change | ops | Δtotal_changes | touched | Doc source |
|---|---|---|---|---|
| REPLACE that deletes another row through a UNIQUE index | 2 | 1 | 1 | total_changes: "those made as part of REPLACE constraint resolution are not" counted. update_hook: not invoked "when conflicting rows are deleted because of an ON CONFLICT REPLACE clause" |
| WITHOUT ROWID write | 1 | 1 | **0** | update_hook: "not invoked when WITHOUT ROWID tables are modified" |
| UPDATE that changes a rowid-alias primary key (session records DELETE+INSERT) | 2 | 1 | 1 | measured |

Combined with the NULL `INTEGER PRIMARY KEY DESC` row, which passes the static pre-check at `changeset_schema.go:140`, the probes give:

- `Test_V3_NullDescOffsetByReplace`: `INSERT INTO foo(k,v) VALUES(NULL,'lost'); REPLACE INTO bar VALUES(2,'k','new')`. ops=2, Δtotal=2, touched=2, so the capture is accepted. Both nodes have `foo: []`; the statement path has `[[1,null,"lost"]]`.
- `Test_V3_NullDescOffsetByWithoutRowid`: ops=1 equals touched=1. Lost.
- `Test_V3_NullDescOffsetByPKUpdate`: ops=2 equals both counts. Lost.
- `Test_V3_NullDescOffsetByTrigger`: **one statement**, `INSERT INTO foo(k,v) VALUES(NULL,'lost')`. An AFTER INSERT trigger writes a WITHOUT ROWID audit table. ops=1 equals touched=1. Lost.
- Store level, 2 nodes, flag on:
  - `Test_V3_StoreNullDescOffsetByTrigger`: the client gets `rows_affected:1`, err=nil. `foo` is empty on the Leader and on the follower.
  - `Test_V3_StoreNullDescOffsetByReplace`: the same result.

The trigger case needs no attacker. A schema with an audit table that is WITHOUT ROWID loses every NULL-keyed insert silently.

The PR body (line 16 of the round-3 body) says these cases "give a count that matches neither". That holds for each case alone, not for their sum.

Fix:
- Check per row, not per sum. The session/changeset can be compared with a per-(table, PK) set built from the preupdate stream. Alternatively, require **both** counts to be explained: count WITHOUT ROWID and REPLACE deletes explicitly.
- Fix `changeset_schema.go:140` independently: ask SQLite whether the column is the rowid alias, for example by checking for the absence of an `origin='pk'` index in `pragma_index_list`.

### N2. HIGH: a client can stop every follower (quorum loss), because apply conflicts are now fatal. CONFIRMED (store, 2 nodes)

The statement path is still used for non-capturable requests, and it still replicates non-deterministic SQL. `randomblob(?)` with a bound parameter is never rewritten (`store/changeset_test.go:83-86`). A DDL-led request is not capturable (`db/changeset.go:72-75`).

`Test_V3_StoreClientKillsFollowers` sends two requests:
1. `[CREATE TABLE IF NOT EXISTS z(...), INSERT INTO foo(id,b) VALUES(1, randomblob(?))]`. The Leader stores `yRi5HyJGqxs=` and the follower stores `jvuudHjW1+Y=`.
2. `DELETE FROM foo WHERE id=1`. This is captured. The Leader applies it and the client gets `rows_affected:1`. The follower logs `fatal error applying command at index 7 … changeset conflicts with database contents (kind 1, rc 4)` and the process exits (`store/store.go:2642` via `command_processor.go:98-99`).

With 3 nodes, both followers exit and the Leader loses quorum.

On restart, a follower replays the log. The `randomblob` entry is re-evaluated, or the diverged state is restored from the node's own snapshot. Either way the DELETE conflicts again, so the node crash-loops until it is re-seeded from the Leader. This crash loop is PLAUSIBLE, not run.

Before this round, the same input produced a logged divergence. Now it is a cluster outage that any client can trigger.

Same class, PLAUSIBLE: `-fk` is a per-node flag. A Leader with FK enforcement off reports `deferredFKs()==0`, so it captures an orphan insert. Followers with `-fk` on hit a FOREIGN_KEY conflict and exit.

Fix: close the non-deterministic fallback first (refuse instead of falling back, or route DDL-led and rollback_on_error requests through capture). Until then, a fatal conflict turns a known divergence into an outage. At minimum, evict or fence the node rather than crash-loop it.

### N3. LOW: AUTOINCREMENT high-water mark lost via an offset. CONFIRMED (db)

`Test_V3_AutoincOffsetByWithoutRowid`: `INSERT INTO foo(v) …; DELETE FROM foo; INSERT INTO w …` (w is WITHOUT ROWID). ops=1 equals touched=1, so the capture is accepted. `sqlite_sequence` is `[]` on every node; the statement path has `seq=1`. The nodes agree with each other; the never-reuse guarantee is broken.

### N4. LOW: a primary-key swap through a temporary value leaves different rowids than the statement path. CONFIRMED (db), nodes agree

`Test_V3_UniqueCycleAndPKSwap` (TEXT PK table): the changeset nodes hold `[[1,"x","2"],[2,"y","1"]]`, while the statement path holds `[[1,"y","1"],[2,"x","2"]]`. The logical content is equal; only the hidden rowid differs. This matters only for a node that ever mixes the two paths for the same history.

## Checked, no defect

- **UNIQUE swap and 3-cycle across statements** (`Test_V3_UniqueSwap`, `Test_V3_UniqueCycleAndPKSwap`): applied without conflict on both nodes. The session's constraint retry resolves them, so this is not a client-triggerable fatal conflict.
- **`total_changes` semantics** (`Test_V3_CountSemantics`): the measurements match the docs. Triggers are counted (+2 for an insert plus its trigger), FK CASCADE and SET NULL are counted (Δ=3 and 2), and REPLACE deletes are not counted. UPSERT and same-PK REPLACE give 1/1/1 and are captured identically. INSERT plus DELETE of different rows gives 2/2/2 and is identical.
- **The update hook skips WITHOUT ROWID tables**: CONFIRMED (touched=0). The PR relies on the Δtotal form for these tables, and that is exactly what makes N1's touched-form bypass possible.
- **The deferred-FK fallback is deterministic**: it is a Leader-only decision (`db/changeset.go:248-252`, `:262-265`), and the statement path it chooses runs identically on nodes with the same `-fk`. The only hazard is per-node `-fk` disagreement (N2).

## Not examined

- Restart crash-loop reproduction (N2's second half).
- Non-voters.
- The race detector.
- e2e.
- Sessions with `sqlite_sequence` through other routes.
