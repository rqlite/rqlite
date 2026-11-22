package store

import (
	"context"
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// Regression tests for two independent reviews of the prototype, at the cluster
// level. The invariant: every node holds the same data, and a write the client
// is told succeeded is on every node.

func execOn(t *testing.T, s *Store, stmts ...string) {
	t.Helper()
	if _, _, err := s.Execute(context.Background(), executeRequestFromStrings(stmts, false, false)); err != nil {
		t.Fatalf("execute %q: %s", stmts, err)
	}
}

func wantConverged(t *testing.T, stores []*Store) {
	t.Helper()
	waitConverged(t, stores)
	want := dumpStore(t, stores[0])
	for i, s := range stores[1:] {
		if got := dumpStore(t, s); got != want {
			t.Fatalf("node %d differs from the Leader:\nleader:\n%snode:\n%s", i+1, want, got)
		}
	}
}

// A second statement in one SQL string used to commit on the Leader only.
func Test_Review_StoreMultiStatementConverges(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	execOn(t, stores[0], `CREATE TABLE foo (v TEXT)`)
	execOn(t, stores[0], `INSERT INTO foo VALUES('a'); COMMIT; INSERT INTO foo VALUES('b')`)
	wantConverged(t, stores)
}

// One request with INSERT OR ROLLBACK followed by another write used to leave the
// Leader with one more row than its followers.
func Test_Review_StoreOrRollbackConverges(t *testing.T) {
	stores := mustNewCluster(t, 3, true)
	execOn(t, stores[0], "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	execOn(t, stores[0], "CREATE TABLE bar (v TEXT)")
	execOn(t, stores[0], "INSERT INTO foo VALUES(1)")
	execOn(t, stores[0], "INSERT OR ROLLBACK INTO foo VALUES(1)", "INSERT INTO bar VALUES('once')")
	wantConverged(t, stores)
	execOn(t, stores[0], "DELETE FROM bar")
	wantConverged(t, stores)
}

// VACUUM renumbers the rowids of a table without an explicit key, on one node.
func Test_Review_StoreVacuumRowidConverges(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	execOn(t, stores[0], `CREATE TABLE foo (v TEXT)`)
	execOn(t, stores[0], `INSERT INTO foo VALUES('a'),('b'),('c'),('d')`)
	execOn(t, stores[0], `DELETE FROM foo WHERE v IN ('a','c')`)
	waitConverged(t, stores)
	if err := stores[1].Vacuum(); err != nil {
		t.Fatal(err)
	}
	execOn(t, stores[0], `UPDATE foo SET v = 'z' WHERE v = 'd'`)
	execOn(t, stores[0], `INSERT INTO foo VALUES('e')`)
	waitConverged(t, stores)
	// A table without a key is not captured, so it replicates as SQL text and no
	// changeset addresses a row by a rowid that VACUUM has renumbered. The rows
	// are the same on both nodes even though their rowids are not.
	var got [2]string
	for i, s := range stores {
		q := queryRequestFromString(`SELECT v FROM foo ORDER BY v`, false, false, false)
		q.Level = proto.ConsistencyLevel_NONE
		r, _, _, err := s.Query(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		got[i] = asJSON(r)
	}
	if got[0] != got[1] {
		t.Fatalf("rows differ:\n%s\nvs\n%s", got[0], got[1])
	}
}

// A write into a column the session extension does not record is still stored
// on every node.
func Test_Review_StoreNullPKStored(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	execOn(t, stores[0], `CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT)`)
	execOn(t, stores[0], `INSERT INTO t VALUES(NULL, 'x')`)
	wantConverged(t, stores)
	q := queryRequestFromString(`SELECT count(*) FROM t`, false, false, false)
	q.Level = proto.ConsistencyLevel_NONE
	r, _, _, err := stores[1].Query(context.Background(), q)
	if err != nil || r[0].Values[0].Parameters[0].GetI() != 1 {
		t.Fatalf("the row is not on the follower: %v %s", err, asJSON(r))
	}
}
