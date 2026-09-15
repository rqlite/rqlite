package db

import (
	"os"
	"sync/atomic"
	"testing"
)

func Test_CommitHook(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	count := &atomic.Int32{}
	hook := func() bool {
		count.Add(1)
		return true
	}
	if err := db.RegisterCommitHook(hook); err != nil {
		t.Fatalf("error registering commit hook")
	}

	// A select should not trigger the hook and a basic insert should trigger the hook.
	mustQuery(db, "SELECT * FROM foo")
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if count.Load() != 1 {
		t.Fatalf("expected count 1, got %d", count.Load())
	}
}

// Test_CommitHook_Rollback demonstrates that the Commit hook is not called for a
// transaction which is rolled back.
func Test_CommitHook_Rollback(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	count := &atomic.Int32{}
	hook := func() bool {
		count.Add(1)
		return false
	}
	if err := db.RegisterCommitHook(hook); err != nil {
		t.Fatalf("error registering commit hook")
	}

	r, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing statement: %s", err.Error())
	}
	if r[0].GetError() == "" {
		t.Fatal("expected error in response body due to rollback, got nil")
	}
	if count.Load() != 1 {
		t.Fatalf("expected count 1, got %d", count.Load())
	}

	// Query the count in the table, should be zero.
	q, err := db.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[0]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

// Test_CommitHook_Tx demonstrates that the Commit hook is not called for a
// transaction which is rolled back.
func Test_CommitHook_Tx(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	count := &atomic.Int32{}
	hook := func() bool {
		count.Add(1)
		return true
	}
	if err := db.RegisterCommitHook(hook); err != nil {
		t.Fatalf("error registering commit hook")
	}
	mustExecute(db, "BEGIN")
	mustExecute(db, "INSERT INTO foo(id, name) VALUES(1, 'fiona')")
	mustExecute(db, "ROLLBACK")

	if count.Load() != 0 {
		t.Fatalf("expected count 0, got %d", count.Load())
	}
}
