package db

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	command "github.com/rqlite/rqlite/v8/command/proto"
)

// Test_PreupdateBasic tests the basic functionality of the preupdate hook, ensuring
// it is triggered for inserts, updates, and deletes, and not triggered for selects,
// executes that don't change anything, and when unregistered.
func Test_PreupdateBasic(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	count := &atomic.Int32{}
	var wg sync.WaitGroup
	hook := func(ev *command.CDCEvent) {
		count.Add(1)
		wg.Done()
	}
	if err := db.RegisterPreUpdateHook(hook, true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}

	// A select should not trigger the hook and a basic insert should trigger the hook.
	wg.Add(1)
	mustQuery(db, "SELECT * FROM foo")
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	wg.Wait()
	if count.Load() != 1 {
		t.Fatalf("expected count 1, got %d", count.Load())
	}

	// An update should trigger the hook, and an update that doesn't change anything
	// should not trigger the hook.
	wg.Add(1)
	mustExecute(db, "UPDATE foo SET name='fiona2' WHERE id=5")
	mustExecute(db, "UPDATE foo SET name='fiona2' WHERE id=1")
	wg.Wait()
	if count.Load() != 2 {
		t.Fatalf("expected count 2, got %d", count.Load())
	}

	// A delete should trigger the hook.
	wg.Add(1)
	mustExecute(db, "DELETE FROM foo WHERE id=1")
	wg.Wait()
	if count.Load() != 3 {
		t.Fatalf("expected count 3, got %d", count.Load())
	}

	// Insert 5 rows, make sure the hook is triggered 5 times.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	}
	wg.Wait()
	if count.Load() != 8 {
		t.Fatalf("expected count 8, got %d", count.Load())
	}

	// Unregister the hook, insert a row, and make sure the hook is not triggered.
	if err := db.RegisterPreUpdateHook(nil, false); err != nil {
		t.Fatalf("error unregistering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if count.Load() != 8 {
		t.Fatalf("expected count 8, got %d", count.Load())
	}
}
