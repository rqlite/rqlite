package db

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	command "github.com/rqlite/rqlite/v8/command/proto"
)

// Test_Preupdate_Basic tests the basic functionality of the preupdate hook, ensuring
// it is triggered for inserts, updates, and deletes, and not triggered for selects,
// executes that don't change anything, and when unregistered.
func Test_Preupdate_Basic(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	count := &atomic.Int32{}
	hook := func(ev *command.CDCEvent) {
		count.Add(1)
	}
	if err := db.RegisterPreUpdateHook(hook); err != nil {
		t.Fatalf("error registering preupdate hook")
	}

	// A select should not trigger the hook and a basic insert should trigger the hook.
	mustQuery(db, "SELECT * FROM foo")
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if count.Load() != 1 {
		t.Fatalf("expected count 1, got %d", count.Load())
	}

	// An update should trigger the hook, and an update that doesn't change anything
	// should not trigger the hook.
	mustExecute(db, "UPDATE foo SET name='fiona2' WHERE id=5")
	mustExecute(db, "UPDATE foo SET name='fiona2' WHERE id=1")
	if count.Load() != 2 {
		t.Fatalf("expected count 2, got %d", count.Load())
	}

	// A delete should trigger the hook.
	mustExecute(db, "DELETE FROM foo WHERE id=1")
	if count.Load() != 3 {
		t.Fatalf("expected count 3, got %d", count.Load())
	}

	// Insert 5 rows, make sure the hook is triggered 5 times.
	for i := 0; i < 5; i++ {
		mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	}
	if count.Load() != 8 {
		t.Fatalf("expected count 8, got %d", count.Load())
	}

	// Unregister the hook, insert a row, and make sure the hook is not triggered.
	if err := db.RegisterPreUpdateHook(nil); err != nil {
		t.Fatalf("error unregistering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if count.Load() != 8 {
		t.Fatalf("expected count 8, got %d", count.Load())
	}
}

// Test_Preupdate_Constraint tests that the preupdate hook is not triggered for
// inserts that violate a constraint.
func Test_Preupdate_Constraint(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")

	count := &atomic.Int32{}
	hook := func(ev *command.CDCEvent) {
		count.Add(1)
	}
	if err := db.RegisterPreUpdateHook(hook); err != nil {
		t.Fatalf("error registering preupdate hook")
	}

	// Insert a row, with an explicit ID.
	mustExecute(db, "INSERT INTO foo(id, name) VALUES(5, 'fiona')")
	if count.Load() != 1 {
		t.Fatalf("expected count 1, got %d", count.Load())
	}

	// Insert a row with the same ID, should not trigger the hook.
	r, err := db.ExecuteStringStmt("INSERT INTO foo(id, name) VALUES(5, 'fiona2')")
	if err == nil && r[0].GetError() == "" {
		t.Fatalf("expected error, got nil")
	}
	if count.Load() != 1 {
		t.Fatalf("expected count 1, got %d", count.Load())
	}

	// Ensure the hook is not triggered for a INSERT that violates a unique constraint.
	r, err = db.ExecuteStringStmt("INSERT INTO foo(id, name) VALUES(6, 'fiona')")
	if err == nil && r[0].GetError() == "" {
		t.Fatalf("expected error, got nil")
	}
	if count.Load() != 1 {
		t.Fatalf("expected count 1, got %d", count.Load())
	}
}

func Test_Preupdate_Data(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT UNIQUE, age float)")

	// Insert a row, with an explicit ID.
	var wg sync.WaitGroup
	hook := func(ev *command.CDCEvent) {
		defer wg.Done()
		if ev.Table != "foo" {
			t.Fatalf("expected table foo, got %s", ev.Table)
		}
		if ev.Op != command.CDCEvent_INSERT {
			t.Fatalf("expected operation insert, got %s", ev.Op)
		}

		if ev.OldRow != nil {
			t.Fatalf("expected no old row")
		}
		if ev.NewRow == nil {
			t.Fatalf("expected new row")
		}
		if len(ev.NewRow.Values) != 3 {
			t.Fatalf("expected 3 values, got %d", len(ev.NewRow.Values))
		}
		if exp, got := int64(5), ev.NewRow.Values[0].GetI(); exp != got {
			t.Fatalf("expected id %d, got %d", exp, got)
		}
		if exp, got := "fiona", ev.NewRow.Values[1].GetS(); exp != got {
			t.Fatalf("expected name %s, got %s", exp, got)
		}
		if exp, got := 2.4, ev.NewRow.Values[2].GetD(); exp != got {
			t.Fatalf("expected age %f, got %f", exp, got)
		}
	}
	if err := db.RegisterPreUpdateHook(hook); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "INSERT INTO foo(id, name, age) VALUES(5, 'fiona', 2.4)")
	wg.Wait()

	// Update a row.
	hook = func(ev *command.CDCEvent) {
		defer wg.Done()
		if ev.Table != "foo" {
			t.Fatalf("expected table foo, got %s", ev.Table)
		}
		if ev.Op != command.CDCEvent_UPDATE {
			t.Fatalf("expected operation update, got %s", ev.Op)
		}

		if ev.OldRow == nil {
			t.Fatalf("expected old row")
		}
		if len(ev.OldRow.Values) != 3 {
			t.Fatalf("expected 3 values, got %d", len(ev.OldRow.Values))
		}
		if exp, got := int64(5), ev.OldRow.Values[0].GetI(); exp != got {
			t.Fatalf("expected id %d, got %d", exp, got)
		}
		if exp, got := "fiona", ev.OldRow.Values[1].GetS(); exp != got {
			t.Fatalf("expected name %s, got %s", exp, got)
		}
		if exp, got := 2.4, ev.OldRow.Values[2].GetD(); exp != got {
			t.Fatalf("expected age %f, got %f", exp, got)
		}

		if ev.NewRow == nil {
			t.Fatalf("expected new row")
		}
		if len(ev.NewRow.Values) != 3 {
			t.Fatalf("expected 3 values, got %d", len(ev.NewRow.Values))
		}
		if exp, got := int64(5), ev.NewRow.Values[0].GetI(); exp != got {
			t.Fatalf("expected id %d, got %d", exp, got)
		}
		if exp, got := "fiona2", ev.NewRow.Values[1].GetS(); exp != got {
			t.Fatalf("expected name %s, got %s", exp, got)
		}
		if exp, got := 2.4, ev.NewRow.Values[2].GetD(); exp != got {
			t.Fatalf("expected age %f, got %f", exp, got)
		}
	}
	if err := db.RegisterPreUpdateHook(hook); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "UPDATE foo SET name='fiona2' WHERE id=5")
	wg.Wait()

	// Delete a row.
	hook = func(ev *command.CDCEvent) {
		defer wg.Done()
		if ev.Table != "foo" {
			t.Fatalf("expected table foo, got %s", ev.Table)
		}
		if ev.Op != command.CDCEvent_DELETE {
			t.Fatalf("expected operation delete, got %s", ev.Op)
		}

		if ev.OldRow == nil {
			t.Fatalf("expected old row")
		}
		if len(ev.OldRow.Values) != 3 {
			t.Fatalf("expected 3 values, got %d", len(ev.OldRow.Values))
		}
		if exp, got := int64(5), ev.OldRow.Values[0].GetI(); exp != got {
			t.Fatalf("expected id %d, got %d", exp, got)
		}
		if exp, got := "fiona2", ev.OldRow.Values[1].GetS(); exp != got {
			t.Fatalf("expected name %s, got %s", exp, got)
		}
		if exp, got := 2.4, ev.OldRow.Values[2].GetD(); exp != got {
			t.Fatalf("expected age %f, got %f", exp, got)
		}
		if ev.NewRow != nil {
			t.Fatalf("expected no new row")
		}
	}
	if err := db.RegisterPreUpdateHook(hook); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "DELETE FROM foo WHERE id=5")
	wg.Wait()
}
