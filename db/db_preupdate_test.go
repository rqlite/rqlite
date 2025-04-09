package db

import (
	"os"
	"reflect"
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
	hook := func(ev *command.CDCEvent) error {
		count.Add(1)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, true); err != nil {
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

	// Delete all rows, make sure the hook is triggered 5 times.
	r := mustQuery(db, "SELECT COUNT(*) FROM foo")
	if exp, got := int64(5), r[0].Values[0].Parameters[0].GetI(); exp != got {
		t.Fatalf("expected count %d, got %d", exp, got)
	}
	mustExecute(db, "DELETE FROM foo")
	if count.Load() != 13 {
		t.Fatalf("expected count 13, got %d", count.Load())
	}

	// Create table shouldn't trigger the hook.
	mustExecute(db, "CREATE TABLE bar (id INTEGER PRIMARY KEY, name TEXT)")
	if count.Load() != 13 {
		t.Fatalf("expected count 13, got %d", count.Load())
	}

	// Unregister the hook, insert a row, and make sure the hook is not triggered.
	if err := db.RegisterPreUpdateHook(nil, true); err != nil {
		t.Fatalf("error unregistering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if count.Load() != 13 {
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
	hook := func(ev *command.CDCEvent) error {
		count.Add(1)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, true); err != nil {
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

func Test_Preupdate_RowIDs(t *testing.T) {
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
	hook := func(ev *command.CDCEvent) error {
		defer wg.Done()
		if ev.Table != "foo" {
			t.Fatalf("expected table foo, got %s", ev.Table)
		}
		if ev.Op != command.CDCEvent_INSERT {
			t.Fatalf("expected operation insert, got %s", ev.Op)
		}
		if exp, got := int64(5), ev.OldRowId; exp != got {
			t.Fatalf("expected old row id %d, got %d", exp, ev.OldRowId)
		}
		if exp, got := int64(5), ev.NewRowId; exp != got {
			t.Fatalf("expected new row id %d, got %d", exp, ev.OldRowId)
		}

		if ev.OldRow != nil || ev.NewRow != nil {
			t.Fatalf("expected no old row and new row data")
		}
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "INSERT INTO foo(id, name, age) VALUES(5, 'fiona', 2.4)")
	wg.Wait()

	// Update a row.
	hook = func(ev *command.CDCEvent) error {
		defer wg.Done()
		if ev.Table != "foo" {
			t.Fatalf("expected table foo, got %s", ev.Table)
		}
		if ev.Op != command.CDCEvent_UPDATE {
			t.Fatalf("expected operation update, got %s", ev.Op)
		}
		if exp, got := int64(5), ev.OldRowId; exp != got {
			t.Fatalf("expected old row id %d, got %d", exp, ev.OldRowId)
		}
		if exp, got := int64(5), ev.NewRowId; exp != got {
			t.Fatalf("expected new row id %d, got %d", exp, ev.OldRowId)
		}

		if ev.OldRow != nil || ev.NewRow != nil {
			t.Fatalf("expected no old row and new row data")
		}
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "UPDATE foo SET name='fiona2' WHERE id=5")
	wg.Wait()

	// Delete a row.
	hook = func(ev *command.CDCEvent) error {
		defer wg.Done()
		if ev.Table != "foo" {
			t.Fatalf("expected table foo, got %s", ev.Table)
		}
		if ev.Op != command.CDCEvent_DELETE {
			t.Fatalf("expected operation delete, got %s", ev.Op)
		}
		if exp, got := int64(5), ev.OldRowId; exp != got {
			t.Fatalf("expected old row id %d, got %d", exp, ev.OldRowId)
		}
		if exp, got := int64(5), ev.NewRowId; exp != got {
			t.Fatalf("expected new row id %d, got %d", exp, ev.OldRowId)
		}

		if ev.OldRow != nil || ev.NewRow != nil {
			t.Fatalf("expected no old row and new row data")
		}
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "DELETE FROM foo WHERE id=5")
	wg.Wait()
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

	/////////////////////////////////////////////////////////////////
	// Insert a row, with an explicit ID.
	var wg sync.WaitGroup
	hook := func(got *command.CDCEvent) error {
		defer wg.Done()
		exp := &command.CDCEvent{
			Table:    "foo",
			Op:       command.CDCEvent_INSERT,
			OldRowId: 5,
			NewRowId: 5,
			OldRow:   nil,
			NewRow: &command.CDCRow{
				Values: []*command.CDCValue{
					{Value: &command.CDCValue_I{I: 5}},
					{Value: &command.CDCValue_S{S: "fiona"}},
					{Value: &command.CDCValue_D{D: 2.4}},
				},
			},
		}
		compareEvents(t, exp, got)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "INSERT INTO foo(id, name, age) VALUES(5, 'fiona', 2.4)")
	wg.Wait()

	/////////////////////////////////////////////////////////////////
	// Insert a row with more complex data
	hook = func(got *command.CDCEvent) error {
		defer wg.Done()
		exp := &command.CDCEvent{
			Table:    "foo",
			Op:       command.CDCEvent_INSERT,
			OldRowId: 20,
			NewRowId: 20,
			OldRow:   nil,
			NewRow: &command.CDCRow{
				Values: []*command.CDCValue{
					{Value: &command.CDCValue_I{I: 20}},
					{Value: &command.CDCValue_S{S: "😃💁 People 大鹿"}},
					{Value: &command.CDCValue_D{D: 1.23}},
				},
			},
		}
		compareEvents(t, exp, got)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "INSERT INTO foo(id, name, age) VALUES(20, '😃💁 People 大鹿', 1.23)")
	wg.Wait()

	/////////////////////////////////////////////////////////////////
	// Insert a row, adding subset of columns
	hook = func(got *command.CDCEvent) error {
		defer wg.Done()
		exp := &command.CDCEvent{
			Table:    "foo",
			Op:       command.CDCEvent_INSERT,
			OldRowId: 6,
			NewRowId: 6,
			OldRow:   nil,
			NewRow: &command.CDCRow{
				Values: []*command.CDCValue{
					{Value: &command.CDCValue_I{I: 6}},
					nil,
					{Value: &command.CDCValue_D{D: 3.7}},
				},
			},
		}
		compareEvents(t, exp, got)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "INSERT INTO foo(id, age) VALUES(6, 3.7)")
	wg.Wait()

	/////////////////////////////////////////////////////////////////
	// Update a row.
	hook = func(got *command.CDCEvent) error {
		defer wg.Done()
		exp := &command.CDCEvent{
			Table:    "foo",
			Op:       command.CDCEvent_UPDATE,
			OldRowId: 5,
			NewRowId: 5,
			OldRow: &command.CDCRow{
				Values: []*command.CDCValue{
					{Value: &command.CDCValue_I{I: 5}},
					{Value: &command.CDCValue_S{S: "fiona"}},
					{Value: &command.CDCValue_D{D: 2.4}},
				},
			},
			NewRow: &command.CDCRow{
				Values: []*command.CDCValue{
					{Value: &command.CDCValue_I{I: 5}},
					{Value: &command.CDCValue_S{S: "fiona2"}},
					{Value: &command.CDCValue_D{D: 2.4}},
				},
			},
		}
		compareEvents(t, exp, got)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "UPDATE foo SET name='fiona2' WHERE id=5")
	wg.Wait()

	// Delete a row.
	hook = func(got *command.CDCEvent) error {
		defer wg.Done()
		exp := &command.CDCEvent{
			Table:    "foo",
			Op:       command.CDCEvent_DELETE,
			OldRowId: 5,
			NewRowId: 5,
			OldRow: &command.CDCRow{
				Values: []*command.CDCValue{
					{Value: &command.CDCValue_I{I: 5}},
					{Value: &command.CDCValue_S{S: "fiona2"}},
					{Value: &command.CDCValue_D{D: 2.4}},
				},
			},
			NewRow: nil,
		}
		compareEvents(t, exp, got)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "DELETE FROM foo WHERE id=5")
	wg.Wait()
}

// Test_Preupdate_Multi tests that the preupdate hook is called for multiple
// deletes.
func Test_Preupdate_Multi(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	mustExecute(db, "INSERT INTO foo(id, name) VALUES(1, 'fiona')")
	mustExecute(db, "INSERT INTO foo(id, name) VALUES(2, 'fiona')")

	var wg sync.WaitGroup
	hook := func(got *command.CDCEvent) error {
		defer wg.Done()
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(2)
	mustExecute(db, "DELETE FROM foo")
	wg.Wait()
}

// Test_Preupdate_Tx demostrates that the preupdate hook is called for
// a transaction which is rolled back.
func Test_Preupdate_Tx(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	var wg sync.WaitGroup
	hook := func(got *command.CDCEvent) error {
		defer wg.Done()
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "BEGIN")
	mustExecute(db, "INSERT INTO foo(id, name) VALUES(1, 'fiona')")
	mustExecute(db, "ROLLBACK")
	wg.Wait()
}

func compareEvents(t *testing.T, exp, got *command.CDCEvent) {
	t.Helper()
	if exp, got := exp.Table, got.Table; exp != got {
		t.Fatalf("expected table %s, got %s", exp, got)
	}
	if exp, got := exp.Op, got.Op; exp != got {
		t.Fatalf("expected operation %s, got %s", exp, got)
	}

	if exp, got := exp.OldRowId, got.OldRowId; exp != got {
		t.Fatalf("expected old Row ID %d, got %d", exp, got)
	}
	if exp, got := exp.NewRowId, got.NewRowId; exp != got {
		t.Fatalf("expected new Row ID %d, got %d", exp, got)
	}

	if exp.OldRow == nil && got.OldRow != nil {
		t.Fatalf("exp old row is nil, but got non-nil old row")
	}
	if exp.NewRow == nil && got.NewRow != nil {
		t.Fatalf("exp new row is nil, but got non-nil new row")
	}

	if exp.OldRow != nil && got.OldRow == nil {
		t.Fatalf("exp old row is not nil, but got nil old row")
	}
	if exp.NewRow != nil && got.NewRow == nil {
		t.Fatalf("exp new row is not nil, but got nil new row")
	}

	if exp.OldRow != nil {
		if exp, got := len(exp.OldRow.Values), len(got.OldRow.Values); exp != got {
			t.Fatalf("exp %d old values, got %d values", exp, got)
		}
		for i := range exp.OldRow.Values {
			if !reflect.DeepEqual(exp.OldRow.Values[i], got.OldRow.Values[i]) {
				t.Fatalf("exp old value at index %d (%v) does not equal got old value at index %d (%v)",
					i, exp.OldRow.Values[i], i, got.OldRow.Values[i])
			}
		}
	}
	if exp.NewRow != nil {
		if exp, got := len(exp.NewRow.Values), len(got.NewRow.Values); exp != got {
			t.Fatalf("exp %d new values, got %d values", exp, got)
		}
		for i := range exp.NewRow.Values {
			if !reflect.DeepEqual(exp.NewRow.Values[i], got.NewRow.Values[i]) {
				t.Fatalf("exp new value at index %d (%v) does not equal got new value at index %d (%v)",
					i, exp.NewRow.Values[i], i, got.NewRow.Values[i])
			}
		}
	}

}
