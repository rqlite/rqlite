package db

import (
	"os"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rqlite/rqlite/v9/command"
	"github.com/rqlite/rqlite/v9/command/proto"
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
	hook := func(ev *proto.CDCEvent) error {
		count.Add(1)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, true); err != nil {
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
	if err := db.RegisterPreUpdateHook(nil, nil, true); err != nil {
		t.Fatalf("error unregistering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if count.Load() != 13 {
		t.Fatalf("expected count 8, got %d", count.Load())
	}
}

func Test_Preupdate_AllTypes(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, `CREATE TABLE foo (
		id INTEGER PRIMARY KEY,
		name TEXT,
		employer VARCHAR(255),
		ssn CHAR(11),
		age INT,
		weight FLOAT,
		dob DATE,
		active BOOLEAN,
		data BLOB)`)

	for _, tt := range []struct {
		name string
		sql  string
		ev   *proto.CDCEvent
	}{
		{
			name: "INSERT new row",
			sql: `INSERT INTO foo(id, name, employer, ssn, age, weight, dob, active, data) VALUES(
			5,
			"fiona",
			"Acme",
			NULL,
			21,
			167.3,
			'1990-01-02',
			true,
			x'010203')`,
			ev: &proto.CDCEvent{
				Table:    "foo",
				Op:       proto.CDCEvent_INSERT,
				NewRowId: 5,
				NewRow: &proto.CDCRow{
					Values: []*proto.CDCValue{
						{Value: &proto.CDCValue_I{I: 5}},
						{Value: &proto.CDCValue_S{S: "fiona"}},
						{Value: &proto.CDCValue_S{S: "Acme"}},
						{Value: nil},
						{Value: &proto.CDCValue_I{I: 21}},
						{Value: &proto.CDCValue_D{D: 167.3}},
						{Value: &proto.CDCValue_S{S: "1990-01-02"}},
						{Value: &proto.CDCValue_I{I: 1}},
						{Value: &proto.CDCValue_Y{Y: []byte{1, 2, 3}}},
					},
				},
			},
		},
		{
			name: "UPDATE single column",
			sql: `UPDATE foo SET
			name="declan"
			WHERE id=5`,
			ev: &proto.CDCEvent{
				Table:    "foo",
				Op:       proto.CDCEvent_UPDATE,
				OldRowId: 5,
				NewRowId: 5,
				OldRow: &proto.CDCRow{
					Values: []*proto.CDCValue{
						{Value: &proto.CDCValue_I{I: 5}},
						{Value: &proto.CDCValue_S{S: "fiona"}},
						{Value: &proto.CDCValue_S{S: "Acme"}},
						{Value: nil},
						{Value: &proto.CDCValue_I{I: 21}},
						{Value: &proto.CDCValue_D{D: 167.3}},
						{Value: &proto.CDCValue_S{S: "1990-01-02"}},
						{Value: &proto.CDCValue_I{I: 1}},
						{Value: &proto.CDCValue_Y{Y: []byte{1, 2, 3}}},
					},
				},
				NewRow: &proto.CDCRow{
					Values: []*proto.CDCValue{
						{Value: &proto.CDCValue_I{I: 5}},
						{Value: &proto.CDCValue_S{S: "declan"}},
						{Value: &proto.CDCValue_S{S: "Acme"}},
						{Value: nil},
						{Value: &proto.CDCValue_I{I: 21}},
						{Value: &proto.CDCValue_D{D: 167.3}},
						{Value: &proto.CDCValue_S{S: "1990-01-02"}},
						{Value: &proto.CDCValue_I{I: 1}},
						{Value: &proto.CDCValue_Y{Y: []byte{1, 2, 3}}},
					},
				},
			},
		},
		{
			name: "UPDATE all columns",
			sql: `UPDATE foo SET
			name="fiona2",
			employer="Acme2",
			ssn="123-45-6789",
			age=22,
			weight=170.1,
			dob='1991-02-03',
			active=false,
			data=x'040506'
			WHERE id=5`,
			ev: &proto.CDCEvent{
				Table:    "foo",
				Op:       proto.CDCEvent_UPDATE,
				OldRowId: 5,
				NewRowId: 5,
				OldRow: &proto.CDCRow{
					Values: []*proto.CDCValue{
						{Value: &proto.CDCValue_I{I: 5}},
						{Value: &proto.CDCValue_S{S: "declan"}},
						{Value: &proto.CDCValue_S{S: "Acme"}},
						{Value: nil},
						{Value: &proto.CDCValue_I{I: 21}},
						{Value: &proto.CDCValue_D{D: 167.3}},
						{Value: &proto.CDCValue_S{S: "1990-01-02"}},
						{Value: &proto.CDCValue_I{I: 1}},
						{Value: &proto.CDCValue_Y{Y: []byte{1, 2, 3}}},
					},
				},
				NewRow: &proto.CDCRow{
					Values: []*proto.CDCValue{
						{Value: &proto.CDCValue_I{I: 5}},
						{Value: &proto.CDCValue_S{S: "fiona2"}},
						{Value: &proto.CDCValue_S{S: "Acme2"}},
						{Value: &proto.CDCValue_S{S: "123-45-6789"}},
						{Value: &proto.CDCValue_I{I: 22}},
						{Value: &proto.CDCValue_D{D: 170.1}},
						{Value: &proto.CDCValue_S{S: "1991-02-03"}},
						{Value: &proto.CDCValue_I{I: 0}},
						{Value: &proto.CDCValue_Y{Y: []byte{4, 5, 6}}},
					},
				},
			},
		},
		{
			name: "DELETE row",
			sql:  "DELETE FROM foo WHERE id=5",
			ev: &proto.CDCEvent{
				Table:    "foo",
				Op:       proto.CDCEvent_DELETE,
				OldRowId: 5,
				OldRow: &proto.CDCRow{
					Values: []*proto.CDCValue{
						{Value: &proto.CDCValue_I{I: 5}},
						{Value: &proto.CDCValue_S{S: "fiona2"}},
						{Value: &proto.CDCValue_S{S: "Acme2"}},
						{Value: &proto.CDCValue_S{S: "123-45-6789"}},
						{Value: &proto.CDCValue_I{I: 22}},
						{Value: &proto.CDCValue_D{D: 170.1}},
						{Value: &proto.CDCValue_S{S: "1991-02-03"}},
						{Value: &proto.CDCValue_I{I: 0}},
						{Value: &proto.CDCValue_Y{Y: []byte{4, 5, 6}}},
					},
				},
			},
		},
	} {
		var wg sync.WaitGroup
		if err := db.RegisterPreUpdateHook(func(ev *proto.CDCEvent) error {
			defer wg.Done()

			if exp, got := tt.ev.Table, ev.Table; exp != got {
				t.Fatalf("test %s: expected table %s, got %s", tt.name, exp, got)
			}
			if exp, got := tt.ev.Op, ev.Op; exp != got {
				t.Fatalf("test %s: expected operation %s, got %s", tt.name, exp, got)
			}

			// Old row checks.
			if tt.ev.OldRowId != 0 {
				if exp, got := tt.ev.OldRowId, ev.OldRowId; exp != got {
					t.Fatalf("test %s: expected old Row ID %d, got %d", tt.name, exp, got)
				}
			}
			if tt.ev.OldRow != nil {
				if ev.OldRow == nil {
					t.Fatalf("test %s: exp non-nil new row, got nil new row", tt.name)
				}
				if len(tt.ev.OldRow.Values) != len(ev.OldRow.Values) {
					t.Fatalf("test %s: exp %d old values, got %d values", tt.name, len(tt.ev.OldRow.Values), len(ev.OldRow.Values))
				}
				for i := range tt.ev.OldRow.Values {
					if !command.CDCValueEqual(tt.ev.OldRow.Values[i], ev.OldRow.Values[i]) {
						t.Fatalf("test %s: exp old value at index %d (%v) does not equal got old value at index %d (%v)",
							tt.name, i, tt.ev.OldRow.Values[i], i, ev.OldRow.Values[i])
					}
				}
			}

			// New row checks.
			if tt.ev.NewRowId != 0 {
				if exp, got := tt.ev.NewRowId, ev.NewRowId; exp != got {
					t.Fatalf("test %s: expected new Row ID %d, got %d", tt.name, exp, got)
				}
			}
			if tt.ev.NewRow != nil {
				if ev.NewRow == nil {
					t.Fatalf("test %s: exp non-nil new row, got nil new row", tt.name)
				}
				if len(tt.ev.NewRow.Values) != len(ev.NewRow.Values) {
					t.Fatalf("test %s: exp %d new values, got %d values", tt.name, len(tt.ev.NewRow.Values), len(ev.NewRow.Values))
				}
				for i := range tt.ev.NewRow.Values {
					if !command.CDCValueEqual(tt.ev.NewRow.Values[i], ev.NewRow.Values[i]) {
						t.Fatalf("test %s: exp new value at index %d (%v) does not equal got new value at index %d (%v)",
							tt.name, i, tt.ev.NewRow.Values[i], i, ev.NewRow.Values[i])
					}
				}
			}
			return nil
		}, nil, false); err != nil {
			t.Fatalf("error registering preupdate hook: %s", err)
		}

		wg.Add(1)
		mustExecute(db, tt.sql)
		wg.Wait()
	}

}

func Test_Preupdate_Basic_Regex(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening database")
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	mustExecute(db, "CREATE TABLE foobar (id INTEGER PRIMARY KEY, name TEXT)")

	count := &atomic.Int32{}
	hook := func(ev *proto.CDCEvent) error {
		count.Add(1)
		return nil
	}
	exp := 0

	// Match.
	exp++
	if err := db.RegisterPreUpdateHook(hook, nil, true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if exp, got := exp, count.Load(); exp != int(got) {
		t.Fatalf("expected count %d, got %d", exp, got)
	}

	// Match it twice to test memoization.
	exp += 2
	if err := db.RegisterPreUpdateHook(hook, mustRegex("foo"), true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if exp, got := exp, count.Load(); exp != int(got) {
		t.Fatalf("expected count %d, got %d", exp, got)
	}

	// No match
	if err := db.RegisterPreUpdateHook(hook, mustRegex("foobar"), true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if exp, got := exp, count.Load(); exp != int(got) {
		t.Fatalf("expected count %d, got %d", exp, got)
	}

	// No match
	if err := db.RegisterPreUpdateHook(hook, mustRegex("^foob.*"), true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if exp, got := exp, count.Load(); exp != int(got) {
		t.Fatalf("expected count %d, got %d", exp, got)
	}

	// Two matches.
	exp += 2
	if err := db.RegisterPreUpdateHook(hook, mustRegex("^foo.*"), true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	mustExecute(db, "INSERT INTO foobar(name) VALUES('fiona')")
	if exp, got := exp, count.Load(); exp != int(got) {
		t.Fatalf("expected count %d, got %d", exp, got)
	}

	// No match
	if err := db.RegisterPreUpdateHook(hook, mustRegex("qux"), true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if exp, got := exp, count.Load(); exp != int(got) {
		t.Fatalf("expected count %d, got %d", exp, got)
	}

	// No match
	if err := db.RegisterPreUpdateHook(hook, mustRegex(" foo"), true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	mustExecute(db, "INSERT INTO foo(name) VALUES('fiona')")
	if exp, got := exp, count.Load(); exp != int(got) {
		t.Fatalf("expected count %d, got %d", exp, got)
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
	hook := func(ev *proto.CDCEvent) error {
		count.Add(1)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, true); err != nil {
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
	hook := func(ev *proto.CDCEvent) error {
		defer wg.Done()
		if ev.Table != "foo" {
			t.Fatalf("expected table foo, got %s", ev.Table)
		}
		if ev.Op != proto.CDCEvent_INSERT {
			t.Fatalf("expected operation insert, got %s", ev.Op)
		}
		if exp, got := int64(5), ev.NewRowId; exp != got {
			t.Fatalf("expected new row id %d, got %d", exp, ev.OldRowId)
		}

		if ev.OldRow != nil || ev.NewRow != nil {
			t.Fatalf("expected no old row and new row data")
		}
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "INSERT INTO foo(id, name, age) VALUES(5, 'fiona', 2.4)")
	wg.Wait()

	// Update a row.
	hook = func(ev *proto.CDCEvent) error {
		defer wg.Done()
		if ev.Table != "foo" {
			t.Fatalf("expected table foo, got %s", ev.Table)
		}
		if ev.Op != proto.CDCEvent_UPDATE {
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
	if err := db.RegisterPreUpdateHook(hook, nil, true); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "UPDATE foo SET name='fiona2' WHERE id=5")
	wg.Wait()

	// Delete a row.
	hook = func(ev *proto.CDCEvent) error {
		defer wg.Done()
		if ev.Table != "foo" {
			t.Fatalf("expected table foo, got %s", ev.Table)
		}
		if ev.Op != proto.CDCEvent_DELETE {
			t.Fatalf("expected operation delete, got %s", ev.Op)
		}
		if exp, got := int64(5), ev.OldRowId; exp != got {
			t.Fatalf("expected old row id %d, got %d", exp, ev.OldRowId)
		}

		if ev.OldRow != nil || ev.NewRow != nil {
			t.Fatalf("expected no old row and new row data")
		}
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, true); err != nil {
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
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT UNIQUE, age FLOAT)")

	/////////////////////////////////////////////////////////////////
	// Insert a row, with an explicit ID.
	var wg sync.WaitGroup
	hook := func(got *proto.CDCEvent) error {
		defer wg.Done()
		exp := &proto.CDCEvent{
			Table:    "foo",
			Op:       proto.CDCEvent_INSERT,
			NewRowId: 5,
			OldRow:   nil,
			NewRow: &proto.CDCRow{
				Values: []*proto.CDCValue{
					{Value: &proto.CDCValue_I{I: 5}},
					{Value: &proto.CDCValue_S{S: "fiona"}},
					{Value: &proto.CDCValue_D{D: 2.4}},
				},
			},
		}
		compareEvents(t, exp, got)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "INSERT INTO foo(id, name, age) VALUES(5, 'fiona', 2.4)")
	wg.Wait()

	/////////////////////////////////////////////////////////////////
	// Insert a row with more complex data
	hook = func(got *proto.CDCEvent) error {
		defer wg.Done()
		exp := &proto.CDCEvent{
			Table:    "foo",
			Op:       proto.CDCEvent_INSERT,
			NewRowId: 20,
			OldRow:   nil,
			NewRow: &proto.CDCRow{
				Values: []*proto.CDCValue{
					{Value: &proto.CDCValue_I{I: 20}},
					{Value: &proto.CDCValue_S{S: "😃💁 People 大鹿"}},
					{Value: &proto.CDCValue_D{D: 1.23}},
				},
			},
		}
		compareEvents(t, exp, got)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "INSERT INTO foo(id, name, age) VALUES(20, '😃💁 People 大鹿', 1.23)")
	wg.Wait()

	/////////////////////////////////////////////////////////////////
	// Insert a row, adding subset of columns
	hook = func(got *proto.CDCEvent) error {
		defer wg.Done()
		exp := &proto.CDCEvent{
			Table:    "foo",
			Op:       proto.CDCEvent_INSERT,
			NewRowId: 6,
			OldRow:   nil,
			NewRow: &proto.CDCRow{
				Values: []*proto.CDCValue{
					{Value: &proto.CDCValue_I{I: 6}},
					{Value: nil},
					{Value: &proto.CDCValue_D{D: 3.7}},
				},
			},
		}
		compareEvents(t, exp, got)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "INSERT INTO foo(id, age) VALUES(6, 3.7)")
	wg.Wait()

	/////////////////////////////////////////////////////////////////
	// Update a row.
	hook = func(got *proto.CDCEvent) error {
		defer wg.Done()
		exp := &proto.CDCEvent{
			Table:    "foo",
			Op:       proto.CDCEvent_UPDATE,
			OldRowId: 5,
			NewRowId: 5,
			OldRow: &proto.CDCRow{
				Values: []*proto.CDCValue{
					{Value: &proto.CDCValue_I{I: 5}},
					{Value: &proto.CDCValue_S{S: "fiona"}},
					{Value: &proto.CDCValue_D{D: 2.4}},
				},
			},
			NewRow: &proto.CDCRow{
				Values: []*proto.CDCValue{
					{Value: &proto.CDCValue_I{I: 5}},
					{Value: &proto.CDCValue_S{S: "fiona2"}},
					{Value: &proto.CDCValue_D{D: 2.4}},
				},
			},
		}
		compareEvents(t, exp, got)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "UPDATE foo SET name='fiona2' WHERE id=5")
	wg.Wait()

	// Delete a row.
	hook = func(got *proto.CDCEvent) error {
		defer wg.Done()
		exp := &proto.CDCEvent{
			Table:    "foo",
			Op:       proto.CDCEvent_DELETE,
			OldRowId: 5,
			OldRow: &proto.CDCRow{
				Values: []*proto.CDCValue{
					{Value: &proto.CDCValue_I{I: 5}},
					{Value: &proto.CDCValue_S{S: "fiona2"}},
					{Value: &proto.CDCValue_D{D: 2.4}},
				},
			},
			NewRow: nil,
		}
		compareEvents(t, exp, got)
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, false); err != nil {
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
	hook := func(got *proto.CDCEvent) error {
		defer wg.Done()
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, false); err != nil {
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
	hook := func(got *proto.CDCEvent) error {
		defer wg.Done()
		return nil
	}
	if err := db.RegisterPreUpdateHook(hook, nil, false); err != nil {
		t.Fatalf("error registering preupdate hook")
	}
	wg.Add(1)
	mustExecute(db, "BEGIN")
	mustExecute(db, "INSERT INTO foo(id, name) VALUES(1, 'fiona')")
	mustExecute(db, "ROLLBACK")
	wg.Wait()
}

func compareEvents(t *testing.T, exp, got *proto.CDCEvent) {
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

func mustRegex(s string) *regexp.Regexp {
	r, err := regexp.Compile(s)
	if err != nil {
		panic(err)
	}
	return r
}
