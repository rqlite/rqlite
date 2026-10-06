package db

import (
	"context"
	"database/sql"
	"expvar"
	"fmt"
	"regexp"
	"sync/atomic"
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db/querylog"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

func Test_DefaultDriver(t *testing.T) {
	d := DefaultDriver()
	if d == nil {
		t.Fatalf("DefaultDriver returned nil")
	}
	if d.Name() != defaultDriverName {
		t.Fatalf("DefaultDriver returned incorrect name: %s", d.Name())
	}

	// Call it again, make sure it doesn't panic.
	d = DefaultDriver()
	if d == nil {
		t.Fatalf("DefaultDriver returned nil")
	}

	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithDriver(d, path, false, true)
	if err != nil {
		t.Fatalf("OpenWithDriver failed: %s", err.Error())
	}
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	q, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	if !fsutil.FileExists(db.WALPath()) {
		t.Fatalf("WAL file not created")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %s", err.Error())
	}
	if !fsutil.FileExists(db.WALPath()) {
		t.Fatalf("WAL file removed on close")
	}

	// Now, delete the WAL file, and re-open the database. The SELECT should
	// fail with "no table", proving the WAL was not checkpointed.
	if err := fsutil.Remove(db.WALPath()); err != nil {
		t.Fatalf("Failed to remove WAL file: %s", err.Error())
	}
	db, err = OpenWithDriver(d, path, false, true)
	if err != nil {
		t.Fatalf("OpenWithDriver failed: %s", err.Error())
	}

	q, err = db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: foo"}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %s", err.Error())
	}
}

func Test_CheckpointDriver(t *testing.T) {
	d := CheckpointDriver()
	if d == nil {
		t.Fatalf("CheckpointDriver returned nil")
	}
	if d.Name() != chkDriverName {
		t.Fatalf("CheckpointDriver returned incorrect name: %s", d.Name())
	}

	// Call it again, make sure it doesn't panic.
	d = CheckpointDriver()
	if d == nil {
		t.Fatalf("CheckpointDriver returned nil")
	}

	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithDriver(d, path, false, true)
	if err != nil {
		t.Fatalf("OpenWithDriver failed: %s", err.Error())
	}
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	if !fsutil.FileExists(db.WALPath()) {
		t.Fatalf("WAL file not created")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %s", err.Error())
	}
	if fsutil.FileExists(db.WALPath()) {
		t.Fatalf("WAL file not removed on close")
	}
}

func Test_NewDriver(t *testing.T) {
	name := "test-driver"
	extensions := []string{"test1", "test2"}
	d := NewDriver(name, extensions, CnkOnCloseModeEnabled)
	if d == nil {
		t.Fatalf("NewDriver returned nil")
	}
	if d.Name() != name {
		t.Fatalf("NewDriver returned incorrect name: %s", d.Name())
	}
	if len(d.Extensions()) != 2 {
		t.Fatalf("NewDriver returned incorrect extensions: %v", d.Extensions())
	}
	if d.CheckpointOnCloseMode() != CnkOnCloseModeEnabled {
		t.Fatalf("NewDriver returned incorrect checkpoint mode: %v", d.CheckpointOnCloseMode())
	}
}

// Test_Drivers_AutoCheckpointDisabled tests that every connection opened by
// every driver has automatic checkpointing disabled. rqlite must have full
// control over checkpointing, and database/sql is free to open a new
// connection at any time.
func Test_Drivers_AutoCheckpointDisabled(t *testing.T) {
	for _, d := range []*Driver{
		DefaultDriver(),
		CheckpointDriver(),
		ForeignKeyDriver(),
		NewDriver(testDriverConfigName(), nil, CnkOnCloseModeEnabled),
	} {
		path := mustTempPath()
		defer fsutil.RemoveAll(path)
		for _, readOnly := range []bool{ModeReadWrite, ModeReadOnly} {
			db, err := sql.Open(d.Name(), MakeDSN(path, readOnly, false, true))
			if err != nil {
				t.Fatalf("driver %s: failed to open database: %s", d.Name(), err)
			}
			defer db.Close()

			// Hold each connection open so the pool must create a new one each time.
			for i := 0; i < 3; i++ {
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Fatalf("driver %s: failed to get connection: %s", d.Name(), err)
				}
				defer conn.Close()
				var n int
				if err := conn.QueryRowContext(context.Background(), "PRAGMA wal_autocheckpoint").Scan(&n); err != nil {
					t.Fatalf("driver %s: failed to read autocheckpoint setting: %s", d.Name(), err)
				}
				if n != 0 {
					t.Errorf("driver %s, read-only %v, connection %d: autocheckpoint is %d, want 0", d.Name(), readOnly, i, n)
				}
			}
		}
	}
}

// A local counter for generating unique driver names.
var driverTestSeq atomic.Int64

func testDriverConfigName() string {
	return fmt.Sprintf("test-driver-config-%d", driverTestSeq.Add(1))
}

// Verifies that a DriverConfig with a QueryLogger does not interfere with
// normal database operations. Log output capture is tested in db/querylog.
func Test_NewDriverFromConfig_QueryLogOnly(t *testing.T) {
	ql := querylog.New(querylog.DefaultConfig())

	d := NewDriverFromConfig(testDriverConfigName(), &DriverConfig{
		ChkOnClose:  CnkOnCloseModeDisabled,
		QueryLogger: ql,
	})

	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithDriver(d, path, false, true)
	if err != nil {
		t.Fatalf("OpenWithDriver failed: %s", err)
	}
	defer db.Close()

	mustExecute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	mustExecute(db, "INSERT INTO t VALUES (1, 'hello')")

	rows, err := db.QueryStringStmt("SELECT val FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %s", err)
	}
	if len(rows) != 1 || len(rows[0].Values) != 1 {
		t.Fatalf("expected 1 row from SELECT, got unexpected result")
	}
}

// Verifies that a DriverConfig with nil
// QueryLogger opens and operates normally without tracing.
func Test_NewDriverFromConfig_NoQueryLog(t *testing.T) {
	d := NewDriverFromConfig(testDriverConfigName(), &DriverConfig{
		ChkOnClose:  CnkOnCloseModeDisabled,
		QueryLogger: nil,
	})
	if d.CheckpointOnCloseMode() != CnkOnCloseModeDisabled {
		t.Fatalf("expected CnkOnCloseModeDisabled, got %v", d.CheckpointOnCloseMode())
	}

	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithDriver(d, path, false, true)
	if err != nil {
		t.Fatalf("OpenWithDriver failed: %s", err)
	}
	defer db.Close()

	mustExecute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
}

// Test_NewDriverFromConfig_NoHooks verifies that a driver whose config has no
// hooks installs none: writing to a database opened with it fires nothing.
func Test_NewDriverFromConfig_NoHooks(t *testing.T) {
	d := NewDriverFromConfig(testDriverConfigName(), &DriverConfig{
		ChkOnClose: CnkOnCloseModeDisabled,
	})

	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithDriver(d, path, false, true)
	if err != nil {
		t.Fatalf("OpenWithDriver failed: %s", err)
	}
	defer db.Close()

	before := hookStats()
	mustExecute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	mustExecute(db, "INSERT INTO t VALUES (1)")
	mustExecute(db, "BEGIN; INSERT INTO t VALUES (2); ROLLBACK")
	if after := hookStats(); after != before {
		t.Fatalf("hooks fired on driver with no hooks configured: before %v, after %v", before, after)
	}
}

// Test_NewDriverFromConfig_Hooks verifies that hooks set in the config are
// installed, and are called with converted event data.
func Test_NewDriverFromConfig_Hooks(t *testing.T) {
	var preupdates, updates, commits, rollbacks int
	var lastPreupdate *proto.CDCEvent
	var lastUpdate *proto.UpdateHookEvent
	d := NewDriverFromConfig(testDriverConfigName(), &DriverConfig{
		ChkOnClose: CnkOnCloseModeDisabled,
		PreUpdateHook: func(ev *proto.CDCEvent) error {
			preupdates++
			lastPreupdate = ev
			return nil
		},
		UpdateHook: func(ev *proto.UpdateHookEvent) error {
			updates++
			lastUpdate = ev
			return nil
		},
		CommitHook: func() bool {
			commits++
			return true
		},
		RollbackHook: func() {
			rollbacks++
		},
	})

	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithDriver(d, path, false, true)
	if err != nil {
		t.Fatalf("OpenWithDriver failed: %s", err)
	}
	defer db.Close()

	// Opening a WAL-mode database runs a transaction which is rolled back, to
	// force creation of the WAL files, so the rollback hook has already fired.
	if rollbacks != 1 {
		t.Fatalf("expected 1 rollback on open, got %d", rollbacks)
	}
	rollbacks = 0

	mustExecute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

	// The schema change commits, but changes no rows.
	if preupdates != 0 || updates != 0 || commits != 1 || rollbacks != 0 {
		t.Fatalf("after CREATE TABLE: preupdates=%d, updates=%d, commits=%d, rollbacks=%d",
			preupdates, updates, commits, rollbacks)
	}

	mustExecute(db, "INSERT INTO t VALUES (1, 'fiona')")
	if preupdates != 1 || updates != 1 || commits != 2 || rollbacks != 0 {
		t.Fatalf("after INSERT: preupdates=%d, updates=%d, commits=%d, rollbacks=%d",
			preupdates, updates, commits, rollbacks)
	}
	if lastPreupdate.Table != "t" || lastPreupdate.Op != proto.CDCEvent_INSERT || lastPreupdate.NewRowId != 1 ||
		lastPreupdate.NewRow == nil || lastPreupdate.NewRow.Values[1].GetS() != "fiona" {
		t.Fatalf("unexpected preupdate event: %v", lastPreupdate)
	}
	if lastUpdate.Table != "t" || lastUpdate.Op != proto.UpdateHookEvent_INSERT || lastUpdate.RowId != 1 {
		t.Fatalf("unexpected update event: %v", lastUpdate)
	}

	mustExecute(db, "BEGIN; INSERT INTO t VALUES (2, 'declan'); ROLLBACK")
	if preupdates != 2 || updates != 2 || commits != 2 || rollbacks != 1 {
		t.Fatalf("after rolled-back INSERT: preupdates=%d, updates=%d, commits=%d, rollbacks=%d",
			preupdates, updates, commits, rollbacks)
	}
}

// Test_NewDriverFromConfig_PreUpdateHookOptions verifies the table filter and
// row-IDs-only options of the preupdate hook.
func Test_NewDriverFromConfig_PreUpdateHookOptions(t *testing.T) {
	var events []*proto.CDCEvent
	d := NewDriverFromConfig(testDriverConfigName(), &DriverConfig{
		ChkOnClose: CnkOnCloseModeDisabled,
		PreUpdateHook: func(ev *proto.CDCEvent) error {
			events = append(events, ev)
			return nil
		},
		PreUpdateTableRe:    regexp.MustCompile("^foo$"),
		PreUpdateRowIDsOnly: true,
	})

	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithDriver(d, path, false, true)
	if err != nil {
		t.Fatalf("OpenWithDriver failed: %s", err)
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	mustExecute(db, "CREATE TABLE bar (id INTEGER PRIMARY KEY, name TEXT)")

	mustExecute(db, "INSERT INTO foo VALUES (1, 'fiona')")
	mustExecute(db, "INSERT INTO bar VALUES (1, 'fiona')")
	if len(events) != 1 {
		t.Fatalf("expected 1 event for table matching filter, got %d", len(events))
	}
	if events[0].Table != "foo" || events[0].NewRowId != 1 {
		t.Fatalf("unexpected event: %v", events[0])
	}
	if events[0].NewRow != nil {
		t.Fatalf("expected no row data with row IDs only, got %v", events[0].NewRow)
	}
}

// hookStats returns the current values of the hook invocation stats.
func hookStats() [3]int64 {
	return [3]int64{
		stats.Get(numPreupdates).(*expvar.Int).Value(),
		stats.Get(numUpdateHooks).(*expvar.Int).Value(),
		stats.Get(numCommitHooks).(*expvar.Int).Value(),
	}
}

// Verifies that extension paths set in
// DriverConfig are reflected on the returned Driver struct.
func Test_DriverConfig_ExtensionsFields(t *testing.T) {
	exts := []string{"/tmp/ext1.so", "/tmp/ext2.so"}
	d := NewDriverFromConfig(testDriverConfigName(), &DriverConfig{
		Extensions: exts,
		ChkOnClose: CnkOnCloseModeDisabled,
	})
	if len(d.Extensions()) != 2 {
		t.Fatalf("expected 2 extensions, got %d", len(d.Extensions()))
	}
	names := d.ExtensionNames()
	if names[0] != "ext1.so" || names[1] != "ext2.so" {
		t.Fatalf("unexpected extension names: %v", names)
	}
}
