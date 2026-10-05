package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/rqlite/rqlite/v10/db/querylog"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

func Test_DefaultDriver(t *testing.T) {
	d := DefaultDriver()
	if d == nil {
		t.Fatalf("DefaultDriver returned nil")
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
	extensions := []string{"test1", "test2"}
	d := NewDriver(extensions, CnkOnCloseModeEnabled)
	if d == nil {
		t.Fatalf("NewDriver returned nil")
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
	for name, d := range map[string]*Driver{
		"default":     DefaultDriver(),
		"checkpoint":  CheckpointDriver(),
		"foreign key": ForeignKeyDriver(),
		"new":         NewDriver(nil, CnkOnCloseModeEnabled),
	} {
		path := mustTempPath()
		defer fsutil.RemoveAll(path)
		for _, readOnly := range []bool{ModeReadWrite, ModeReadOnly} {
			db := sql.OpenDB(d.factory(MakeDSN(path, readOnly, false, true)))
			defer db.Close()

			// Hold each connection open so the pool must create a new one each time.
			for i := 0; i < 3; i++ {
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Fatalf("driver %s: failed to get connection: %s", name, err)
				}
				defer conn.Close()
				var n int
				if err := conn.QueryRowContext(context.Background(), "PRAGMA wal_autocheckpoint").Scan(&n); err != nil {
					t.Fatalf("driver %s: failed to read autocheckpoint setting: %s", name, err)
				}
				if n != 0 {
					t.Errorf("driver %s, read-only %v, connection %d: autocheckpoint is %d, want 0", name, readOnly, i, n)
				}
			}
		}
	}
}

// Verifies that a DriverConfig with a QueryLogger does not interfere with
// normal database operations. Log output capture is tested in db/querylog.
func Test_NewDriverFromConfig_QueryLogOnly(t *testing.T) {
	ql := querylog.New(querylog.DefaultConfig())

	d := NewDriverFromConfig(&DriverConfig{
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
	d := NewDriverFromConfig(&DriverConfig{
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

// Verifies that extension paths set in
// DriverConfig are reflected on the returned Driver struct.
func Test_DriverConfig_ExtensionsFields(t *testing.T) {
	exts := []string{"/tmp/ext1.so", "/tmp/ext2.so"}
	d := NewDriverFromConfig(&DriverConfig{
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
