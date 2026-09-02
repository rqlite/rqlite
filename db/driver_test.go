package db

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"testing"

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
	defer os.RemoveAll(path)
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
	if err := os.Remove(db.WALPath()); err != nil {
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
	defer os.RemoveAll(path)
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

// A local counter for generating unique driver names.
var driverTestSeq atomic.Int64

func testDriverConfigName() string {
	return fmt.Sprintf("test-driver-config-%d", driverTestSeq.Add(1))
}

// Verifies that a DriverConfig with a QueryLogger
// produces log output for every executed statement.
func Test_NewDriverFromConfig_QueryLogOnly(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})

	d := NewDriverFromConfig(testDriverConfigName(), DriverConfig{
		ChkOnClose:  CnkOnCloseModeDisabled,
		QueryLogger: ql,
	})

	path := mustTempPath()
	defer os.RemoveAll(path)
	db, err := OpenWithDriver(d, path, false, true)
	if err != nil {
		t.Fatalf("OpenWithDriver failed: %s", err)
	}
	defer db.Close()

	mustExecute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	mustExecute(db, "INSERT INTO t VALUES (1, 'hello')")

	output := buf.String()
	if !strings.Contains(output, "CREATE TABLE t") {
		t.Fatalf("expected CREATE TABLE in query log, got:\n%s", output)
	}
	if !strings.Contains(output, "INSERT INTO t") {
		t.Fatalf("expected INSERT in query log, got:\n%s", output)
	}
}

// Verifies that a DriverConfig with nil
// QueryLogger opens and operates normally without tracing.
func Test_NewDriverFromConfig_NoQueryLog(t *testing.T) {
	d := NewDriverFromConfig(testDriverConfigName(), DriverConfig{
		ChkOnClose:  CnkOnCloseModeDisabled,
		QueryLogger: nil,
	})
	if d.CheckpointOnCloseMode() != CnkOnCloseModeDisabled {
		t.Fatalf("expected CnkOnCloseModeDisabled, got %v", d.CheckpointOnCloseMode())
	}

	path := mustTempPath()
	defer os.RemoveAll(path)
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
	d := NewDriverFromConfig(testDriverConfigName(), DriverConfig{
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
