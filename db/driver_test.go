package db

import (
	"os"
	"testing"
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
	if !fileExists(db.WALPath()) {
		t.Fatalf("WAL file not created")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %s", err.Error())
	}
	if !fileExists(db.WALPath()) {
		t.Fatalf("WAL file removed on close")
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
	if !fileExists(db.WALPath()) {
		t.Fatalf("WAL file not created")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %s", err.Error())
	}
	if fileExists(db.WALPath()) {
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
