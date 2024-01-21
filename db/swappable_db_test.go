package db

import (
	"os"
	"testing"
)

// Test_OpenSwappable_Success tests that OpenSwappable correctly opens a database and returns
// a valid SwappableDB instance.
func Test_OpenSwappable_Success(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)

	// Attempt to open a swappable database
	swappableDB, err := OpenSwappable(path, false, false)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Verify that the returned SwappableDB is not nil
	if swappableDB == nil {
		t.Fatalf("expected non-nil SwappableDB")
	}

	// Confirm a file was created at the specified path
	if !fileExists(path) {
		t.Fatalf("database file not created at %s", path)
	}

	// Check the paths of the underlying database
	if swappableDB.Path() != path {
		t.Fatalf("expected swappable database path to be %s, got %s", path, swappableDB.Path())
	}
}

// Test_OpenSwappable_InvalidPath tests that OpenSwappable returns an error when provided
// with an invalid file path.
func Test_OpenSwappable_InvalidPath(t *testing.T) {
	invalidPath := "/invalid/path/to/database"

	// Attempt to open a swappable database with an invalid path
	swappableDB, err := OpenSwappable(invalidPath, false, false)
	if err == nil {
		swappableDB.Close()
		t.Fatalf("expected an error when opening swappable database with invalid path, got nil")
	}

	// Check that no SwappableDB instance is returned
	if swappableDB != nil {
		t.Fatalf("expected nil SwappableDB instance, got non-nil")
	}
}

// Test_SwapSuccess tests that the Swap function successfully swaps the underlying database.
func Test_SwapSuccess(t *testing.T) {
	// Create a new database with content
	srcPath := mustTempPath()
	defer os.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	defer srcDB.Close()
	mustExecute(srcDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(srcDB, `INSERT INTO foo(name) VALUES("test")`)

	// Create a SwappableDB with an empty database
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, false, false)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Perform the swap
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database pre-swap: %s", err)
	}
	if err := swappableDB.Swap(srcPath, false, false); err != nil {
		t.Fatalf("failed to swap database: %s", err)
	}

	// Confirm the SwappableDB contains the data from the source database
	rows, err := swappableDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query swapped database: %s", err)
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"test"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results after swap, expected %s, got %s", exp, got)
	}
}

// Test_SwapInvalidSQLiteFile tests that the Swap function returns an error when provided
// with an invalid SQLite file.
func Test_SwapInvalidSQLiteFile(t *testing.T) {
	// Create a SwappableDB with an empty database
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, false, false)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Create an invalid SQLite file
	invalidSQLiteFilePath := mustTempPath()
	defer os.Remove(invalidSQLiteFilePath)
	file, err := os.Create(invalidSQLiteFilePath)
	if err != nil {
		t.Fatalf("failed to create invalid SQLite file: %s", err)
	}
	if _, err := file.WriteString("not a valid SQLite file"); err != nil {
		t.Fatalf("failed to write to invalid SQLite file: %s", err)
	}
	file.Close()

	// Attempt to swap with the invalid SQLite file
	err = swappableDB.Swap(invalidSQLiteFilePath, false, false)
	if err == nil {
		t.Fatalf("expected an error when swapping with an invalid SQLite file, got nil")
	}
}
