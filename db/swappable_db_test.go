package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// Test_OpenSwappable_Success tests that OpenSwappable correctly opens a database and returns
// a valid SwappableDB instance.
func Test_OpenSwappable_Success(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)

	// Attempt to open a swappable database
	swappableDB, err := OpenSwappable(path, nil, false, false, 0)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Verify that the returned SwappableDB is not nil
	if swappableDB == nil {
		t.Fatalf("expected non-nil SwappableDB")
	}

	// Confirm a file was created at the specified path
	if !fsutil.FileExists(path) {
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
	swappableDB, err := OpenSwappable(invalidPath, nil, false, false, 0)
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
	swappableDB, err := OpenSwappable(swappablePath, nil, false, false, 0)
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

func Test_SwapSuccess_Driver(t *testing.T) {
	// Create a new database and confirm foreign key support is enabled
	srcPath := mustTempPath()
	defer os.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	defer srcDB.Close()
	mustExecute(srcDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	rows := mustQuery(srcDB, "PRAGMA foreign_keys")
	if exp, got := `[{"columns":["foreign_keys"],"types":["integer"],"values":[[0]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected foreign key support to be disabled, got %s", got)
	}

	// Create a SwappableDB with an empty database
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, ForeignKeyDriver(), false, false, 0)
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

	// Confirm the SwappableDB still has the right FK setting, checking that it's using the right driver.
	rows, err = swappableDB.QueryStringStmt("PRAGMA foreign_keys")
	if err != nil {
		t.Fatalf("failed to query swapped database: %s", err)
	}
	if exp, got := `[{"columns":["foreign_keys"],"types":["integer"],"values":[[1]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected foreign key support to be enabled, got %s", got)
	}
}

// Test_SwapInvalidSQLiteFile tests that the Swap function returns an error when provided
// with an invalid SQLite file.
func Test_SwapInvalidSQLiteFile(t *testing.T) {
	// Create a SwappableDB with an empty database
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, nil, false, false, 0)
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

// Test_SwapFailure_PreservesOriginalDatabase verifies that a failed swap does
// not remove or modify the existing database.
func Test_SwapFailure_PreservesOriginalDatabase(t *testing.T) {
	originalPath := mustTempPath()
	defer os.Remove(originalPath)

	originalDB, err := Open(originalPath, false, false)
	if err != nil {
		t.Fatalf("failed to open original database: %s", err)
	}

	mustExecute(originalDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(originalDB, `INSERT INTO foo(name) VALUES("original")`)

	if err := originalDB.Close(); err != nil {
		t.Fatalf("failed to close original database: %s", err)
	}
	replacementPath := mustTempPath()
	defer os.Remove(replacementPath)

	file, err := os.Create(replacementPath)
	if err != nil {
		t.Fatalf("failed to create replacement database: %s", err)
	}

	if _, err := file.WriteString("SQLite format 3\x00invalid"); err != nil {
		_ = file.Close()
		t.Fatalf("failed to write replacement database: %s", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("failed to close replacement database: %s", err)
	}

	swappableDB, err := OpenSwappable(originalPath, nil, false, false, 0)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	if err := swappableDB.Swap(replacementPath, false, false); err == nil {
		t.Fatal("expected swap to fail, got nil")
	}

	if !fsutil.FileExists(originalPath) {
		t.Fatal("original database file was removed after failed swap")
	}

	rows, err := swappableDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("swappable database unusable after failed swap: %s", err)
	}

	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"original"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected data after failed swap, expected %s, got %s", exp, got)
	}
}

// Verifies that a pre-existing stale swap temporary file is not overwritten or deleted by a subsequent swap, and that
// the swap still uses its own isolated temporary path.
func Test_SwapFailure_StaleTempPath(t *testing.T) {
	// Create the original database with known data.
	originalPath := mustTempPath()
	defer os.Remove(originalPath)

	originalDB, err := Open(originalPath, false, false)
	if err != nil {
		t.Fatalf("failed to open original database: %s", err)
	}
	mustExecute(originalDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(originalDB, `INSERT INTO foo(name) VALUES("stale-test")`)
	if err := originalDB.Close(); err != nil {
		t.Fatalf("failed to close original database: %s", err)
	}

	dir := filepath.Dir(originalPath)
	base := filepath.Base(originalPath)
	staleFile, err := os.CreateTemp(dir, base+"-swap-")
	if err != nil {
		t.Fatalf("failed to create stale swap file: %s", err)
	}
	stalePath := staleFile.Name()
	if _, err := staleFile.WriteString("stale sentinel content"); err != nil {
		_ = staleFile.Close()
		t.Fatalf("failed to write stale swap file: %s", err)
	}
	if err := staleFile.Close(); err != nil {
		t.Fatalf("failed to close stale swap file: %s", err)
	}
	defer os.Remove(stalePath)

	replacementPath := mustTempPath()
	defer os.Remove(replacementPath)
	rf, err := os.Create(replacementPath)
	if err != nil {
		t.Fatalf("failed to create replacement file: %s", err)
	}
	if _, err := rf.WriteString("SQLite format 3\x00invalid"); err != nil {
		_ = rf.Close()
		t.Fatalf("failed to write replacement file: %s", err)
	}
	if err := rf.Close(); err != nil {
		t.Fatalf("failed to close replacement file: %s", err)
	}

	swappableDB, err := OpenSwappable(originalPath, nil, false, false, 0)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	if err := swappableDB.Swap(replacementPath, false, false); err == nil {
		t.Fatal("expected swap to fail, got nil")
	}

	// The stale file must still exist with its original content untouched.
	if !fsutil.FileExists(stalePath) {
		t.Fatal("stale swap file was removed by the swap operation")
	}
	staleContent, err := os.ReadFile(stalePath)
	if err != nil {
		t.Fatalf("failed to read stale swap file: %s", err)
	}
	if got := string(staleContent); got != "stale sentinel content" {
		t.Fatalf("stale swap file content changed, got %q", got)
	}

	if !fsutil.FileExists(originalPath) {
		t.Fatal("original database file does not exist after failed swap")
	}

	// The SwappableDB must remain fully usable after rollback.
	rows, err := swappableDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("swappable database unusable after failed swap with stale path: %s", err)
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"stale-test"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected data after failed swap, expected %s, got %s", exp, got)
	}
}

// Verifies that WAL and SHM sidecar files belonging to the original database are preserved and restored correctly when a swap fails.
func Test_SwapFailure_WALSidecarFilesPreserved(t *testing.T) {
	originalPath := mustTempPath()
	defer os.Remove(originalPath)
	defer os.Remove(originalPath + "-wal")
	defer os.Remove(originalPath + "-shm")

	originalDB, err := Open(originalPath, false, true) // WAL mode
	if err != nil {
		t.Fatalf("failed to open original WAL database: %s", err)
	}
	mustExecute(originalDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(originalDB, `INSERT INTO foo(name) VALUES("wal-original")`)

	if err := originalDB.Close(); err != nil {
		t.Fatalf("failed to close original database: %s", err)
	}

	// Open the same path through SwappableDB in WAL mode.
	swappableDB, err := OpenSwappable(originalPath, nil, false, true, 0)
	if err != nil {
		t.Fatalf("failed to open swappable WAL database: %s", err)
	}
	defer swappableDB.Close()

	// Write one more row via SwappableDB so the WAL file has data.
	mustExecute(swappableDB.db, `INSERT INTO foo(name) VALUES("via-swappable")`)

	// Confirm the WAL file exists before the swap attempt.
	if !fsutil.FileExists(originalPath + "-wal") {
		t.Skip("WAL file not present; skipping WAL sidecar preservation test")
	}

	// Create a replacement that passes the SQLite header check but cannot be opened.
	replacementPath := mustTempPath()
	defer os.Remove(replacementPath)

	rf, err := os.Create(replacementPath)
	if err != nil {
		t.Fatalf("failed to create replacement file: %s", err)
	}

	if _, err := rf.WriteString("SQLite format 3\x00invalid"); err != nil {
		_ = rf.Close()
		t.Fatalf("failed to write replacement file: %s", err)
	}

	if err := rf.Close(); err != nil {
		t.Fatalf("failed to close replacement file: %s", err)
	}

	// Perform the swap — it must fail.
	if err := swappableDB.Swap(replacementPath, false, true); err == nil {
		t.Fatal("expected swap to fail, got nil")
	}

	// The original database file must still be present.
	if !fsutil.FileExists(originalPath) {
		t.Fatal("original database file missing after failed WAL swap")
	}

	// The SwappableDB must remain usable and contain the original data.
	rows, err := swappableDB.QueryStringStmt("SELECT * FROM foo ORDER BY id")
	if err != nil {
		t.Fatalf("swappable WAL database unusable after failed swap: %s", err)
	}

	expected := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"wal-original"],[2,"via-swappable"]]}]`
	if got := asJSON(rows); got != expected {
		t.Fatalf(
			"unexpected data after failed WAL swap, expected %s, got %s",
			expected,
			got,
		)
	}

	// No temporary swap files should remain in the database directory.
	dir := filepath.Dir(originalPath)
	base := filepath.Base(originalPath)
	pattern := filepath.Join(dir, base+"-swap-*")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("failed to glob for temp swap files: %s", err)
	}

	if len(matches) != 0 {
		t.Fatalf("leftover temp swap files found after rollback: %v", matches)
	}
}
