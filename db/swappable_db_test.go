package db

import (
	"os"
	"runtime"
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

// Test_SwapSuccess_RemovesBackup tests that a successful swap removes the backup
// copy of the original database.
func Test_SwapSuccess_RemovesBackup(t *testing.T) {
	srcPath := mustTempPath()
	defer os.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	mustExecute(srcDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database: %s", err)
	}

	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, nil, false, false, 0)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	if err := swappableDB.Swap(srcPath, false, false); err != nil {
		t.Fatalf("failed to swap database: %s", err)
	}

	for _, ext := range []string{"", "-wal", "-shm"} {
		if fsutil.FileExists(swappablePath + ".swap-backup" + ext) {
			t.Fatalf("backup file %s left behind after successful swap", swappablePath+".swap-backup"+ext)
		}
	}
}

// Test_SwapRenameFailure_RestoresOriginal tests that if a swap fails partway through,
// the original database is restored and remains fully usable.
func Test_SwapRenameFailure_RestoresOriginal(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("test needs a second filesystem, only guaranteed on Linux")
	}
	if _, err := os.Stat("/dev/shm"); err != nil {
		t.Skip("/dev/shm not available")
	}

	// Confirm /dev/shm and the temp dir are on different filesystems, otherwise
	// this test can't force the rename to fail.
	probe, err := os.CreateTemp("/dev/shm", "rqlite-swap-probe")
	if err != nil {
		t.Fatalf("failed to create probe file: %s", err)
	}
	probePath := probe.Name()
	probe.Close()
	probeDst := mustTempPath()
	if err := os.Rename(probePath, probeDst); err == nil {
		os.Remove(probeDst)
		t.Skip("/dev/shm and os.TempDir are on the same filesystem")
	}
	os.Remove(probePath)

	// Create the original database with content.
	origPath := mustTempPath()
	defer os.Remove(origPath)
	origDB, err := Open(origPath, false, false)
	if err != nil {
		t.Fatalf("failed to open original database: %s", err)
	}
	mustExecute(origDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(origDB, `INSERT INTO foo(name) VALUES("original")`)
	if err := origDB.Close(); err != nil {
		t.Fatalf("failed to close original database: %s", err)
	}

	swappableDB, err := OpenSwappable(origPath, nil, false, false, 0)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Create a valid SQLite database on /dev/shm. Renaming it into place will
	// fail with EXDEV, failing the swap after the original has been set aside.
	srcFile, err := os.CreateTemp("/dev/shm", "rqlite-swap-test")
	if err != nil {
		t.Fatalf("failed to create source file: %s", err)
	}
	srcPath := srcFile.Name()
	srcFile.Close()
	os.Remove(srcPath)
	defer os.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	mustExecute(srcDB, "CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database: %s", err)
	}

	if err := swappableDB.Swap(srcPath, false, false); err == nil {
		t.Fatalf("expected swap to fail, but it didn't")
	}

	// The original database must be back in place and usable.
	rows, err := swappableDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query original database after failed swap: %s", err)
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"original"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results after failed swap, expected %s, got %s", exp, got)
	}

	// No backup files may be left behind.
	for _, ext := range []string{"", "-wal", "-shm"} {
		if fsutil.FileExists(origPath + ".swap-backup" + ext) {
			t.Fatalf("backup file %s left behind after failed swap", origPath+".swap-backup"+ext)
		}
	}
}
