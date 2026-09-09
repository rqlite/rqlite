package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// Test_SwapOpenFailureRestoresOriginal tests that the original database is restored
// when the incoming database cannot be opened after being renamed into place.
func Test_SwapOpenFailureRestoresOriginal(t *testing.T) {
	// Create a source database with content
	srcPath := mustTempPath()
	defer os.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	mustExecute(srcDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(srcDB, `INSERT INTO foo(name) VALUES("incoming")`)
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database: %s", err)
	}

	// Create a SwappableDB with content
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, nil, false, false, 0)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()
	mustExecute(swappableDB.db, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(swappableDB.db, `INSERT INTO foo(name) VALUES("original")`)

	// Point the SwappableDB at a driver that cannot open anything, so opening the
	// incoming database fails after its file has been renamed into place.
	swappableDB.drv = &Driver{name: "no-such-driver-for-swap-test"}
	err = swappableDB.Swap(srcPath, false, false)
	if err == nil {
		t.Fatalf("expected an error when swapping in an unopenable database, got nil")
	}

	// The original database must be back in place, with its data intact.
	db, err := Open(swappablePath, false, false)
	if err != nil {
		t.Fatalf("failed to reopen database after failed swap: %s", err)
	}
	defer db.Close()
	rows := mustQuery(db, "SELECT name FROM foo")
	if exp, got := `[{"columns":["name"],"types":["text"],"values":[["original"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results after failed swap, expected %s, got %s", exp, got)
	}

	// No set-aside files may be left behind.
	if fsutil.FileExists(swapAsidePath(swappablePath)) {
		t.Fatalf("set-aside files not cleaned up after failed swap")
	}
}

// Test_SwapRenameFailureRestoresOriginal tests that the original database is restored
// when the incoming database cannot be renamed into place. The rename is forced to
// fail by placing the incoming file on a different filesystem (/dev/shm is a tmpfs).
func Test_SwapRenameFailureRestoresOriginal(t *testing.T) {
	shmDir := "/dev/shm"
	if fi, err := os.Stat(shmDir); err != nil || !fi.IsDir() {
		t.Skipf("no %s filesystem available", shmDir)
	}

	// Create a source database with content, on a different filesystem
	srcPath := filepath.Join(shmDir, fmt.Sprintf("rqlite-swap-src-%d", time.Now().UnixNano()))
	defer os.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	mustExecute(srcDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(srcDB, `INSERT INTO foo(name) VALUES("incoming")`)
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database: %s", err)
	}

	// Create a SwappableDB with content
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, nil, false, false, 0)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()
	mustExecute(swappableDB.db, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(swappableDB.db, `INSERT INTO foo(name) VALUES("original")`)

	if err := swappableDB.Swap(srcPath, false, false); err == nil {
		t.Skip("rename across filesystems unexpectedly succeeded, cannot exercise failure path")
	}

	// The original database must be back in place, with its data intact.
	db, err := Open(swappablePath, false, false)
	if err != nil {
		t.Fatalf("failed to reopen database after failed swap: %s", err)
	}
	defer db.Close()
	rows := mustQuery(db, "SELECT name FROM foo")
	if exp, got := `[{"columns":["name"],"types":["text"],"values":[["original"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results after failed swap, expected %s, got %s", exp, got)
	}

	// No set-aside files may be left behind.
	if fsutil.FileExists(swapAsidePath(swappablePath)) {
		t.Fatalf("set-aside files not cleaned up after failed swap")
	}
}

// Test_SwapSuccessWAL tests a successful swap where the SwappableDB is in WAL
// mode, and no set-aside files are left behind.
func Test_SwapSuccessWAL(t *testing.T) {
	// Create a source database with content. The incoming file is a single,
	// self-contained database, as produced by the callers of Swap (backups,
	// serialized databases, and uploads).
	srcPath := mustTempPath()
	defer os.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	mustExecute(srcDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(srcDB, `INSERT INTO foo(name) VALUES("incoming")`)
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database: %s", err)
	}

	// Create a SwappableDB with content
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, nil, false, true, 0)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()
	mustExecute(swappableDB.db, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(swappableDB.db, `INSERT INTO foo(name) VALUES("original")`)

	if err := swappableDB.Swap(srcPath, false, true); err != nil {
		t.Fatalf("failed to swap database: %s", err)
	}

	// Confirm the SwappableDB contains the data from the source database
	rows, err := swappableDB.QueryStringStmt("SELECT name FROM foo")
	if err != nil {
		t.Fatalf("failed to query swapped database: %s", err)
	}
	if exp, got := `[{"columns":["name"],"types":["text"],"values":[["incoming"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results after swap, expected %s, got %s", exp, got)
	}

	// No set-aside files may be left behind.
	if fsutil.FileExists(swapAsidePath(swappablePath)) {
		t.Fatalf("set-aside files not cleaned up after successful swap")
	}
}

// Test_RecoverPendingSwap tests recovery of a swap interrupted by a crash.
func Test_RecoverPendingSwap(t *testing.T) {
	t.Run("NoAsideFiles", func(t *testing.T) {
		dbPath := mustTempPath()
		defer os.Remove(dbPath)
		if err := RecoverPendingSwap(dbPath, false); err != nil {
			t.Fatalf("unexpected error when no swap is pending: %s", err)
		}
	})

	t.Run("RestoresWhenDatabaseMissing", func(t *testing.T) {
		dbPath := mustTempPath()
		defer os.Remove(dbPath)
		if err := os.WriteFile(dbPath, []byte("original"), 0644); err != nil {
			t.Fatalf("failed to create database file: %s", err)
		}
		if err := moveFilesAside(dbPath, swapAsidePath(dbPath)); err != nil {
			t.Fatalf("failed to set aside database files: %s", err)
		}
		if err := RecoverPendingSwap(dbPath, false); err != nil {
			t.Fatalf("failed to recover pending swap: %s", err)
		}
		if b, err := os.ReadFile(dbPath); err != nil || string(b) != "original" {
			t.Fatalf("database file not restored")
		}
		if fsutil.FileExists(swapAsidePath(dbPath)) {
			t.Fatalf("set-aside files not removed after recovery")
		}
	})

	t.Run("RestoresWALAndSHMFiles", func(t *testing.T) {
		dbPath := mustTempPath()
		defer os.Remove(dbPath)
		for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
			if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
				t.Fatalf("failed to create file %s: %s", path, err)
			}
		}
		if err := moveFilesAside(dbPath, swapAsidePath(dbPath)); err != nil {
			t.Fatalf("failed to set aside database files: %s", err)
		}
		if err := RecoverPendingSwap(dbPath, false); err != nil {
			t.Fatalf("failed to recover pending swap: %s", err)
		}
		for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
			if !fsutil.FileExists(path) {
				t.Fatalf("file %s not restored", path)
			}
		}
		if fsutil.FileExists(swapAsidePath(dbPath)) {
			t.Fatalf("set-aside files not removed after recovery")
		}
	})

	t.Run("RemovesAsideWhenDatabasePresent", func(t *testing.T) {
		dbPath := mustTempPath()
		defer os.Remove(dbPath)
		if err := os.WriteFile(dbPath, []byte("new"), 0644); err != nil {
			t.Fatalf("failed to create database file: %s", err)
		}
		if err := os.WriteFile(swapAsidePath(dbPath), []byte("old"), 0644); err != nil {
			t.Fatalf("failed to create set-aside file: %s", err)
		}
		if err := RecoverPendingSwap(dbPath, false); err != nil {
			t.Fatalf("failed to recover pending swap: %s", err)
		}
		if b, err := os.ReadFile(dbPath); err != nil || string(b) != "new" {
			t.Fatalf("database file was modified during recovery")
		}
		if fsutil.FileExists(swapAsidePath(dbPath)) {
			t.Fatalf("stale set-aside files not removed")
		}
	})

	t.Run("DiscardRemovesAside", func(t *testing.T) {
		dbPath := mustTempPath()
		defer os.Remove(dbPath)
		if err := os.WriteFile(dbPath, []byte("new"), 0644); err != nil {
			t.Fatalf("failed to create database file: %s", err)
		}
		if err := os.WriteFile(swapAsidePath(dbPath), []byte("old"), 0644); err != nil {
			t.Fatalf("failed to create set-aside file: %s", err)
		}
		if err := RecoverPendingSwap(dbPath, true); err != nil {
			t.Fatalf("failed to discard pending swap: %s", err)
		}
		if fsutil.FileExists(swapAsidePath(dbPath)) {
			t.Fatalf("set-aside files not removed when discarding")
		}
	})
}
