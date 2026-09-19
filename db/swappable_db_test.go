package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	command "github.com/rqlite/rqlite/v10/command/proto"
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
	for _, wal := range []bool{false, true} {
		for _, corruptFile := range []bool{false, true} {
			t.Run(fmt.Sprintf("wal-%t-corrupt-%t", wal, corruptFile), func(t *testing.T) {
				s, srcPath := newSwapFailureTestDB(t, wal)
				if corruptFile {
					// Pass the header check but fail SQLite's actual open.
					data := append([]byte("SQLite format 3\x00"), make([]byte, 84)...)
					if err := os.WriteFile(srcPath, data, 0600); err != nil {
						t.Fatal(err)
					}
				} else {
					s.drv = &Driver{name: "no-such-driver-for-swap-test"}
				}
				// Use different settings for the replacement to check that recovery
				// restores the original settings, including the connection pool limit.
				err := s.Swap(srcPath, false, !wal)
				if err == nil || !strings.Contains(err.Error(), "open SQLite file failed") {
					t.Fatalf("expected replacement open failure, got %v", err)
				}
				assertSwapOriginalUsable(t, s, wal)
				for _, suffix := range []string{"", "-wal", "-shm"} {
					if _, err := os.Stat(s.Path() + stashedFilesSuffix + suffix); !os.IsNotExist(err) {
						t.Fatalf("stash remains after rollback: %v", err)
					}
				}
			})
		}
	}
}

func Test_SwapStashFailureLeavesOriginalUsable(t *testing.T) {
	s, srcPath := newSwapFailureTestDB(t, true)
	stash := s.Path() + stashedFilesSuffix
	if err := os.WriteFile(stash, []byte("existing stash"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Swap(srcPath, false, false); err == nil {
		t.Fatal("expected stash conflict")
	}
	assertStashFileContents(t, stash, "existing stash")
	assertSwapOriginalUsable(t, s, true)
}

func Test_SwapSelfLeavesOriginalUsable(t *testing.T) {
	s, _ := newSwapFailureTestDB(t, true)
	if err := s.Swap(s.Path(), false, false); err == nil {
		t.Fatal("expected self-swap error")
	}
	assertSwapOriginalUsable(t, s, true)
}

func Test_SwapRollbackFailureReported(t *testing.T) {
	s, srcPath := newSwapFailureTestDB(t, false)
	path := s.Path()
	name := fmt.Sprintf("swap-failed-cleanup-%d", time.Now().UnixNano())
	sql.Register(name, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			// Leave a nonempty directory where a replacement sidecar would be,
			// then fail opening. Removing the replacement must report the error
			// and retain the original stash for recovery.
			if err := os.Mkdir(path+"-shm", 0700); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(path+"-shm", "blocked"), nil, 0600); err != nil {
				return err
			}
			return fmt.Errorf("injected open failure")
		},
	})
	s.drv = &Driver{name: name}
	err := s.Swap(srcPath, false, false)
	if err == nil || !strings.Contains(err.Error(), "injected open failure") ||
		!strings.Contains(err.Error(), "failed to remove replacement files") {
		t.Fatalf("expected both open and rollback errors, got %v", err)
	}
	original, err := Open(path+stashedFilesSuffix, true, false)
	if err != nil {
		t.Fatalf("original stash not readable: %v", err)
	}
	defer original.Close()
	rows := mustQuery(original, "SELECT name FROM foo")
	if got, want := asJSON(rows), `[{"columns":["name"],"types":["text"],"values":[["original"]]}]`; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func newSwapFailureTestDB(t *testing.T, wal bool) (*SwappableDB, string) {
	t.Helper()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "incoming.sqlite")
	src, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatal(err)
	}
	mustExecute(src, "CREATE TABLE foo (name TEXT)")
	mustExecute(src, "INSERT INTO foo VALUES ('incoming')")
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenSwappable(filepath.Join(dir, "current.sqlite"), nil, true, wal, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	mustExecute(s.db, "CREATE TABLE foo (name TEXT)")
	mustExecute(s.db, "INSERT INTO foo VALUES ('original')")
	if err := s.SetSynchronousMode(SynchronousFull); err != nil {
		t.Fatal(err)
	}
	return s, srcPath
}

func assertSwapOriginalUsable(t *testing.T, s *SwappableDB, wal bool) {
	t.Helper()
	rows, err := s.QueryStringStmt("SELECT name FROM foo")
	if err != nil {
		t.Fatalf("same SwappableDB is not queryable after failure: %v", err)
	}
	if got, want := asJSON(rows), `[{"columns":["name"],"types":["text"],"values":[["original"]]}]`; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	if !s.FKEnabled() || s.WALEnabled() != wal || s.db.roDB.Stats().MaxOpenConnections != 3 {
		t.Fatal("original database configuration not restored")
	}
	if mode, err := s.db.GetSynchronousMode(); err != nil || mode != SynchronousFull {
		t.Fatalf("original synchronous mode not restored: %v, %v", mode, err)
	}
	results, err := s.Execute(&command.Request{Statements: []*command.Statement{{Sql: "INSERT INTO foo VALUES ('after failure')"}}}, false)
	if err != nil || len(results) != 1 || results[0].GetError() != "" || results[0].GetE() == nil || results[0].GetE().GetRowsAffected() != 1 {
		t.Fatalf("same SwappableDB is not writable: %v, %v", results, err)
	}
	if s.checkpointMgr.db != s.db {
		t.Fatal("checkpoint manager still points at the old connection")
	}
	if wal {
		if _, _, err := s.Checkpoint(nil, time.Second); err != nil {
			t.Fatalf("restored checkpoint manager is not usable: %v", err)
		}
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
	} else if !strings.Contains(err.Error(), "failed to rename database") {
		t.Fatalf("unexpected swap failure: %v", err)
	}

	// The existing wrapper must be usable with its original data.
	rows, err := swappableDB.QueryStringStmt("SELECT name FROM foo")
	if err != nil {
		t.Fatalf("same SwappableDB is not queryable after rename failure: %v", err)
	}
	if exp, got := `[{"columns":["name"],"types":["text"],"values":[["original"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results after failed swap, expected %s, got %s", exp, got)
	}

	// No set-aside files may be left behind.
	if fsutil.FileExists(swappablePath + stashedFilesSuffix) {
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
	swappableDB, err := OpenSwappable(swappablePath, nil, false, true, 3)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()
	mustExecute(swappableDB.db, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(swappableDB.db, `INSERT INTO foo(name) VALUES("original")`)

	if err := swappableDB.Swap(srcPath, false, true); err != nil {
		t.Fatalf("failed to swap database: %s", err)
	}

	if got := swappableDB.db.roDB.Stats().MaxOpenConnections; got != 3 {
		t.Fatalf("read-only pool limit changed after swap: %d", got)
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
	if fsutil.FileExists(swappablePath + stashedFilesSuffix) {
		t.Fatalf("set-aside files not cleaned up after successful swap")
	}
}

func Test_SwapRetryAfterRollback(t *testing.T) {
	s, srcPath := newSwapFailureTestDB(t, true)
	drv := s.drv
	s.drv = &Driver{name: "no-such-driver-for-swap-retry-test"}
	if err := s.Swap(srcPath, false, true); err == nil {
		t.Fatal("expected first swap to fail")
	}
	assertSwapOriginalUsable(t, s, true)
	s.drv = drv
	writeSwapReplacement(t, srcPath, "retry")
	if err := s.Swap(srcPath, true, true); err != nil {
		t.Fatalf("retry failed after rollback: %v", err)
	}
	rows, err := s.QueryStringStmt("SELECT name FROM foo")
	if err != nil || asJSON(rows) != `[{"columns":["name"],"types":["text"],"values":[["retry"]]}]` {
		t.Fatalf("unexpected retry results: %s, %v", asJSON(rows), err)
	}
}

func Test_SwapRetriesCommittedStashCleanup(t *testing.T) {
	s, srcPath := newSwapFailureTestDB(t, false)
	path := s.Path()
	blockedPath := path + stashedFilesSuffix + "-shm"
	name := fmt.Sprintf("swap-cleanup-retry-%d", time.Now().UnixNano())
	blockCleanup := true
	sql.Register(name, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if !blockCleanup {
				return nil
			}
			blockCleanup = false
			if err := os.Mkdir(blockedPath, 0700); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(blockedPath, "blocked"), nil, 0600)
		},
	})
	s.drv = &Driver{name: name}
	if err := s.Swap(srcPath, false, false); err != nil {
		t.Fatalf("successful installation failed on cleanup: %v", err)
	}
	if !s.stashCleanupPending {
		t.Fatal("completed swap did not record pending cleanup")
	}
	writeSwapReplacement(t, srcPath, "retry")
	if err := s.Swap(srcPath, false, false); err == nil || !strings.Contains(err.Error(), "stash from completed swap") {
		t.Fatalf("expected cleanup error while obstruction remains: %v", err)
	}
	rows, err := s.QueryStringStmt("SELECT name FROM foo")
	if err != nil || asJSON(rows) != `[{"columns":["name"],"types":["text"],"values":[["incoming"]]}]` {
		t.Fatalf("cleanup failure affected the active database: %s, %v", asJSON(rows), err)
	}
	if err := os.RemoveAll(blockedPath); err != nil {
		t.Fatal(err)
	}
	if err := s.Swap(srcPath, false, false); err != nil {
		t.Fatalf("retry failed after cleanup obstruction was removed: %v", err)
	}
	if s.stashCleanupPending {
		t.Fatal("cleanup still marked pending")
	}
	rows, err = s.QueryStringStmt("SELECT name FROM foo")
	if err != nil || asJSON(rows) != `[{"columns":["name"],"types":["text"],"values":[["retry"]]}]` {
		t.Fatalf("unexpected retry results: %s, %v", asJSON(rows), err)
	}
}

func writeSwapReplacement(t *testing.T, path, name string) {
	t.Helper()
	d, err := Open(path, false, false)
	if err != nil {
		t.Fatal(err)
	}
	mustExecute(d, "CREATE TABLE foo (name TEXT)")
	mustExecute(d, "INSERT INTO foo VALUES ('"+name+"')")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
