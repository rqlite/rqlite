package db

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"

	"github.com/mattn/go-sqlite3"
)

// Test_OpenSwappable_Success tests that OpenSwappable correctly opens a database and returns
// a valid SwappableDB instance.
func Test_OpenSwappable_Success(t *testing.T) {
	path := mustTempPath()
	defer fsutil.Remove(path)

	// Attempt to open a swappable database
	swappableDB, err := OpenSwappable(path, NewConfig(ConnConfig{}, false))
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
	swappableDB, err := OpenSwappable(invalidPath, NewConfig(ConnConfig{}, false))
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
	defer fsutil.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	defer srcDB.Close()
	mustExecute(srcDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(srcDB, `INSERT INTO foo(name) VALUES("test")`)

	// Create a SwappableDB with an empty database
	swappablePath := mustTempPath()
	defer fsutil.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, NewConfig(ConnConfig{}, false))
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Perform the swap
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database pre-swap: %s", err)
	}
	if err := swappableDB.Swap(srcPath); err != nil {
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

func Test_SwapSuccess_ForeignKeys(t *testing.T) {
	// Create a new database and confirm foreign key support is enabled
	srcPath := mustTempPath()
	defer fsutil.Remove(srcPath)
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
	defer fsutil.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, NewConfig(ConnConfig{ForeignKeys: true}, false))
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Perform the swap
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database pre-swap: %s", err)
	}
	if err := swappableDB.Swap(srcPath); err != nil {
		t.Fatalf("failed to swap database: %s", err)
	}

	// Confirm the SwappableDB still has the right FK setting, checking that it's using the right configuration.
	rows, err = swappableDB.QueryStringStmt("PRAGMA foreign_keys")
	if err != nil {
		t.Fatalf("failed to query swapped database: %s", err)
	}
	if exp, got := `[{"columns":["foreign_keys"],"types":["integer"],"values":[[1]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected foreign key support to be enabled, got %s", got)
	}
}

// Test_SwapSuccess_Hooks tests that hooks configured on the writer are in place
// after a swap, since the swapped-in database is opened with the same configuration.
func Test_SwapSuccess_Hooks(t *testing.T) {
	// Create the database to be swapped in.
	srcPath := mustTempPath()
	defer fsutil.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	mustExecute(srcDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database pre-swap: %s", err)
	}

	// Create a SwappableDB with an empty database, with hooks on the writer.
	var preupdates, commits int
	cfg := NewConfig(ConnConfig{}, false)
	cfg.Writer.PreUpdateHook = func(ev *proto.CDCEvent) error {
		preupdates++
		return nil
	}
	cfg.Writer.CommitHook = func(*sqlite3.SQLiteConn) bool {
		commits++
		return true
	}
	swappablePath := mustTempPath()
	defer fsutil.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, cfg)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	if err := swappableDB.Swap(srcPath); err != nil {
		t.Fatalf("failed to swap database: %s", err)
	}

	// Write to the swapped-in database, and confirm the hooks fired.
	preupdates, commits = 0, 0
	if _, err := swappableDB.Execute(&proto.Request{Statements: []*proto.Statement{
		{Sql: "INSERT INTO foo(name) VALUES('fiona')"},
	}}, false); err != nil {
		t.Fatalf("failed to write to swapped database: %s", err)
	}
	if preupdates != 1 || commits != 1 {
		t.Fatalf("hooks after swap: preupdates=%d, commits=%d, want 1 each", preupdates, commits)
	}
}

// Test_OpenSwappable_HooksPerRole tests that hooks configured on the writer
// fire for a write, and that hooks configured only on the readers never fire,
// because a query runs on a reader and a reader never changes the database.
func Test_OpenSwappable_HooksPerRole(t *testing.T) {
	var writerCommits, readerCommits, readerPreupdates int
	cfg := NewConfig(ConnConfig{}, true)
	cfg.Writer.CommitHook = func(*sqlite3.SQLiteConn) bool {
		writerCommits++
		return true
	}
	cfg.Readers.CommitHook = func(*sqlite3.SQLiteConn) bool {
		readerCommits++
		return true
	}
	cfg.Readers.PreUpdateHook = func(ev *proto.CDCEvent) error {
		readerPreupdates++
		return nil
	}

	path := mustTempPath()
	defer fsutil.Remove(path)
	db, err := OpenSwappable(path, cfg)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer db.Close()

	// A write goes through the writer.
	writerCommits = 0
	if _, err := db.Request(&proto.Request{Statements: []*proto.Statement{
		{Sql: "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"},
		{Sql: "INSERT INTO foo(name) VALUES('fiona')"},
	}}, false); err != nil {
		t.Fatalf("failed to write: %s", err)
	}
	if writerCommits != 2 {
		t.Fatalf("writer commit hook fired %d times for 2 writes, want 2", writerCommits)
	}

	// A query goes through a reader, and nothing on a reader fires.
	rows, err := db.Query(&proto.Request{Statements: []*proto.Statement{
		{Sql: "SELECT name FROM foo"},
	}}, false)
	if err != nil {
		t.Fatalf("failed to query: %s", err)
	}
	if exp, got := `[{"columns":["name"],"types":["text"],"values":[["fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
	if readerCommits != 0 || readerPreupdates != 0 {
		t.Fatalf("reader hooks fired: commits=%d, preupdates=%d, want 0 each", readerCommits, readerPreupdates)
	}
	if writerCommits != 2 {
		t.Fatalf("writer commit hook fired on a query: %d, want 2", writerCommits)
	}
}

// Test_SwapInvalidSQLiteFile tests that the Swap function returns an error when provided
// with an invalid SQLite file.
func Test_SwapInvalidSQLiteFile(t *testing.T) {
	// Create a SwappableDB with an empty database
	swappablePath := mustTempPath()
	defer fsutil.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, NewConfig(ConnConfig{}, false))
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Create an invalid SQLite file
	invalidSQLiteFilePath := mustTempPath()
	defer fsutil.Remove(invalidSQLiteFilePath)
	file, err := os.Create(invalidSQLiteFilePath)
	if err != nil {
		t.Fatalf("failed to create invalid SQLite file: %s", err)
	}
	if _, err := file.WriteString("not a valid SQLite file"); err != nil {
		t.Fatalf("failed to write to invalid SQLite file: %s", err)
	}
	file.Close()

	// Attempt to swap with the invalid SQLite file
	err = swappableDB.Swap(invalidSQLiteFilePath)
	if err == nil {
		t.Fatalf("expected an error when swapping with an invalid SQLite file, got nil")
	}
}

// A plausible SQLite header is not enough to accept a replacement. In
// particular, rejecting an invalid replacement must leave the live database,
// including writes still in its WAL, usable.
func Test_SwapCorruptSQLitePreservesDatabase(t *testing.T) {
	for _, wal := range []bool{false, true} {
		for _, corruption := range []string{"header", "table_page"} {
			t.Run(fmt.Sprintf("wal=%t/%s", wal, corruption), func(t *testing.T) {
				dir := t.TempDir()
				s, err := OpenSwappable(filepath.Join(dir, "live.db"), NewConfig(ConnConfig{}, wal))
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				mustExecute(s.db, "CREATE TABLE important (id INTEGER PRIMARY KEY)")
				mustExecute(s.db, "INSERT INTO important VALUES (1)")
				oldDB, oldManager := s.db, s.checkpointMgr

				path := filepath.Join(dir, "replacement.db")
				var data []byte
				if corruption == "header" {
					data = make([]byte, 100)
					copy(data, "SQLite format 3\x00")
				} else {
					candidate, err := Open(path, false, false)
					if err != nil {
						t.Fatal(err)
					}
					mustExecute(candidate, "CREATE TABLE replacement (id INTEGER PRIMARY KEY)")
					mustExecute(candidate, "INSERT INTO replacement VALUES (42)")
					root := mustQuery(candidate, "SELECT rootpage FROM sqlite_schema WHERE name='replacement'")[0].Values[0].Parameters[0].GetI()
					if err := candidate.Close(); err != nil {
						t.Fatal(err)
					}
					data, err = os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					pageSize := int(binary.BigEndian.Uint16(data[16:18]))
					if pageSize == 1 {
						pageSize = 65536
					}
					// Keep the schema intact but invalidate the table's b-tree page.
					data[(int(root)-1)*pageSize] = 0
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				if !IsValidSQLiteFile(path) {
					t.Fatal("fixture must pass the header-only check")
				}
				if err := s.Swap(path); err == nil {
					t.Fatal("expected corrupt replacement to be rejected")
				}
				rows, err := s.QueryStringStmt("SELECT id FROM important")
				if err != nil {
					t.Fatalf("failed swap made the original database unreadable: %s", err)
				}
				if exp, got := `[{"columns":["id"],"types":["integer"],"values":[[1]]}]`, asJSON(rows); got != exp {
					t.Fatalf("original data changed: got %s, want %s", got, exp)
				}
				if s.db != oldDB || s.checkpointMgr != oldManager {
					t.Fatal("failed validation replaced the live database or checkpoint manager")
				}
				res, err := s.Execute(&proto.Request{Statements: []*proto.Statement{{Sql: "INSERT INTO important VALUES (2)"}}}, false)
				if err != nil || len(res) != 1 || res[0].GetE().GetError() != "" {
					t.Fatalf("failed swap made the original database unwritable: %v, %v", res, err)
				}
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("rejected replacement was removed: %s", err)
				}
			})
		}
	}
}

func Test_SwapSuccess_UncheckpointedWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "replacement.db")
	candidate, err := Open(path, false, true)
	if err != nil {
		t.Fatal(err)
	}
	mustExecute(candidate, "CREATE TABLE replacement (id INTEGER PRIMARY KEY)")
	mustExecute(candidate, "INSERT INTO replacement VALUES (42)")
	if err := candidate.Close(); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path + "-wal"); err != nil || fi.Size() == 0 {
		t.Fatalf("expected an uncheckpointed WAL, got %v, %v", fi, err)
	}

	s, err := OpenSwappable(filepath.Join(dir, "live.db"), NewConfig(ConnConfig{}, true))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Swap(path); err != nil {
		t.Fatal(err)
	}
	rows, err := s.QueryStringStmt("SELECT id FROM replacement")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := `[{"columns":["id"],"types":["integer"],"values":[[42]]}]`, asJSON(rows); got != exp {
		t.Fatalf("replacement WAL data lost: got %s, want %s", got, exp)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("replacement file left behind at %s: %v", path+suffix, err)
		}
	}
}
