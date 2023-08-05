package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func Test_IsValidSQLiteOnDisk(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

	dsn := fmt.Sprintf("file:%s", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("failed to create SQLite database: %s", err.Error())
	}
	_, err = db.Exec("CREATE TABLE foo (name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %s", err.Error())
	}

	if !IsValidSQLiteFile(path) {
		t.Fatalf("good SQLite file marked as invalid")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read SQLite file: %s", err.Error())
	}
	if !IsValidSQLiteData(data) {
		t.Fatalf("good SQLite data marked as invalid")
	}
}

func Test_CheckIntegrityOnDisk(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

	dsn := fmt.Sprintf("file:%s", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("failed to create SQLite database: %s", err.Error())
	}
	_, err = db.Exec("CREATE TABLE foo (name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %s", err.Error())
	}

	for _, check := range []bool{true, false} {
		ok, err := CheckIntegrity(path, check)
		if err != nil {
			t.Fatalf("failed to check integrity of database (full=%t): %s", check, err.Error())
		}
		if !ok {
			t.Fatalf("database failed integrity check (full=%t)", check)
		}
	}

	sz := int(mustFileSize(path))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read SQLite file: %s", err.Error())
	}

	// Chop the file in half, integrity check shouldn't even be error-free
	if err := os.WriteFile(path, b[:len(b)-sz/2], 0644); err != nil {
		t.Fatalf("failed to write SQLite file: %s", err.Error())
	}
	for _, check := range []bool{true, false} {
		_, err := CheckIntegrity(path, check)
		if err == nil {
			t.Fatalf("succeeded checking integrity of database (full=%t): %s", check, err.Error())
		}
	}
	// Unable to create a data set that actually fails integrity check.
}

func Test_IsWALModeEnabledOnDiskDELETE(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

	dsn := fmt.Sprintf("file:%s", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("failed to create SQLite database: %s", err.Error())
	}
	_, err = db.Exec("CREATE TABLE foo (name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %s", err.Error())
	}

	if !IsDELETEModeEnabledSQLiteFile(path) {
		t.Fatalf("DELETE file marked as non-DELETE")
	}
	if IsWALModeEnabledSQLiteFile(path) {
		t.Fatalf("non WAL file marked as WAL")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read SQLite file: %s", err.Error())
	}
	if IsWALModeEnabled(data) {
		t.Fatalf("non WAL data marked as WAL")
	}
	if !IsDELETEModeEnabled(data) {
		t.Fatalf("data marked as non-DELETE")
	}
}

func Test_IsWALModeEnabledOnDiskWAL(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

	dsn := fmt.Sprintf("file:%s", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("failed to create SQLite database: %s", err.Error())
	}
	_, err = db.Exec("CREATE TABLE foo (name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		t.Fatalf("failed to enable WAL mode: %s", err.Error())
	}

	_, err = db.Exec(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error inserting record into table: %s", err.Error())
	}

	if !IsValidSQLiteWALFile(path + "-wal") {
		t.Fatalf("WAL file marked not marked as valid")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %s", err.Error())
	}

	if !IsWALModeEnabledSQLiteFile(path) {
		t.Fatalf("WAL file marked as non-WAL")
	}
	if IsDELETEModeEnabledSQLiteFile(path) {
		t.Fatalf("WAL file marked as DELETE")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read SQLite file: %s", err.Error())
	}
	if !IsWALModeEnabled(data) {
		t.Fatalf("WAL data marked as non-WAL")
	}
	if IsDELETEModeEnabled(data) {
		t.Fatalf("WAL data marked as DELETE")
	}
}

// Test_WALDatabaseCreatedOK tests that a WAL file is created, and that
// a checkpoint succeeds
func Test_WALDatabaseCreatedOK(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()

	if !db.WALEnabled() {
		t.Fatalf("WAL mode not enabled")
	}

	if db.InMemory() {
		t.Fatalf("on-disk WAL database marked as in-memory")
	}

	if _, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	if !IsWALModeEnabledSQLiteFile(path) {
		t.Fatalf("SQLite file not marked as WAL")
	}

	walPath := path + "-wal"
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Fatalf("WAL file does not exist")
	}

	if err := db.Checkpoint(5 * time.Second); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
	}
}

// Test_WALDatabaseCreatedOKFromDELETE tests that a WAL database is created properly,
// even when supplied with a DELETE-mode database.
func Test_WALDatabaseCreatedOKFromDELETE(t *testing.T) {
	deletePath := mustTempFile()
	defer os.Remove(deletePath)
	deleteDB, err := Open(deletePath, false, false)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer deleteDB.Close()
	if _, err := deleteDB.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	_, err = deleteDB.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}

	walDB, err := Open(deletePath, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer walDB.Close()
	if !IsWALModeEnabledSQLiteFile(deletePath) {
		t.Fatalf("SQLite file not marked as WAL")
	}
	rows, err := walDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query WAL table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

// Test_DELETEDatabaseCreatedOKFromWAL tests that a DELETE database is created properly,
// even when supplied with a WAL-mode database.
func Test_DELETEDatabaseCreatedOKFromWAL(t *testing.T) {
	walPath := mustTempFile()
	defer os.Remove(walPath)
	walDB, err := Open(walPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer walDB.Close()
	if _, err := walDB.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	_, err = walDB.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}
	if err := walDB.Close(); err != nil {
		// Closing the WAL database is required if it's to be opened in DELETE mode.
		t.Fatalf("failed to close database: %s", err.Error())
	}

	deleteDB, err2 := Open(walPath, false, false)
	if err2 != nil {
		t.Fatalf("failed to open database in DELETE mode: %s", err2.Error())
	}
	defer deleteDB.Close()
	if !IsDELETEModeEnabledSQLiteFile(walPath) {
		t.Fatalf("SQLite file not marked as WAL")
	}
	rows, err := deleteDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query WAL table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

// Test_WALReplayOK tests that WAL files are replayed as expected.
func Test_WALReplayOK(t *testing.T) {
	testFunc := func(t *testing.T, replayIntoDelete bool) {
		dbPath := mustTempFile()
		defer os.Remove(dbPath)
		db, err := Open(dbPath, false, true)
		if err != nil {
			t.Fatalf("failed to open database in WAL mode: %s", err.Error())
		}
		defer db.Close()

		dbFile := filepath.Base(dbPath)
		walPath := dbPath + "-wal"
		walFile := filepath.Base(walPath)

		replayDir := mustTempDir()
		defer os.RemoveAll(replayDir)
		replayDBPath := filepath.Join(replayDir, dbFile)

		// Take over control of checkpointing
		if err := db.DisableCheckpointing(); err != nil {
			t.Fatalf("failed to disable checkpointing: %s", err.Error())
		}

		// Copy the SQLite file and WAL #1
		if _, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
			t.Fatalf("failed to create table: %s", err.Error())
		}
		if !fileExists(walPath) {
			t.Fatalf("WAL file at %s does not exist", walPath)
		}
		mustCopyFile(replayDBPath, dbPath)
		mustCopyFile(filepath.Join(replayDir, walFile+"_001"), walPath)
		if err := db.Checkpoint(5 * time.Second); err != nil {
			t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
		}

		// Copy WAL #2
		_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
		if err != nil {
			t.Fatalf("error executing insertion into table: %s", err.Error())
		}
		if !fileExists(walPath) {
			t.Fatalf("WAL file at %s does not exist", walPath)
		}
		mustCopyFile(filepath.Join(replayDir, walFile+"_002"), walPath)
		if err := db.Checkpoint(5 * time.Second); err != nil {
			t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
		}

		// Copy WAL #3
		_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("declan")`)
		if err != nil {
			t.Fatalf("error executing insertion into table: %s", err.Error())
		}
		if !fileExists(walPath) {
			t.Fatalf("WAL file at %s does not exist", walPath)
		}
		mustCopyFile(filepath.Join(replayDir, walFile+"_003"), walPath)

		if err := db.Close(); err != nil {
			t.Fatalf("failed to close database: %s", err.Error())
		}

		wals := []string{
			filepath.Join(replayDir, walFile+"_001"),
			filepath.Join(replayDir, walFile+"_002"),
			filepath.Join(replayDir, walFile+"_003"),
		}
		if err := ReplayWAL(replayDBPath, wals, replayIntoDelete); err != nil {
			t.Fatalf("failed to replay WAL files: %s", err.Error())
		}

		if replayIntoDelete {
			if !IsDELETEModeEnabledSQLiteFile(replayDBPath) {
				t.Fatal("replayed database not marked as DELETE mode")
			}
		} else {
			if !IsWALModeEnabledSQLiteFile(replayDBPath) {
				t.Fatal("replayed database not marked as WAL mode")
			}
		}

		// Check that there are no files ending in -wal in the replay directory
		walFiles, err := filepath.Glob(filepath.Join(replayDir, "*-wal"))
		if err != nil {
			t.Fatalf("failed to glob replay directory: %s", err.Error())
		}
		if len(walFiles) != 0 {
			t.Fatalf("replay directory contains WAL files: %s", walFiles)
		}

		replayedDB, err := Open(replayDBPath, false, true)
		if err != nil {
			t.Fatalf("failed to open replayed database: %s", err.Error())
		}
		rows, err := replayedDB.QueryStringStmt("SELECT * FROM foo")
		if err != nil {
			t.Fatalf("failed to query WAL table: %s", err.Error())
		}
		if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"declan"]]}]`, asJSON(rows); exp != got {
			t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
		}
	}

	t.Run("replayIntoWAL", func(t *testing.T) {
		testFunc(t, false)
	})
	t.Run("replayIntoDELETE", func(t *testing.T) {
		testFunc(t, true)
	})
}

func Test_WALReplayFailures(t *testing.T) {
	dbDir := mustTempDir()
	defer os.RemoveAll(dbDir)
	walDir := mustTempDir()
	defer os.RemoveAll(walDir)

	err := ReplayWAL(filepath.Join(dbDir, "foo.db"), []string{filepath.Join(walDir, "foo.db-wal")}, false)
	if err != ErrWALReplayDirectoryMismatch {
		t.Fatalf("expected %s, got %s", ErrWALReplayDirectoryMismatch, err.Error())
	}
}

func test_FileCreationOnDisk(t *testing.T, db *DB) {
	defer db.Close()
	if db.InMemory() {
		t.Fatal("on-disk database marked as in-memory")
	}
	if db.FKEnabled() {
		t.Fatal("FK constraints marked as enabled")
	}

	// Confirm checkpoint works on all types of on-disk databases. Worst case, this
	// should be ignored.
	if err := db.Checkpoint(5 * time.Second); err != nil {
		t.Fatalf("failed to checkpoint database in DELETE mode: %s", err.Error())
	}
}

// test_ConnectionIsolationOnDisk test that ISOLATION behavior of on-disk databases doesn't
// change unexpectedly.
func test_ConnectionIsolationOnDisk(t *testing.T, db *DB) {
	r, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt("BEGIN")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for execute, expected %s, got %s", exp, got)
	}

	q, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	_, err = db.ExecuteStringStmt("COMMIT")
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}

	q, err = db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func Test_DatabaseCommonOnDiskOperations(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(*testing.T, *DB)
	}{
		{"FileCreationOnDisk", test_FileCreationOnDisk},
		{"ConnectionIsolationOnDisk", test_ConnectionIsolationOnDisk},
	}

	for _, tc := range testCases {
		db, path := mustCreateOnDiskDatabase()
		defer db.Close()
		defer os.Remove(path)
		t.Run(tc.name+":disk", func(t *testing.T) {
			tc.testFunc(t, db)
		})

		db, path = mustCreateOnDiskDatabaseWAL()
		defer db.Close()
		defer os.Remove(path)
		t.Run(tc.name+":wal", func(t *testing.T) {
			tc.testFunc(t, db)
		})
	}
}
