package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
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

func Test_EnsureDelete(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()
	if !IsWALModeEnabledSQLiteFile(path) {
		t.Fatalf("WAL file marked as non-WAL")
	}

	if err := EnsureDeleteMode(path); err != nil {
		t.Fatalf("failed to ensure DELETE mode: %s", err.Error())
	}
	if !IsDELETEModeEnabledSQLiteFile(path) {
		t.Fatalf("database not marked as DELETE mode")
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

		n, err := db.GetCheckpointing()
		if err != nil {
			t.Fatalf("failed to get checkpoint value: %s", err.Error())
		}
		if exp, got := 0, n; exp != got {
			t.Fatalf("unexpected checkpoint value, expected %d, got %d", exp, got)
		}

		dbFile := filepath.Base(dbPath)
		walPath := dbPath + "-wal"
		walFile := filepath.Base(walPath)

		replayDir := mustTempDir()
		defer os.RemoveAll(replayDir)
		replayDBPath := filepath.Join(replayDir, dbFile)

		// Create and copy the SQLite file and WAL #1
		if _, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
			t.Fatalf("failed to create table: %s", err.Error())
		}
		if !fileExists(walPath) {
			t.Fatalf("WAL file at %s does not exist", walPath)
		}
		mustCopyFile(replayDBPath, dbPath)
		mustCopyFile(filepath.Join(replayDir, walFile+"_001"), walPath)
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
		}

		// Create and copy WAL #2
		_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
		if err != nil {
			t.Fatalf("error executing insertion into table: %s", err.Error())
		}
		if !fileExists(walPath) {
			t.Fatalf("WAL file at %s does not exist", walPath)
		}
		mustCopyFile(filepath.Join(replayDir, walFile+"_002"), walPath)
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
		}

		// Create and copy WAL #3
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

		// Check that there are no files related to WALs in the replay directory
		// Both the copied WAL files should be gone, and there should be no
		// "real" WAL file either.
		walFiles, err := filepath.Glob(filepath.Join(replayDir, "*-wal*"))
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
