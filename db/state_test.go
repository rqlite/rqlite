package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func Test_MakeDSN(t *testing.T) {
	tests := []struct {
		path       string
		readOnly   bool
		fkEnabled  bool
		walEnabled bool
		want       string
	}{
		{
			path: "foo.db",
			want: "file:foo.db?_fk=false&_journal=DELETE&_sync=0",
		},
		{
			path:     "foo.db",
			readOnly: true,
			want:     "file:foo.db?_fk=false&_journal=DELETE&_sync=0&mode=ro",
		},
		{
			path:       "foo.db",
			readOnly:   true,
			walEnabled: true,
			want:       "file:foo.db?_fk=false&_journal=WAL&_sync=0&mode=ro",
		},
		{
			path:      "foo.db",
			readOnly:  true,
			fkEnabled: true,
			want:      "file:foo.db?_fk=true&_journal=DELETE&_sync=0&mode=ro",
		},
	}

	for _, test := range tests {
		got := MakeDSN(test.path, test.readOnly, test.fkEnabled, test.walEnabled)
		if test.want != got {
			t.Fatalf("expected %s, got %s", test.want, got)
		}
	}
}

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

func Test_WALReplayOK_Complex(t *testing.T) {
	srcPath := mustTempFile()
	defer os.Remove(srcPath)
	srcWALPath := srcPath + "-wal"
	dstPath := srcPath + "-dst"

	srcDB, err := Open(srcPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer srcDB.Close()

	if _, err := srcDB.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if err := srcDB.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
	}
	mustCopyFile(dstPath, srcPath)

	var dstWALs []string
	defer func() {
		for _, p := range dstWALs {
			os.Remove(p)
		}
	}()
	for i := 0; i < 20; i++ {
		for j := 0; j < 2000; j++ {
			if _, err := srcDB.ExecuteStringStmt(fmt.Sprintf(`INSERT INTO foo(name) VALUES("fiona-%d")`, i)); err != nil {
				t.Fatalf("error executing insertion into table: %s", err.Error())
			}
		}
		dstWALPath := fmt.Sprintf("%s-%d", dstPath, i)
		mustCopyFile(dstWALPath, srcWALPath)
		dstWALs = append(dstWALs, dstWALPath)

		// if i%5 == 0 {
		// 	if err := srcDB.Vacuum(); err != nil {
		// 		t.Fatalf("failed to vacuum database during INSERT: %s", err.Error())
		// 	}
		// }
		if err := srcDB.Checkpoint(); err != nil {
			t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
		}
	}

	// Create some other type of transactions in src - first DELETE, then UPDATE.
	if _, err := srcDB.ExecuteStringStmt(`DELETE FROM foo WHERE id >= 100 AND id <= 200`); err != nil {
		t.Fatalf("error executing deletion from table: %s", err.Error())
	}
	dstWALPath := fmt.Sprintf("%s-postdelete", dstPath)
	mustCopyFile(dstWALPath, srcWALPath)
	dstWALs = append(dstWALs, dstWALPath)
	if err := srcDB.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
	}

	if _, err := srcDB.ExecuteStringStmt(`UPDATE foo SET name="fiona-updated" WHERE id >= 300 AND id <= 600`); err != nil {
		t.Fatalf("error executing update of table: %s", err.Error())
	}
	dstWALPath = fmt.Sprintf("%s-postupdate", dstPath)
	mustCopyFile(dstWALPath, srcWALPath)
	dstWALs = append(dstWALs, dstWALPath)
	// if err := srcDB.Vacuum(); err != nil {
	// 	t.Fatalf("failed to vacuum database post UPDATE: %s", err.Error())
	// }
	if err := srcDB.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
	}

	for i := 0; i < 20; i++ {
		createTable := fmt.Sprintf("CREATE TABLE bar%d (id INTEGER NOT NULL PRIMARY KEY, name TEXT)", i)
		if _, err := srcDB.ExecuteStringStmt(createTable); err != nil {
			t.Fatalf("failed to create table: %s", err.Error())
		}
	}
	dstWALPath = fmt.Sprintf("%s-create-tables", dstPath)
	mustCopyFile(dstWALPath, srcWALPath)
	dstWALs = append(dstWALs, dstWALPath)
	if err := srcDB.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
	}

	// Replay all the WALs into dst and check the data looks good. Then compare
	// the data in src and dst.
	if err := ReplayWAL(dstPath, dstWALs, false); err != nil {
		t.Fatalf("failed to replay WALs: %s", err.Error())
	}
	dstDB, err := Open(dstPath, false, true)
	if err != nil {
		t.Fatalf("failed to open dst database: %s", err.Error())
	}
	defer dstDB.Close()

	// Run various queries to make sure src and dst are the same.
	for _, q := range []string{
		"SELECT COUNT(*) FROM foo",
		"SELECT COUNT(*) FROM foo WHERE name='fiona-updated'",
		"SELECT COUNT(*) FROM foo WHERE name='no-one'",
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
	} {
		r, err := srcDB.QueryStringStmt(q)
		if err != nil {
			t.Fatalf("failed to query table: %s", err.Error())
		}
		srcRes := asJSON(r)

		r, err = dstDB.QueryStringStmt(q)
		if err != nil {
			t.Fatalf("failed to query table: %s", err.Error())
		}
		if exp, got := srcRes, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query (%s) of dst, expected %s, got %s", q, exp, got)
		}
	}
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
