package db

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/encoding"
	command "github.com/rqlite/rqlite/v8/command/proto"
)

// Test_OpenNonExistentDatabase tests that opening a non-existent database
// works OK. It should.
func Test_OpenNonExistentDatabase(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	_, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("error opening non-existent database")
	}
	// Confirm a file was created.
	if !fileExists(path) {
		t.Fatalf("database file not created")
	}
}

func Test_WALRemovedOnClose(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)
	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("error opening non-existent database")
	}
	defer db.Close()
	if !db.WALEnabled() {
		t.Fatalf("WAL mode not enabled")
	}

	_, err = db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	walPath := db.WALPath()
	if !fileExists(walPath) {
		t.Fatalf("WAL file does not exist after creating a table")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("error closing database: %s", err.Error())
	}

	if fileExists(db.WALPath()) {
		t.Fatalf("WAL file not removed after closing the database")
	}
}

func Test_RemoveFiles(t *testing.T) {
	d := t.TempDir()
	mustCreateClosedFile(fmt.Sprintf("%s/foo", d))
	mustCreateClosedFile(fmt.Sprintf("%s/foo-wal", d))

	if err := RemoveFiles(fmt.Sprintf("%s/foo", d)); err != nil {
		t.Fatalf("failed to remove files: %s", err.Error())
	}

	files, err := os.ReadDir(d)
	if err != nil {
		t.Fatalf("failed to read directory: %s", err.Error())
	}
	if len(files) != 0 {
		t.Fatalf("expected directory to be empty, but wasn't")
	}
}

func Test_DBPaths(t *testing.T) {
	dbWAL, pathWAL := mustCreateOnDiskDatabaseWAL()
	defer dbWAL.Close()
	defer os.Remove(pathWAL)
	if exp, got := pathWAL, dbWAL.Path(); exp != got {
		t.Fatalf("expected path %s, got %s", exp, got)
	}
	if exp, got := pathWAL+"-wal", dbWAL.WALPath(); exp != got {
		t.Fatalf("expected WAL path %s, got %s", exp, got)
	}
	if p1, p2 := WALPath(pathWAL), dbWAL.WALPath(); p1 != p2 {
		t.Fatalf("WAL paths are not equal (%s != %s)", p1, p2)
	}

	db, path := mustCreateOnDiskDatabase()
	defer db.Close()
	defer os.Remove(path)
	if exp, got := path, db.Path(); exp != got {
		t.Fatalf("expected path %s, got %s", exp, got)
	}
	if exp, got := "", db.WALPath(); exp != got {
		t.Fatalf("expected WAL path %s, got %s", exp, got)
	}
}

// Test_TableCreation tests basic operation of an database
func Test_TableCreation(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(path)

	r, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	testQ := func() {
		t.Helper()
		q, err := db.QueryStringStmt("SELECT * FROM foo")
		if err != nil {
			t.Fatalf("failed to query empty table: %s", err.Error())
		}
		if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(q); exp != got {
			t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
		}
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}
	testQ()

	// Confirm checkpoint works without error.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
	testQ()

	// Check that VACUUM returns without error.
	if err := db.Vacuum(); err != nil {
		t.Fatalf("failed to VACUUM database: %s", err.Error())
	}
	testQ()
}

// Test_TableCreationFK ensures foreign key constraints work
func Test_TableCreationFK(t *testing.T) {
	createTableFoo := "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"
	createTableBar := "CREATE TABLE bar (fooid INTEGER NOT NULL PRIMARY KEY, FOREIGN KEY(fooid) REFERENCES foo(id))"
	insertIntoBar := "INSERT INTO bar(fooid) VALUES(1)"

	db, path := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(path)

	r, err := db.ExecuteStringStmt(createTableFoo)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(createTableBar)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(insertIntoBar)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	// Now, do same testing with FK constraints enabled.
	dbFK, path := mustCreateOnDiskDatabaseWALFK()
	defer dbFK.Close()
	defer os.Remove(path)

	if !dbFK.FKEnabled() {
		t.Fatal("FK constraints not marked as enabled")
	}

	r, err = dbFK.ExecuteStringStmt(createTableFoo)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = dbFK.ExecuteStringStmt(createTableBar)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = dbFK.ExecuteStringStmt(insertIntoBar)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"error":"FOREIGN KEY constraint failed"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func Test_ConcurrentQueries(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(path)

	r, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	for i := 0; i < 5000; i++ {
		r, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
		if err != nil {
			t.Fatalf("failed to insert record: %s", err.Error())
		}
		if exp, got := fmt.Sprintf(`[{"last_insert_id":%d,"rows_affected":1}]`, i+1), asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ro, err := db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
			if err != nil {
				t.Logf("failed to query table: %s", err.Error())
			}
			if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[5000]]}]`, asJSON(ro); exp != got {
				t.Logf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
			}
		}()
	}
	wg.Wait()
}

func Test_SimpleTransaction(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(3, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	r, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"last_insert_id":3,"rows_affected":1},{"last_insert_id":4,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_PartialFailTransaction(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	r, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
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

func Test_DBLastModified(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)
	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()

	lm, err := db.LastModified()
	if err != nil {
		t.Fatalf("failed to get last modified time: %s", err.Error())
	}
	if lm.IsZero() {
		t.Fatalf("last modified time is zero")
	}

	// Write some data, check time is later.
	_, err = db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	lm2, err := db.LastModified()
	if err != nil {
		t.Fatalf("failed to get last modified time: %s", err.Error())
	}
	if lm2.Before(lm) {
		t.Fatalf("last modified time not updated")
	}

	// Checkpoint, check time is later.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
	lm3, err := db.LastModified()
	if err != nil {
		t.Fatalf("failed to get last modified time: %s", err.Error())
	}
	if lm3.Before(lm2) {
		t.Fatalf("last modified time not updated after checkpoint")
	}

	// Call again, without changes, check time is same.
	lm4, err := db.LastModified()
	if err != nil {
		t.Fatalf("failed to get last modified time: %s", err.Error())
	}
	if !lm4.Equal(lm3) {
		t.Fatalf("last modified time updated without changes")
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

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
	}
	// Checkpoint a second time, to ensure it's idempotent.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
	}
}

// Test_WALDatabaseCheckpointOKNoWAL tests that a checkpoint succeeds
// even when no WAL file exists.
func Test_WALDatabaseCheckpointOKNoWAL(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	if !db.WALEnabled() {
		t.Fatalf("WAL mode not enabled")
	}
	if fileExists(db.WALPath()) {
		t.Fatalf("WAL file exists when no writes have happened")
	}
	defer db.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode with non-existent WAL: %s", err.Error())
	}
}

// Test_WALDatabaseCheckpointOKDelete tests that a checkpoint returns no error
// even when the database is opened in DELETE mode.
func Test_WALDatabaseCheckpointOKDelete(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("failed to open database in DELETE mode: %s", err.Error())
	}
	if db.WALEnabled() {
		t.Fatalf("WAL mode enabled")
	}
	defer db.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint database in DELETE mode: %s", err.Error())
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

func Test_WALDisableCheckpointing(t *testing.T) {
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

	// Test that databases open with checkpoint disabled by default.
	// This is critical.
	n, err := db.GetCheckpointing()
	if err != nil {
		t.Fatalf("failed to get checkpoint value: %s", err.Error())
	}
	if exp, got := 0, n; exp != got {
		t.Fatalf("unexpected checkpoint value, expected %d, got %d", exp, got)
	}

	if err := db.EnableCheckpointing(); err != nil {
		t.Fatalf("failed to disable checkpointing: %s", err.Error())
	}
	n, err = db.GetCheckpointing()
	if err != nil {
		t.Fatalf("failed to get checkpoint value: %s", err.Error())
	}
	if exp, got := 1000, n; exp != got {
		t.Fatalf("unexpected checkpoint value, expected %d, got %d", exp, got)
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

func test_FileCreationOnDisk(t *testing.T, db *DB) {
	defer db.Close()
	if db.FKEnabled() {
		t.Fatal("FK constraints marked as enabled")
	}

	// Confirm checkpoint works on all types of on-disk databases. Worst case, this
	// should be ignored.
	if err := db.Checkpoint(); err != nil {
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

// Test_ParallelOperationsInMemory runs multiple accesses concurrently, ensuring
// that correct results are returned in every goroutine. It's not 100% that this
// test would bring out a bug, but it's almost 100%.
//
// See https://github.com/mattn/go-sqlite3/issues/959#issuecomment-890283264
func Test_ParallelOperationsInMemory(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(path)

	if _, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	if _, err := db.ExecuteStringStmt("CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	if _, err := db.ExecuteStringStmt("CREATE TABLE qux (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Confirm schema is as expected, when checked from same goroutine.
	if rows, err := db.QueryStringStmt(`SELECT sql FROM sqlite_master`); err != nil {
		t.Fatalf("failed to query for schema after creation: %s", err.Error())
	} else {
		if exp, got := `[{"columns":["sql"],"types":["text"],"values":[["CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"],["CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"],["CREATE TABLE qux (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"]]}]`, asJSON(rows); exp != got {
			t.Fatalf("schema not as expected during after creation, exp %s, got %s", exp, got)
		}
	}

	var exWg sync.WaitGroup
	exWg.Add(3)

	foo := make(chan time.Time)
	bar := make(chan time.Time)
	qux := make(chan time.Time)
	done := make(chan bool)

	ticker := time.NewTicker(1 * time.Millisecond)
	go func() {
		for {
			select {
			case t := <-ticker.C:
				foo <- t
				bar <- t
				qux <- t
			case <-done:
				close(foo)
				close(bar)
				close(qux)
				return
			}
		}
	}()

	go func() {
		defer exWg.Done()
		for range foo {
			if _, err := db.ExecuteStringStmt(`INSERT INTO foo(id, name) VALUES(1, "fiona")`); err != nil {
				t.Logf("failed to insert records into foo: %s", err.Error())
			}
		}
	}()
	go func() {
		defer exWg.Done()
		for range bar {
			if _, err := db.ExecuteStringStmt(`INSERT INTO bar(id, name) VALUES(1, "fiona")`); err != nil {
				t.Logf("failed to insert records into bar: %s", err.Error())
			}
		}
	}()
	go func() {
		defer exWg.Done()
		for range qux {
			if _, err := db.ExecuteStringStmt(`INSERT INTO qux(id, name) VALUES(1, "fiona")`); err != nil {
				t.Logf("failed to insert records into qux: %s", err.Error())
			}
		}
	}()

	var qWg sync.WaitGroup
	qWg.Add(3)
	for i := 0; i < 3; i++ {
		go func(j int) {
			defer qWg.Done()
			var n int
			for {
				if rows, err := db.QueryStringStmt(`SELECT sql FROM sqlite_master`); err != nil {
					t.Logf("failed to query for schema during goroutine %d execution: %s", j, err.Error())
				} else {
					n++
					if exp, got := `[{"columns":["sql"],"types":["text"],"values":[["CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"],["CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"],["CREATE TABLE qux (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"]]}]`, asJSON(rows); exp != got {
						t.Logf("schema not as expected during goroutine execution, exp %s, got %s, after %d queries", exp, got, n)
					}
				}
				if n == 500000 {
					break
				}
			}
		}(i)
	}
	qWg.Wait()

	close(done)
	exWg.Wait()
}

func mustCreateOnDiskDatabase() (*DB, string) {
	var err error
	f := mustTempFile()
	db, err := Open(f, false, false)
	if err != nil {
		panic("failed to open database in DELETE mode")
	}
	return db, f
}

func mustCreateOnDiskDatabaseWAL() (*DB, string) {
	var err error
	f := mustTempFile()
	db, err := Open(f, false, true)
	if err != nil {
		panic("failed to open database in WAL mode")
	}
	return db, f
}

func mustCreateOnDiskDatabaseWALFK() (*DB, string) {
	var err error
	f := mustTempFile()
	db, err := Open(f, true, true)
	if err != nil {
		panic("failed to open database in WAL mode")
	}
	return db, f
}

// mustExecute executes a statement, and panics on failure. Used for statements
// that should never fail, even taking into account test setup.
func mustExecute(db *DB, stmt string) {
	r, err := db.ExecuteStringStmt(stmt)
	if err != nil {
		panic(fmt.Sprintf("failed to execute statement: %s", err.Error()))
	}
	if r[0].Error != "" {
		panic(fmt.Sprintf("failed to execute statement: %s", r[0].Error))
	}
}

func asJSON(v interface{}) string {
	enc := encoding.Encoder{}
	b, err := enc.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}

// mustTempPath returns a path which can be used for a temporary file or directory.
// No file will exist at the path.
func mustTempPath() string {
	tmpfile, err := os.CreateTemp("", "rqlite-db-test")
	if err != nil {
		panic(err.Error())
	}
	tmpfile.Close()
	if err := os.Remove(tmpfile.Name()); err != nil {
		panic(err.Error())
	}
	return tmpfile.Name()
}

// mustTempFile returns a path to a temporary file in directory dir. It is up to the
// caller to remove the file once it is no longer needed.
func mustTempFile() string {
	tmpfile, err := os.CreateTemp("", "rqlite-db-test")
	if err != nil {
		panic(err.Error())
	}
	tmpfile.Close()
	return tmpfile.Name()
}

func mustTempDir() string {
	tmpdir, err := os.MkdirTemp("", "rqlite-db-test")
	if err != nil {
		panic(err.Error())
	}
	return tmpdir
}

// function which copies a src file to a dst file, panics if any error
func mustCopyFile(dst, src string) {
	srcFile, err := os.Open(src)
	if err != nil {
		panic(err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		panic(err)
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		panic(err)
	}
}

func mustCreateClosedFile(path string) {
	f, err := os.Create(path)
	if err != nil {
		panic("failed to create file")
	}
	if err := f.Close(); err != nil {
		panic("failed to close file")
	}
}

func mustStat(path string) os.FileInfo {
	fi, err := os.Stat(path)
	if err != nil {
		panic("failed to stat file")
	}
	return fi
}

func mustFileSize(path string) int64 {
	fi := mustStat(path)
	return fi.Size()
}
