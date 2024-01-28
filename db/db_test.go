package db

import (
	"database/sql"
	"fmt"
	"io"
	"os"
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
		t.Fatalf("error opening non-existent database: %s", err.Error())
	}
	// Confirm a file was created.
	if !fileExists(path) {
		t.Fatalf("database file not created at %s", path)
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
	if err := db.Checkpoint(CheckpointRestart); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
	testQ()
}

func Test_DBSums(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(path)

	getSums := func() (string, string) {
		sumDB, err := db.DBSum()
		if err != nil {
			t.Fatalf("failed to get DB checksum: %s", err.Error())
		}
		sumWAL, err := db.WALSum()
		if err != nil {
			t.Fatalf("failed to get WAL checksum: %s", err.Error())
		}
		return sumDB, sumWAL
	}

	r, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	sumDBPre, sumWALPre := getSums()
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}
	sumDBPost, sumWALPost := getSums()
	if sumDBPost != sumDBPre {
		t.Fatalf("DB sum changed after insertion")
	}
	if sumWALPost == sumWALPre {
		t.Fatalf("WAL sum did not change after insertion")
	}

	if err := db.Checkpoint(CheckpointRestart); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}

	sumDBPostChk, _ := getSums()
	if sumDBPostChk == sumDBPost {
		t.Fatalf("DB sum did not change after checkpoint")
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
	lmDB, err := db.DBLastModified()
	if err != nil {
		t.Fatalf("failed to get last modified time: %s", err.Error())
	}
	if lmDB.IsZero() {
		t.Fatalf("last modified time is zero")
	}

	// Write some data, check times are later.
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
	lmDB2, err := db.DBLastModified()
	if err != nil {
		t.Fatalf("failed to get last modified time: %s", err.Error())
	}
	if !lmDB2.Equal(lmDB) {
		t.Fatalf("last modified time changed for DB even though only WAL should have changed")
	}

	// Checkpoint, and check time is later. On some platforms the time resolution isn't that
	// high, so we sleep so the test won't suffer a false failure.
	time.Sleep(1 * time.Second)
	if err := db.Checkpoint(CheckpointRestart); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
	lm3, err := db.LastModified()
	if err != nil {
		t.Fatalf("failed to get last modified time: %s", err.Error())
	}
	if lm3.Before(lm2) {
		t.Fatalf("last modified time not updated after checkpoint")
	}
	lmDB3, err := db.DBLastModified()
	if err != nil {
		t.Fatalf("failed to get last modified time: %s", err.Error())
	}
	if !lmDB3.After(lmDB2) {
		t.Fatalf("last modified time not updated for DB after checkpoint")
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

func Test_DBVacuum(t *testing.T) {
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

	// Confirm VACUUM works without error and that only the WAL file is altered.
	sumDBPre, err := db.DBSum()
	if err != nil {
		t.Fatalf("failed to get DB checksum: %s", err.Error())
	}
	sumWALPre, err := db.WALSum()
	if err != nil {
		t.Fatalf("failed to get WAL checksum: %s", err.Error())
	}

	if err := db.Vacuum(); err != nil {
		t.Fatalf("failed to vacuum database: %s", err.Error())
	}
	testQ()

	sumDBPost, err := db.DBSum()
	if err != nil {
		t.Fatalf("failed to get DB checksum: %s", err.Error())
	}
	sumWALPost, err := db.WALSum()
	if err != nil {
		t.Fatalf("failed to get WAL checksum: %s", err.Error())
	}

	if sumDBPost != sumDBPre {
		t.Fatalf("DB sum changed after VACUUM")
	}
	if sumWALPost == sumWALPre {
		t.Fatalf("WAL sum did not change after VACUUM")
	}
}

func Test_DBVacuumInto(t *testing.T) {
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
	for i := 0; i < 1000; i++ {
		_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
		if err != nil {
			t.Fatalf("error executing insertion into table: %s", err.Error())
		}
	}

	testQ := func(d *DB) {
		t.Helper()
		q, err := d.QueryStringStmt("SELECT COUNT(*) FROM foo")
		if err != nil {
			t.Fatalf("failed to query empty table: %s", err.Error())
		}
		if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1000]]}]`, asJSON(q); exp != got {
			t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
		}
	}
	testQ(db)

	// VACUUM INTO an empty file, open the database, and check it's correct.
	tmpPath := mustTempFile()
	defer os.Remove(tmpPath)
	if err := db.VacuumInto(tmpPath); err != nil {
		t.Fatalf("failed to vacuum database: %s", err.Error())
	}
	vDB, err := Open(tmpPath, false, false)
	if err != nil {
		t.Fatalf("failed to open database: %s", err.Error())
	}
	defer vDB.Close()
	testQ(vDB)

	// VACUUM INTO an non-existing file, should still work.
	tmpPath2 := mustTempPath()
	defer os.Remove(tmpPath2)
	if err := db.VacuumInto(tmpPath2); err != nil {
		t.Fatalf("failed to vacuum database: %s", err.Error())
	}
	vDB2, err := Open(tmpPath2, false, false)
	if err != nil {
		t.Fatalf("failed to open database: %s", err.Error())
	}
	defer vDB2.Close()
	testQ(vDB2)

	// VACUUM into a file which is an existing SQLIte database. Should fail.
	existDB, existPath := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(existPath)
	_, err = existDB.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if err := existDB.VacuumInto(existPath); err == nil {
		t.Fatalf("expected error vacuuming into existing database file")
	}
	_, err = Open(existPath, false, false)
	if err == nil {
		t.Fatalf("expected error opening database: %s", err.Error())
	}
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
	if mustFileSize(walPath) == 0 {
		t.Fatalf("WAL file exists but is empty")
	}

	if err := db.Checkpoint(CheckpointTruncate); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode: %s", err.Error())
	}
	if mustFileSize(walPath) != 0 {
		t.Fatalf("WAL file exists but is non-empty")
	}
	// Checkpoint a second time, to ensure it's idempotent.
	if err := db.Checkpoint(CheckpointTruncate); err != nil {
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

func test_FileCreationOnDisk(t *testing.T, db *DB) {
	defer db.Close()
	if db.FKEnabled() {
		t.Fatal("FK constraints marked as enabled")
	}

	// Confirm checkpoint works on all types of on-disk databases. Worst case, this
	// should be ignored.
	if err := db.Checkpoint(CheckpointRestart); err != nil {
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
