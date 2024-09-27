package db

import (
	"os"
	"testing"
)

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
		t.Fatalf("expected error opening database")
	}
}
