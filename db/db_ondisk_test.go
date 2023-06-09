package db

import (
	"database/sql"
	"fmt"
	"os"
	"path"
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

func Test_FileCreationOnDisk(t *testing.T) {
	dir := t.TempDir()
	dbPath := path.Join(dir, "test_db")

	db, err := Open(dbPath, false)
	if err != nil {
		t.Fatalf("failed to open new database: %s", err.Error())
	}
	if db == nil {
		t.Fatal("database is nil")
	}
	if db.InMemory() {
		t.Fatal("on-disk database marked as in-memory")
	}
	if db.FKEnabled() {
		t.Fatal("FK constraints marked as enabled")
	}
	if db.Path() != dbPath {
		t.Fatal("database path is incorrect")
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("%s does not exist after open", dbPath)
	}
	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close database: %s", err.Error())
	}
}

// Test_ConnectionIsolationOnDisk test that ISOLATION behavior of on-disk databases doesn't
// change unexpectedly.
func Test_ConnectionIsolationOnDisk(t *testing.T) {
	dir := t.TempDir()
	dbPath := path.Join(dir, "test_db")

	db, err := Open(dbPath, false)
	if err != nil {
		t.Fatalf("failed to open new database: %s", err.Error())
	}

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
