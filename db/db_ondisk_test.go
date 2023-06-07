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
