package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dbName = "db.sqlite"
)

// The SQL database.
type DB struct {
	dbConn *sql.DB
}

// Creates a new database.
func New(dir string) *DB {
	path := path.Join(dir, dbName)
	os.Remove(path)

	fmt.Println("database path is", path)
	dbc, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}
	return &DB{
		dbConn: dbc,
	}
}

// Executes the query.
func (db *DB) Query(query string) string {
	return "the query"
}

// Sets the value for a given key.
func (db *DB) Exec(stmt string) {
	return
}
