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

type RowResult map[string]string
type RowResults []map[string]string

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
func (db *DB) Query(query string) RowResults {
	rows, err := db.dbConn.Query(query)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer rows.Close()

	results := make(RowResults, 0)

	columns, _ := rows.Columns()
	rawResult := make([][]byte, len(columns))
	dest := make([]interface{}, len(columns)) // A temporary interface{} slice
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			log.Fatal("Failed to scan row", err)
		}

		r := make(RowResult)
		for i, raw := range rawResult {
			if raw == nil {
				r[columns[i]] = "null"
			} else {
				r[columns[i]] = string(raw)
			}
		}
		results = append(results, r)
	}
	return results
}

// Sets the value for a given key.
func (db *DB) Exec(stmt string) {
	_, _ = db.dbConn.Exec(stmt)
	return
}
