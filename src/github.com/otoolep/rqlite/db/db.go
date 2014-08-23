package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dbName = "db.sqlite"
)

// Errors
var RowScanError = errors.New("Row scan failure")
var QueryExecuteError = errors.New("Query execute error")

// The SQL database.
type DB struct {
	dbConn *sql.DB
}

// Query result types
type RowResult map[string]string
type RowResults []map[string]string

// New creates a new database.
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

// Query runs the supplied query against the sqlite database. It returns a slice of
// RowResults.
func (db *DB) Query(query string) (RowResults, error) {
	rows, err := db.dbConn.Query(query)
	if err != nil {
		log.Fatal("failed to execute query", err.Error())
		return nil, QueryExecuteError
	}
	defer rows.Close()

	results := make(RowResults, 0)

	columns, _ := rows.Columns()
	rawResult := make([][]byte, len(columns))
	dest := make([]interface{}, len(columns))
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			log.Fatal("failed to scan row", err)
			return nil, RowScanError
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
	return results, nil
}

// Execute executes the given sqlite statement, of a type that doesn't return rows.
func (db *DB) Execute(stmt string) error {
	_, err := db.dbConn.Exec(stmt)
	return err
}
