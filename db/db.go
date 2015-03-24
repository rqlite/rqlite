package db

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3" // required blank import
	"github.com/otoolep/rqlite/log"
)

// DB is the SQL database.
type DB struct {
	dbConn *sql.DB
}

// RowResult is the result type.
type RowResult map[string]string

// RowResults is the list of results.
type RowResults []map[string]string

// New creates a new database. Deletes any existing database.
func New(dbPath string) *DB {
	log.Tracef("Removing any existing SQLite database at %s", dbPath)
	_ = os.Remove(dbPath)
	return Open(dbPath)
}

// Open an existing database, creating it if it does not exist.
func Open(dbPath string) *DB {
	log.Tracef("Opening SQLite database path at %s", dbPath)
	dbc, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	return &DB{
		dbConn: dbc,
	}
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	return db.dbConn.Close()
}

// Query runs the supplied query against the sqlite database. It returns a slice of
// RowResults.
func (db *DB) Query(query string) (RowResults, error) {
	if !strings.HasPrefix(strings.ToUpper(query), "SELECT ") {
		log.Warnf("Query \"%s\" may modify the database", query)
	}
	rows, err := db.dbConn.Query(query)
	if err != nil {
		log.Errorf("failed to execute SQLite query: %s", err.Error())
		return nil, err
	}
	defer func() {
		err = rows.Close()
		if err != nil {
			log.Errorf("failed to close rows: %s", err.Error())
		}
	}()

	results := make(RowResults, 0)

	columns, _ := rows.Columns()
	rawResult := make([][]byte, len(columns))
	dest := make([]interface{}, len(columns))
	for i := range rawResult {
		dest[i] = &rawResult[i] // Pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			log.Errorf("failed to scan SQLite row: %s", err.Error())
			return nil, err
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
	log.Debugf("Executed query successfully: %s", query)
	return results, nil
}

// Execute executes the given sqlite statement, of a type that doesn't return rows.
func (db *DB) Execute(stmt string) error {
	_, err := db.dbConn.Exec(stmt)
	log.Debug(func() string {
		if err != nil {
			return fmt.Sprintf("Error executing \"%s\", error: %s", stmt, err.Error())
		}
		return fmt.Sprintf("Successfully executed \"%s\"", stmt)
	}())

	return err
}

// StartTransaction starts an explicit transaction.
func (db *DB) StartTransaction() error {
	_, err := db.dbConn.Exec("BEGIN")
	log.Debug(func() string {
		if err != nil {
			return "Error starting transaction"
		}
		return "Successfully started transaction"
	}())
	return err
}

// CommitTransaction commits all changes made since StartTraction was called.
func (db *DB) CommitTransaction() error {
	_, err := db.dbConn.Exec("END")
	log.Debug(func() string {
		if err != nil {
			return "Error ending transaction"
		}
		return "Successfully ended transaction"
	}())
	return err
}

// RollbackTransaction aborts the transaction. No statement issued since
// StartTransaction was called will take effect.
func (db *DB) RollbackTransaction() error {
	_, err := db.dbConn.Exec("ROLLBACK")
	log.Debug(func() string {
		if err != nil {
			return "Error rolling back transaction"
		}
		return "Successfully rolled back transaction"
	}())
	return err
}
