package db

import (
	"database/sql"
	"os"

	_ "github.com/mattn/go-sqlite3"

	log "code.google.com/p/log4go"
)

const (
	dbName = "db.sqlite"
)

// The SQL database.
type DB struct {
	dbConn *sql.DB
}

// Query result types
type RowResult map[string]string
type RowResults []map[string]string

// New creates a new database. Deletes any existing database.
func New(dbPath string) *DB {
	os.Remove(dbPath)
	return Open(dbPath)
}

// Open an existing database, creating it if it does not exist.
func Open(dbPath string) *DB {
	log.Trace("SQLite database path is %s", dbPath)
	dbc, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Error(err)
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
	rows, err := db.dbConn.Query(query)
	if err != nil {
		log.Error("failed to execute SQLite query", err.Error())
		return nil, err
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
			log.Error("failed to scan SQLite row", err.Error())
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
	log.Debug(func() string { return "Executed query successfully: " + query })
	return results, nil
}

// Execute executes the given sqlite statement, of a type that doesn't return rows.
func (db *DB) Execute(stmt string) error {
	_, err := db.dbConn.Exec(stmt)
	log.Debug(func() string {
		if err != nil {
			return "Error executing " + stmt
		}
		return "Successfully executed " + stmt
	})
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
	})
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
	})
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
	})
	return err
}
