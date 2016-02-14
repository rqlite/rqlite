package db

import (
	"database/sql"
	"os"

	_ "github.com/mattn/go-sqlite3" // required blank import
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
func New(dbPath string) (*DB, error) {
	_ = os.Remove(dbPath)
	return Open(dbPath)
}

// Open an existing database, creating it if it does not exist.
func Open(dbPath string) (*DB, error) {
	dbc, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	return &DB{
		dbConn: dbc,
	}, nil
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
		return nil, err
	}
	defer func() {
		rows.Close()
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
	return results, nil
}

// Execute executes the given sqlite statement, of a type that doesn't return rows.
func (db *DB) Execute(stmt string) error {
	_, err := db.dbConn.Exec(stmt)
	return err
}

// StartTransaction starts an explicit transaction.
func (db *DB) StartTransaction() error {
	_, err := db.dbConn.Exec("BEGIN")
	return err
}

// CommitTransaction commits all changes made since StartTraction was called.
func (db *DB) CommitTransaction() error {
	_, err := db.dbConn.Exec("END")
	return err
}

// RollbackTransaction aborts the transaction. No statement issued since
// StartTransaction was called will take effect.
func (db *DB) RollbackTransaction() error {
	_, err := db.dbConn.Exec("ROLLBACK")
	return err
}
