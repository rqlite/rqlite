// Package db exposes a lightweight abstraction over the SQLite code.
// It performs some basic mapping of lower-level types to rqlite types.
package db

import (
	"database/sql/driver"
	"fmt"
	"io"
	"time"

	"github.com/mattn/go-sqlite3"
)

const bkDelay = 250

// DBVersion is the SQLite version.
var DBVersion string

func init() {
	DBVersion, _, _ = sqlite3.Version()
}

// DB is the SQL database.
type DB struct {
	sqlite3conn *sqlite3.SQLiteConn // Driver connection to database.
	path        string              // Path to database file.
	dsn         string              // DSN, if any.
	memory      bool                // In-memory only.
}

// Result represents the outcome of an operation that changes rows.
type Result struct {
	LastInsertID int64   `json:"last_insert_id,omitempty"`
	RowsAffected int64   `json:"rows_affected,omitempty"`
	Error        string  `json:"error,omitempty"`
	Time         float64 `json:"time,omitempty"`
}

// Rows represents the outcome of an operation that returns query data.
type Rows struct {
	Columns []string        `json:"columns,omitempty"`
	Types   []string        `json:"types,omitempty"`
	Values  [][]interface{} `json:"values,omitempty"`
	Error   string          `json:"error,omitempty"`
	Time    float64         `json:"time,omitempty"`
}

// Open opens a file-based database, creating it if it does not exist.
func Open(dbPath string) (*DB, error) {
	return open(fqdsn(dbPath, ""))
}

// OpenWithDSN opens a file-based database, creating it if it does not exist.
func OpenWithDSN(dbPath, dsn string) (*DB, error) {
	return open(fqdsn(dbPath, dsn))
}

// OpenInMemory opens an in-memory database.
func OpenInMemory() (*DB, error) {
	return open(fqdsn(":memory:", ""))
}

// OpenInMemoryWithDSN opens an in-memory database with a specific DSN.
func OpenInMemoryWithDSN(dsn string) (*DB, error) {
	return open(fqdsn(":memory:", dsn))
}

// LoadInMemoryWithDSN loads an in-memory database with that at the path,
// with the specified DSN
func LoadInMemoryWithDSN(dbPath, dsn string) (*DB, error) {
	db, err := OpenInMemoryWithDSN(dsn)
	if err != nil {
		return nil, err
	}

	srcDB, err := Open(dbPath)
	if err != nil {
		return nil, err
	}

	bk, err := db.sqlite3conn.Backup("main", srcDB.sqlite3conn, "main")
	if err != nil {
		return nil, err
	}

	for {
		done, err := bk.Step(-1)
		if err != nil {
			bk.Finish()
			return nil, err
		}
		if done {
			break
		}
		time.Sleep(bkDelay * time.Millisecond)
	}

	if err := bk.Finish(); err != nil {
		return nil, err
	}
	if err := srcDB.Close(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	return db.sqlite3conn.Close()
}

func open(dbPath string) (*DB, error) {
	d := sqlite3.SQLiteDriver{}
	dbc, err := d.Open(dbPath)
	if err != nil {
		return nil, err
	}

	return &DB{
		sqlite3conn: dbc.(*sqlite3.SQLiteConn),
		path:        dbPath,
	}, nil
}

// Execute executes queries that modify the database.
func (db *DB) Execute(queries []string, tx, xTime bool) ([]*Result, error) {
	type Execer interface {
		Exec(query string, args []driver.Value) (driver.Result, error)
	}

	var allResults []*Result
	err := func() error {
		var execer Execer
		var rollback bool
		var t driver.Tx
		var err error

		// Check for the err, if set rollback.
		defer func() {
			if t != nil {
				if rollback {
					t.Rollback()
					return
				}
				t.Commit()
			}
		}()

		// handleError sets the error field on the given result. It returns
		// whether the caller should continue processing or break.
		handleError := func(result *Result, err error) bool {
			result.Error = err.Error()
			allResults = append(allResults, result)
			if tx {
				rollback = true // Will trigger the rollback.
				return false
			}
			return true
		}

		execer = db.sqlite3conn

		// Create the correct execution object, depending on whether a
		// transaction was requested.
		if tx {
			t, err = db.sqlite3conn.Begin()
			if err != nil {
				return err
			}
		}

		// Execute each query.
		for _, q := range queries {
			if q == "" {
				continue
			}

			result := &Result{}
			start := time.Now()

			r, err := execer.Exec(q, nil)
			if err != nil {
				if handleError(result, err) {
					continue
				}
				break
			}

			lid, err := r.LastInsertId()
			if err != nil {
				if handleError(result, err) {
					continue
				}
				break
			}
			result.LastInsertID = lid

			ra, err := r.RowsAffected()
			if err != nil {
				if handleError(result, err) {
					continue
				}
				break
			}
			result.RowsAffected = ra
			if xTime {
				result.Time = time.Now().Sub(start).Seconds()
			}
			allResults = append(allResults, result)
		}

		return nil
	}()

	return allResults, err
}

// Query executes queries that return rows, but don't modify the database.
func (db *DB) Query(queries []string, tx, xTime bool) ([]*Rows, error) {
	type Queryer interface {
		Query(query string, args []driver.Value) (driver.Rows, error)
	}

	var allRows []*Rows
	err := func() (err error) {
		var queryer Queryer
		var t driver.Tx
		defer func() {
			// XXX THIS DOESN'T ACTUALLY WORK! Might as WELL JUST COMMIT?
			if t != nil {
				if err != nil {
					t.Rollback()
					return
				}
				t.Commit()
			}
		}()

		queryer = db.sqlite3conn

		// Create the correct query object, depending on whether a
		// transaction was requested.
		if tx {
			t, err = db.sqlite3conn.Begin()
			if err != nil {
				return err
			}
		}

		for _, q := range queries {
			if q == "" {
				continue
			}

			rows := &Rows{}
			start := time.Now()

			rs, err := queryer.Query(q, nil)
			if err != nil {
				rows.Error = err.Error()
				allRows = append(allRows, rows)
				continue
			}
			defer rs.Close() // This adds to all defers, right? Nothing leaks? XXX Could consume memory. Perhaps anon would be best.
			columns := rs.Columns()

			rows.Columns = columns
			rows.Types = rs.(*sqlite3.SQLiteRows).DeclTypes()
			dest := make([]driver.Value, len(rows.Columns))
			for {
				err := rs.Next(dest)
				if err != nil {
					if err != io.EOF {
						rows.Error = err.Error()
					}
					break
				}

				values := make([]interface{}, len(rows.Columns))
				// Text values come over (from sqlite-go) as []byte instead of strings
				// for some reason, so we have explicitly convert (but only when type
				// is "text" so we don't affect BLOB types)
				for i, v := range dest {
					if rows.Types[i] == "text" {
						switch val := v.(type) {
						case []byte:
							values[i] = string(val)
						default:
							values[i] = val
						}
					} else {
						values[i] = v
					}
				}
				rows.Values = append(rows.Values, values)
			}
			if xTime {
				rows.Time = time.Now().Sub(start).Seconds()
			}
			allRows = append(allRows, rows)
		}

		return nil
	}()

	return allRows, err
}

// Backup writes a consistent snapshot of the database to the given file.
func (db *DB) Backup(path string) error {
	dstDB, err := Open(path)
	if err != nil {
		return err
	}

	bk, err := dstDB.sqlite3conn.Backup("main", db.sqlite3conn, "main")
	if err != nil {
		return err
	}

	for {
		done, err := bk.Step(-1)
		if err != nil {
			bk.Finish()
			return err
		}
		if done {
			break
		}
		time.Sleep(bkDelay * time.Millisecond)
	}

	if err := bk.Finish(); err != nil {
		return err
	}
	if err := dstDB.Close(); err != nil {
		return err
	}

	return nil
}

// fqdsn returns the fully-qualified datasource name.
func fqdsn(path, dsn string) string {
	if dsn != "" {
		return fmt.Sprintf("file:%s?%s", path, dsn)
	}
	return path
}
