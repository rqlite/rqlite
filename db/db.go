// Package db exposes a lightweight abstraction over the SQLite code.
package db

import (
	"database/sql/driver"
	"fmt"
	"io"
	"time"

	"github.com/mattn/go-sqlite3"
)

const bkDelay = 250

var DBVersion string

func init() {
	DBVersion, _, _ = sqlite3.Version()
}

// Config represents the configuration of the SQLite database.
type Config struct {
	DSN    string // Datasource name
	Memory bool   // In-memory database enabled?
}

// NewConfig returns an instance of Config for the database at path.
func NewConfig() *Config {
	return &Config{}
}

// FQDSN returns the fully-qualified datasource name.
func (c *Config) FQDSN(path string) string {
	if c.DSN != "" {
		return fmt.Sprintf("file:%s?%s", path, c.DSN)
	}
	return path
}

// DB is the SQL database.
type DB struct {
	sqlite3conn *sqlite3.SQLiteConn // Driver connection to database.
	path        string              // Path to database file.
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

// Open an existing database, creating it if it does not exist.
func Open(dbPath string) (*DB, error) {
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

// OpenWithConfiguration an existing database, creating it if it does not exist.
func OpenWithConfiguration(dbPath string, conf *Config) (*DB, error) {
	return Open(conf.FQDSN(dbPath))
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	return db.sqlite3conn.Close()
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

				// Special case -- convert []uint8 to string. Perhaps this should be a config option.
				for i, v := range dest {
					switch u := v.(type) {
					case []uint8:
						values[i] = string(u)
					default:
						values[i] = u
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
