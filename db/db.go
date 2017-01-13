// Package db exposes a lightweight abstraction over the SQLite code.
// It performs some basic mapping of lower-level types to rqlite types.
package db

import (
	"database/sql/driver"
	"expvar"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
)

const bkDelay = 250

const (
	fkChecks         = "PRAGMA foreign_keys"
	fkChecksEnabled  = "PRAGMA foreign_keys=ON"
	fkChecksDisabled = "PRAGMA foreign_keys=OFF"

	numExecutions      = "executions"
	numExecutionErrors = "execution_errors"
	numQueries         = "queries"
	numETx             = "execute_transactions"
	numQTx             = "query_transactions"
)

// DBVersion is the SQLite version.
var DBVersion string

// stats captures stats for the DB layer.
var stats *expvar.Map

func init() {
	DBVersion, _, _ = sqlite3.Version()
	stats = expvar.NewMap("db")
	stats.Add(numExecutions, 0)
	stats.Add(numExecutionErrors, 0)
	stats.Add(numQueries, 0)
	stats.Add(numETx, 0)
	stats.Add(numQTx, 0)

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

// EnableFKConstraints allows control of foreign key constraint checks.
func (db *DB) EnableFKConstraints(e bool) error {
	q := fkChecksEnabled
	if !e {
		q = fkChecksDisabled
	}
	_, err := db.sqlite3conn.Exec(q, nil)
	return err
}

// FKConstraints returns whether FK constraints are set or not.
func (db *DB) FKConstraints() (bool, error) {
	r, err := db.sqlite3conn.Query(fkChecks, nil)
	if err != nil {
		return false, err
	}

	dest := make([]driver.Value, len(r.Columns()))
	types := r.(*sqlite3.SQLiteRows).DeclTypes()
	if err := r.Next(dest); err != nil {
		return false, err
	}

	values := normalizeRowValues(dest, types)
	if values[0] == int64(1) {
		return true, nil
	}
	return false, nil
}

// Execute executes queries that modify the database.
func (db *DB) Execute(queries []string, tx, xTime bool) ([]*Result, error) {
	stats.Add(numExecutions, int64(len(queries)))
	if tx {
		stats.Add(numETx, 1)
	}

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
			stats.Add(numExecutionErrors, 1)

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
			if r == nil {
				continue
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
	stats.Add(numQueries, int64(len(queries)))
	if tx {
		stats.Add(numQTx, 1)
	}

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
			defer rs.Close()
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

				values := normalizeRowValues(dest, rows.Types)
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

// normalizeRowValues performs some normalization of values in the returned rows.
// Text values come over (from sqlite-go) as []byte instead of strings
// for some reason, so we have explicitly convert (but only when type
// is "text" so we don't affect BLOB types)
func normalizeRowValues(row []driver.Value, types []string) []interface{} {
	values := make([]interface{}, len(types))
	for i, v := range row {
		if isTextType(types[i]) {
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
	return values
}

// isTextType returns whether the given type has a SQLite text affinity.
// http://www.sqlite.org/datatype3.html
func isTextType(t string) bool {
	return t == "text" ||
		t == "" ||
		strings.HasPrefix(t, "varchar") ||
		strings.HasPrefix(t, "varying character") ||
		strings.HasPrefix(t, "nchar") ||
		strings.HasPrefix(t, "native character") ||
		strings.HasPrefix(t, "nvarchar") ||
		strings.HasPrefix(t, "clob")
}

// fqdsn returns the fully-qualified datasource name.
func fqdsn(path, dsn string) string {
	if dsn != "" {
		return fmt.Sprintf("file:%s?%s", path, dsn)
	}
	return path
}
