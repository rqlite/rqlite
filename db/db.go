package db

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

// Hook into the SQLite3 connection.
//
// This connection, mutex, and init() allows each Open() call to get access
// to the driver's internal conn object. The mutex ensures only 1 Open() call
// can run concurrently, and when it has returned, sqlite3conn will be set to
// the connection. This connection can be used for backups.
var sqlite3conn *sqlite3.SQLiteConn
var openMu sync.Mutex

const bkDelay = 250

func init() {
	sql.Register("sqlite3_with_hook",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				sqlite3conn = conn
				return nil
			},
		})
}

// Config represents the configuration of the SQLite database.
type Config struct {
	DSN    string
	Memory bool
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
	conn        *sql.DB
	sqlite3conn *sqlite3.SQLiteConn
	path        string
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
	Values  [][]interface{} `json:"values,omitempty"`
	Error   string          `json:"error,omitempty"`
	Time    float64         `json:"time,omitempty"`
}

// Open an existing database, creating it if it does not exist.
func Open(dbPath string) (*DB, error) {
	openMu.Lock()
	defer openMu.Unlock()

	dbc, err := sql.Open("sqlite3_with_hook", dbPath)
	if err != nil {
		return nil, err
	}

	// Ensure a connection is established before going further. Ignore error
	// because database may not exist yet.
	_ = dbc.Ping()

	return &DB{
		conn:        dbc,
		sqlite3conn: sqlite3conn,
		path:        dbPath,
	}, nil
}

// OpenWithConfiguration an existing database, creating it if it does not exist.
func OpenWithConfiguration(dbPath string, conf *Config) (*DB, error) {
	openMu.Lock()
	defer openMu.Unlock()

	dbc, err := sql.Open("sqlite3_with_hook", conf.FQDSN(dbPath))
	if err != nil {
		return nil, err
	}

	// Ensure a connection is established before going further. Ignore error
	// because database may not exist yet.
	_ = dbc.Ping()

	return &DB{
		conn:        dbc,
		sqlite3conn: sqlite3conn,
		path:        conf.FQDSN(dbPath),
	}, nil
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

// Execute executes queries that modify the database.
func (db *DB) Execute(queries []string, tx, xTime bool) ([]*Result, error) {
	type Execer interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
	}

	var allResults []*Result
	err := func() error {
		var execer Execer
		var rollback bool

		// Check for the err, if set rollback.
		defer func() {
			if t, ok := execer.(*sql.Tx); ok {
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

		// Create the correct execution object, depending on whether a
		// transaction was requested.
		if tx {
			execer, _ = db.conn.Begin()
		} else {
			execer = db.conn
		}

		// Execute each query.
		for _, q := range queries {
			if q == "" {
				continue
			}

			result := &Result{}
			start := time.Now()

			r, err := execer.Exec(q)
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
		Query(query string, args ...interface{}) (*sql.Rows, error)
	}

	var allRows []*Rows
	err := func() (err error) {
		var queryer Queryer
		defer func() {
			// XXX THIS DOESN'T ACTUALLY WORK! Might as WELL JUST COMMIT?
			if t, ok := queryer.(*sql.Tx); ok {
				if err != nil {
					t.Rollback()
					return
				}
				t.Commit()
			}
		}()

		// Create the correct query object, depending on whether a
		// transaction was requested.
		if tx {
			queryer, _ = db.conn.Begin()
		} else {
			queryer = db.conn
		}

	QueryLoop:
		for _, q := range queries {
			if q == "" {
				continue
			}

			rows := &Rows{}
			start := time.Now()

			rs, err := queryer.Query(q)
			if err != nil {
				rows.Error = err.Error()
				allRows = append(allRows, rows)
				continue
			}
			defer rs.Close() // This adds to all defers, right? Nothing leaks? XXX Could consume memory. Perhaps anon would be best.
			columns, err := rs.Columns()
			if err != nil {
				rows.Error = err.Error()
				allRows = append(allRows, rows)
				continue
			}

			rows.Columns = columns
			for rs.Next() {
				// Make a slice of interface{}, and the pointers to each item in the slice.
				values := make([]interface{}, len(rows.Columns))
				ptrs := make([]interface{}, len(rows.Columns))
				for i := range values {
					ptrs[i] = &values[i]
				}

				// Read all the values out.
				err = rs.Scan(ptrs...)
				if err != nil {
					rows.Error = err.Error()
					allRows = append(allRows, rows)
					continue QueryLoop
				}

				// Special case -- convert []uint8 to string. Perhaps this should be a config option.
				for i, v := range values {
					if w, ok := v.([]uint8); ok {
						values[i] = string(w)
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

// Backup returns a consistent snapshot of the database.
func (db *DB) Backup() ([]byte, error) {
	f, err := ioutil.TempFile("", "rqlilte-bak-")
	if err != nil {
		return nil, err
	}
	f.Close()
	defer os.Remove(f.Name())

	dstDB, err := Open(f.Name())
	if err != nil {
		return nil, err
	}

	bk, err := dstDB.sqlite3conn.Backup("main", db.sqlite3conn, "main")
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
	if err := dstDB.Close(); err != nil {
		return nil, err
	}

	b, err := ioutil.ReadFile(f.Name())
	if err != nil {
		return nil, err
	}
	return b, nil
}
