package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3" // required blank import
)

// Config represents the configuration of the SQLite database.
type Config struct {
	DSN string
}

// NewConfig returns an instance of Config for the database at path.
func NewConfig() *Config {
	return &Config{}
}

// DSN returns the fully-qualified datasource name.
func (c *Config) FQDSN(path string) string {
	if c.DSN != "" {
		return fmt.Sprintf("file:%s?%s", path, c.DSN)
	}
	return path
}

// DB is the SQL database.
type DB struct {
	conn *sql.DB
}

type Result struct {
	LastInsertID int64  `json:"last_insert_id,omitempty"`
	RowsAffected int64  `json:"rows_affected,omitempty"`
	Error        string `json:"error,omitempty"`
	Time         string `json:"time,omitempty"`
}

type Rows struct {
	Columns []string        `json:"columns,omitempty"`
	Values  [][]interface{} `json:"values,omitempty"`
	Error   string          `json:"error,omitempty"`
	Time    string          `json:"time,omitempty"`
}

// Open an existing database, creating it if it does not exist.
func Open(dbPath string) (*DB, error) {
	dbc, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	return &DB{
		conn: dbc,
	}, nil
}

// OpenWithConfiguration an existing database, creating it if it does not exist.
func OpenWithConfiguration(dbPath string, conf *Config) (*DB, error) {
	dbc, err := sql.Open("sqlite3", conf.FQDSN(dbPath))
	if err != nil {
		return nil, err
	}
	return &DB{
		conn: dbc,
	}, nil
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

// Execute executes queries that modify the database.
func (db *DB) Execute(queries []string, tx bool) ([]*Result, error) {
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
			result.Time = time.Now().Sub(start).String()
			allResults = append(allResults, result)
		}

		return nil
	}()

	return allResults, err
}

// Query executes queries that return rows, but don't modify the database.
func (db *DB) Query(queries []string, tx bool) ([]*Rows, error) {
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
			rows.Time = time.Now().Sub(start).String()
			allRows = append(allRows, rows)
		}

		return nil
	}()

	return allRows, err
}
