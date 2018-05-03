// Package db exposes a lightweight abstraction over the SQLite code.
// It performs some basic mapping of lower-level types to rqlite types.
package db

import (
	"database/sql/driver"
	"expvar"
	"fmt"
	"io"
	"net/url"
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

// DB is the SQL database.
type DB struct {
	path     string // Path to database file.
	dsnQuery string // DSN query params, if any.
	memory   bool   // In-memory only.
	fqdsn    string // Fully-qualified DSN for opening SQLite.
}

// New returns an instance of the database at path. If the database
// has already been created and opened, this database will share
// the data of that database when connected.
func New(path, dsnQuery string, memory bool) (*DB, error) {
	q, err := url.ParseQuery(dsnQuery)
	if err != nil {
		return nil, err
	}
	if memory {
		q.Set("mode", "memory")
		q.Set("cache", "shared")
	}

	if !strings.HasPrefix(path, "file:") {
		path = fmt.Sprintf("file:%s", path)
	}

	var fqdsn string
	if len(q) > 0 {
		fqdsn = fmt.Sprintf("%s?%s", path, q.Encode())
	} else {
		fqdsn = path
	}

	return &DB{
		path:     path,
		dsnQuery: dsnQuery,
		memory:   memory,
		fqdsn:    fqdsn,
	}, nil
}

// Connect returns a connection to the database.
func (d *DB) Connect() (*Conn, error) {
	drv := sqlite3.SQLiteDriver{}
	c, err := drv.Open(d.fqdsn)
	if err != nil {
		return nil, err
	}

	return &Conn{
		sqlite: c.(*sqlite3.SQLiteConn),
	}, nil
}

// Conn represents a connection to a database. Two Connection objects
// to the same database are READ_COMMITTED isolated.
type Conn struct {
	sqlite *sqlite3.SQLiteConn
}

// TransactionActive returns whether a transaction is currently active
// i.e. if the database is NOT in autocommit mode.
func (c *Conn) TransactionActive() bool {
	return !c.sqlite.AutoCommit()
}

// AbortTransaction aborts -- rolls back -- any active transaction. Calling code
// should know exactly what it is doing if it decides to call this function. It
// can be used to clean up any dangling state that may result from certain
// error scenarios.
func (c *Conn) AbortTransaction() error {
	_, err := c.Execute([]string{`ROLLBACK`}, false, false)
	return err
}

// Execute executes queries that modify the database.
func (c *Conn) Execute(queries []string, tx, xTime bool) ([]*Result, error) {
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

		execer = c.sqlite

		// Create the correct execution object, depending on whether a
		// transaction was requested.
		if tx {
			t, err = c.sqlite.Begin()
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
func (c *Conn) Query(queries []string, tx, xTime bool) ([]*Rows, error) {
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

		queryer = c.sqlite

		// Create the correct query object, depending on whether a
		// transaction was requested.
		if tx {
			t, err = c.sqlite.Begin()
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

// EnableFKConstraints allows control of foreign key constraint checks.
func (c *Conn) EnableFKConstraints(e bool) error {
	q := fkChecksEnabled
	if !e {
		q = fkChecksDisabled
	}
	_, err := c.sqlite.Exec(q, nil)
	return err
}

// FKConstraints returns whether FK constraints are set or not.
func (c *Conn) FKConstraints() (bool, error) {
	r, err := c.sqlite.Query(fkChecks, nil)
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

// Load loads the connected database from the database connected to src.
// It overwrites the data contained in this database. It is the caller's
// responsibility to ensure that no other connections to this database
// are accessed while this operation is in progress.
func (c *Conn) Load(src *Conn) error {
	return copyDatabase(c.sqlite, src.sqlite)
}

// Backup writes a snapshot of the database over the given database
// connection, erasing all the contents of the destination database.
// The consistency of the snapshot is READ_COMMITTED relative to any
// other connections currently open to this database. The caller must
// ensure that all connections to the destination database are not
// accessed during this operation.
func (c *Conn) Backup(dst *Conn) error {
	return copyDatabase(dst.sqlite, c.sqlite)
}

// Dump writes a snapshot of the database in SQL text format. The consistency
// of the snapshot is READ_COMMITTED relative to any other connections
// currently open to this database.
func (c *Conn) Dump(w io.Writer) error {
	if _, err := w.Write([]byte("PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n")); err != nil {
		return err
	}

	// Get the schema.
	query := `SELECT "name", "type", "sql" FROM "sqlite_master"
              WHERE "sql" NOT NULL AND "type" == 'table' ORDER BY "name"`
	rows, err := c.Query([]string{query}, false, false)
	if err != nil {
		return err
	}
	row := rows[0]
	for _, v := range row.Values {
		table := v[0].(string)
		var stmt string

		if table == "sqlite_sequence" {
			stmt = `DELETE FROM "sqlite_sequence";`
		} else if table == "sqlite_stat1" {
			stmt = `ANALYZE "sqlite_master";`
		} else if strings.HasPrefix(table, "sqlite_") {
			continue
		} else {
			stmt = v[2].(string)
		}

		if _, err := w.Write([]byte(fmt.Sprintf("%s;\n", stmt))); err != nil {
			return err
		}

		tableIndent := strings.Replace(table, `"`, `""`, -1)
		query = fmt.Sprintf(`PRAGMA table_info("%s")`, tableIndent)
		r, err := c.Query([]string{query}, false, false)
		if err != nil {
			return err
		}
		var columnNames []string
		for _, w := range r[0].Values {
			columnNames = append(columnNames, fmt.Sprintf(`'||quote("%s")||'`, w[1].(string)))
		}

		query = fmt.Sprintf(`SELECT 'INSERT INTO "%s" VALUES(%s)' FROM "%s";`,
			tableIndent,
			strings.Join(columnNames, ","),
			tableIndent)
		r, err = c.Query([]string{query}, false, false)
		if err != nil {
			return err
		}
		for _, x := range r[0].Values {
			y := fmt.Sprintf("%s;\n", x[0].(string))
			if _, err := w.Write([]byte(y)); err != nil {
				return err
			}
		}
	}

	// Do indexes, triggers, and views.
	query = `SELECT "name", "type", "sql" FROM "sqlite_master"
			  WHERE "sql" NOT NULL AND "type" IN ('index', 'trigger', 'view')`
	rows, err = c.Query([]string{query}, false, false)
	if err != nil {
		return err
	}
	row = rows[0]
	for _, v := range row.Values {
		if _, err := w.Write([]byte(fmt.Sprintf("%s;\n", v[2]))); err != nil {
			return err
		}
	}

	if _, err := w.Write([]byte("COMMIT;\n")); err != nil {
		return err
	}

	return nil
}

// Close closes the connection.
func (c *Conn) Close() error {
	if c != nil {
		return c.sqlite.Close()
	}
	return nil
}

func copyDatabase(dst *sqlite3.SQLiteConn, src *sqlite3.SQLiteConn) error {
	bk, err := dst.Backup("main", src, "main")
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

	return bk.Finish()
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
