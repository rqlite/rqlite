// Package db exposes a lightweight abstraction over the SQLite code.
// It performs some basic mapping of lower-level types to rqlite types.
package db

import (
	"database/sql/driver"
	"expvar"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/rqlite/rqlite/command"
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

	if err := copyDatabase(db.sqlite3conn, srcDB.sqlite3conn); err != nil {
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

// Size returns the size of the database in bytes. "Size" is defined as
// page_count * schema.page_size.
func (db *DB) Size() (int64, error) {
	query := `SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()`
	r, err := db.QueryStringStmt(query)
	if err != nil {
		return 0, err
	}

	return r[0].Values[0][0].(int64), nil
}

// FileSize returns the size of the SQLite file on disk. If running in
// on-memory mode, this function returns 0.
func (db *DB) FileSize() (int64, error) {
	if db.memory {
		return 0, nil
	}
	fi, err := os.Stat(db.path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// TransactionActive returns whether a transaction is currently active
// i.e. if the database is NOT in autocommit mode.
func (db *DB) TransactionActive() bool {
	return !db.sqlite3conn.AutoCommit()
}

// AbortTransaction aborts -- rolls back -- any active transaction. Calling code
// should know exactly what it is doing if it decides to call this function. It
// can be used to clean up any dangling state that may result from certain
// error scenarios.
func (db *DB) AbortTransaction() error {
	_, err := db.ExecuteStringStmt("ROLLBACK")
	return err
}

// ExecuteStringStmt executes a single query that modifies the database. This is
// primarily a convenience function.
func (db *DB) ExecuteStringStmt(query string) ([]*Result, error) {
	r := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: query,
			},
		},
	}
	return db.Execute(r, false)
}

// Execute executes queries that modify the database.
func (db *DB) Execute(req *command.Request, xTime bool) ([]*Result, error) {
	stats.Add(numExecutions, int64(len(req.Statements)))

	tx := req.Transaction
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

		// Execute each statement.
		for _, stmt := range req.Statements {
			sql := stmt.Sql
			if sql == "" {
				continue
			}

			result := &Result{}
			start := time.Now()

			parameters, err := parametersToValues(stmt.Parameters)
			if err != nil {
				if handleError(result, err) {
					continue
				}
				break
			}

			r, err := execer.Exec(sql, parameters)
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

// QueryStringStmt executes a single query that return rows, but don't modify database.
func (db *DB) QueryStringStmt(query string) ([]*Rows, error) {
	r := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: query,
			},
		},
	}
	return db.Query(r, false)
}

// Query executes queries that return rows, but don't modify the database.
func (db *DB) Query(req *command.Request, xTime bool) ([]*Rows, error) {
	stats.Add(numQueries, int64(len(req.Statements)))

	tx := req.Transaction
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

		for _, stmt := range req.Statements {
			sql := stmt.Sql
			if sql == "" {
				continue
			}

			rows := &Rows{}
			start := time.Now()

			parameters, err := parametersToValues(stmt.Parameters)
			if err != nil {
				rows.Error = err.Error()
				allRows = append(allRows, rows)
				continue
			}

			rs, err := queryer.Query(sql, parameters)
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

	defer func(db *DB, err *error) {
		cerr := db.Close()
		if *err == nil {
			*err = cerr
		}
	}(dstDB, &err)

	if err := copyDatabase(dstDB.sqlite3conn, db.sqlite3conn); err != nil {
		return err
	}

	return err
}

// Dump writes a consistent snapshot of the database in SQL text format.
func (db *DB) Dump(w io.Writer) error {
	if _, err := w.Write([]byte("PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n")); err != nil {
		return err
	}

	// Get a new connection, so the dump creation is isolated from other activity.
	dstDB, err := OpenInMemory()
	if err != nil {
		return err
	}
	defer func(db *DB, err *error) {
		cerr := db.Close()
		if *err == nil {
			*err = cerr
		}
	}(dstDB, &err)

	if err := copyDatabase(dstDB.sqlite3conn, db.sqlite3conn); err != nil {
		return err
	}

	// Get the schema.
	query := `SELECT "name", "type", "sql" FROM "sqlite_master"
              WHERE "sql" NOT NULL AND "type" == 'table' ORDER BY "name"`
	rows, err := dstDB.QueryStringStmt(query)
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
		r, err := dstDB.QueryStringStmt(fmt.Sprintf(`PRAGMA table_info("%s")`, tableIndent))
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
		r, err = dstDB.QueryStringStmt(query)
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
	rows, err = db.QueryStringStmt(query)
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

	if err := bk.Finish(); err != nil {
		return err
	}

	return nil
}

// parametersToValues maps values in the proto params to SQL driver values.
func parametersToValues(parameters []*command.Parameter) ([]driver.Value, error) {
	if parameters == nil {
		return nil, nil
	}

	values := make([]driver.Value, len(parameters))
	for i := range parameters {
		switch w := parameters[i].GetValue().(type) {
		case *command.Parameter_I:
			values[i] = w.I
		case *command.Parameter_D:
			values[i] = w.D
		case *command.Parameter_B:
			values[i] = w.B
		case *command.Parameter_Y:
			values[i] = w.Y
		case *command.Parameter_S:
			values[i] = w.S
		default:
			return nil, fmt.Errorf("unsupported type: %T", w)
		}
	}
	return values, nil
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
		t == "json" ||
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
