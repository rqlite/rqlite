// Package db exposes a lightweight abstraction over the SQLite code.
// It performs some basic mapping of lower-level types to rqlite types.
package db

import (
	"context"
	"database/sql"
	"expvar"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/rqlite/go-sqlite3"
	"github.com/rqlite/rqlite/command"
)

const bkDelay = 250

const (
	onDiskMaxOpenConns = 32
	onDiskMaxIdleTime  = 120 * time.Second

	fkChecks         = "PRAGMA foreign_keys"
	fkChecksEnabled  = "PRAGMA foreign_keys=ON"
	fkChecksDisabled = "PRAGMA foreign_keys=OFF"
	journalCheck     = "PRAGMA journal_mode"

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
	db     *sql.DB // Std library database connection
	path   string  // Path to database file.
	memory bool    // In-memory only.
}

// PoolStats represents connection pool statistics
type PoolStats struct {
	MaxOpenConnections int           `json:"max_open_connections"`
	OpenConnections    int           `json:"open_connections"`
	InUse              int           `json:"in_use"`
	Idle               int           `json:"idle"`
	WaitCount          int64         `json:"wait_count"`
	WaitDuration       time.Duration `json:"wait_duration"`
	MaxIdleClosed      int64         `json:"max_idle_closed"`
	MaxIdleTimeClosed  int64         `json:"max_idle_time_closed"`
	MaxLifetimeClosed  int64         `json:"max_lifetime_closed"`
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
	db, err := open(dbPath)
	if err != nil {
		return nil, err
	}
	db.db.SetConnMaxIdleTime(onDiskMaxIdleTime)
	db.db.SetConnMaxLifetime(0)
	db.db.SetMaxIdleConns(onDiskMaxOpenConns)
	db.db.SetMaxOpenConns(onDiskMaxOpenConns)
	return db, nil
}

// OpenInMemory returns a new in-memory database.
func OpenInMemory() (*DB, error) {
	db, err := open(randomInMemoryDB())
	if err != nil {
		return nil, err
	}
	// In-memory databases do not support practical connection pooling.
	db.db.SetConnMaxIdleTime(0)
	db.db.SetConnMaxLifetime(0)
	db.db.SetMaxIdleConns(1)
	db.db.SetMaxOpenConns(1)
	return db, nil
}

// LoadInMemory loads an in-memory database with that at the path.
// Not safe to call while other operations are happening with the
// source database.
func LoadInMemory(dbPath string) (*DB, error) {
	dstDB, err := OpenInMemory()
	if err != nil {
		return nil, err
	}

	srcDB, err := Open(dbPath)
	if err != nil {
		return nil, err
	}

	if err := copyDatabase(dstDB, srcDB); err != nil {
		return nil, err
	}

	if err := srcDB.Close(); err != nil {
		return nil, err
	}

	return dstDB, nil
}

// DeserializeInMemory loads an in-memory database with that contained
// in the byte slide. The byte slice must not be changed or garbage-collected
// until after this function returns.
func DeserializeInMemory(b []byte) (retDB *DB, retErr error) {
	tmpDB, err := OpenInMemory()
	if err != nil {
		return nil, fmt.Errorf("DeserializeInMemory: %s", err.Error())
	}
	defer tmpDB.Close()

	tmpConn, err := tmpDB.db.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	if err := tmpConn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*sqlite3.SQLiteConn)
		err2 := c.Deserialize(b, "")
		if err2 != nil {
			return fmt.Errorf("DeserializeInMemory: %s", err.Error())
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Testing shows closing the temp conn is necessary.
	if err := tmpConn.Close(); err != nil {
		return nil, fmt.Errorf("DeserializeInMemory: %s", err.Error())
	}

	// tmpDB is still using memory in Go space, so tmpDB needs to be explicitly
	// copied to a new database.
	db, err := OpenInMemory()
	if err != nil {
		return nil, fmt.Errorf("DeserializeInMemory: %s", err.Error())
	}
	defer func() {
		// Don't leak a database if deserialization fails.
		if retErr != nil {
			db.Close()
		}
	}()

	if err := copyDatabase(db, tmpDB); err != nil {
		return nil, fmt.Errorf("DeserializeInMemory: %s", err.Error())
	}

	return db, nil
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	return db.db.Close()
}

func open(dbPath string) (*DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Ensure database is basically healthy.
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("Ping database: %s", err.Error())
	}

	return &DB{
		db:   db,
		path: dbPath,
	}, nil
}

// EnableFKConstraints allows control of foreign key constraint checks.
func (db *DB) EnableFKConstraints(e bool) error {
	q := fkChecksEnabled
	if !e {
		q = fkChecksDisabled
	}

	_, err := db.ExecuteStringStmt(q)
	return err
}

// FKConstraints returns whether FK constraints are set or not.
func (db *DB) FKConstraints() (bool, error) {
	r, err := db.QueryStringStmt(fkChecks)
	if err != nil {
		return false, err
	}
	if r[0].Values[0][0] == int64(1) {
		return true, nil
	}
	return false, nil
}

// JournalMode returns the current journal mode.
func (db *DB) JournalMode() (string, error) {
	r, err := db.QueryStringStmt(journalCheck)
	if err != nil {
		return "", err
	}
	return r[0].Values[0][0].(string), nil
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

// ConnectionPoolStats returns database pool statistics
func (db *DB) ConnectionPoolStats() *PoolStats {
	s := db.db.Stats()
	return &PoolStats{
		MaxOpenConnections: s.MaxOpenConnections,
		OpenConnections:    s.OpenConnections,
		InUse:              s.InUse,
		Idle:               s.Idle,
		WaitCount:          s.WaitCount,
		WaitDuration:       s.WaitDuration,
		MaxIdleClosed:      s.MaxIdleClosed,
		MaxIdleTimeClosed:  s.MaxIdleTimeClosed,
		MaxLifetimeClosed:  s.MaxLifetimeClosed,
	}

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

	conn, err := db.db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	type Execer interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	}

	var execer Execer
	var tx *sql.Tx
	if req.Transaction {
		stats.Add(numETx, 1)
		tx, err = conn.BeginTx(context.Background(), nil)
		if err != nil {
			return nil, err
		}
		defer func() {
			if tx != nil {
				tx.Rollback() // Will be ignored if tx is committed
			}
		}()
		execer = tx
	} else {
		execer = conn
	}

	var allResults []*Result

	// handleError sets the error field on the given result. It returns
	// whether the caller should continue processing or break.
	handleError := func(result *Result, err error) bool {
		stats.Add(numExecutionErrors, 1)
		result.Error = err.Error()
		allResults = append(allResults, result)
		if tx != nil {
			tx.Rollback()
			tx = nil
			return false
		}
		return true
	}

	// Execute each statement.
	for _, stmt := range req.Statements {
		ss := stmt.Sql
		if ss == "" {
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

		r, err := execer.ExecContext(context.Background(), ss, parameters...)
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

	if tx != nil {
		err = tx.Commit()
	}
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
	conn, err := db.db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return db.queryWithConn(req, xTime, conn)
}

func (db *DB) queryWithConn(req *command.Request, xTime bool, conn *sql.Conn) ([]*Rows, error) {
	var err error
	type Queryer interface {
		QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	}

	var queryer Queryer
	var tx *sql.Tx
	if req.Transaction {
		stats.Add(numQTx, 1)
		tx, err = conn.BeginTx(context.Background(), nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback() // Will be ignored if tx is committed
		queryer = tx
	} else {
		queryer = conn
	}

	var allRows []*Rows
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

		rs, err := queryer.QueryContext(context.Background(), sql, parameters...)
		if err != nil {
			rows.Error = err.Error()
			allRows = append(allRows, rows)
			continue
		}
		defer rs.Close()

		rows.Columns, err = rs.Columns()
		if err != nil {
			return nil, err
		}

		types, err := rs.ColumnTypes()
		if err != nil {
			return nil, err
		}
		rows.Types = make([]string, len(types))
		for i := range types {
			rows.Types[i] = strings.ToLower(types[i].DatabaseTypeName())
		}

		for rs.Next() {
			dest := make([]interface{}, len(rows.Columns))
			ptrs := make([]interface{}, len(dest))
			for i := range ptrs {
				ptrs[i] = &dest[i]
			}
			if err := rs.Scan(ptrs...); err != nil {
				return nil, err
			}
			values := normalizeRowValues(dest, rows.Types)
			rows.Values = append(rows.Values, values)
		}

		// Check for errors from iterating over rows.
		if err := rs.Err(); err != nil {
			return nil, err
		}

		if xTime {
			rows.Time = time.Now().Sub(start).Seconds()
		}
		allRows = append(allRows, rows)
	}

	if tx != nil {
		err = tx.Commit()
	}
	return allRows, err
}

// Backup writes a consistent snapshot of the database to the given file.
// This function can be called when changes to the database are in flight.
func (db *DB) Backup(path string) error {
	dstDB, err := Open(path)
	if err != nil {
		return err
	}

	if err := copyDatabase(dstDB, db); err != nil {
		return fmt.Errorf("backup database: %s", err)
	}
	return nil
}

// Copy copies the contents of the database to the given database. All other
// attributes of the given database remain untouched e.g. whether it's an
// on-disk database. This function can be called when changes to the source
// database are in flight.
func (db *DB) Copy(dstDB *DB) error {
	if err := copyDatabase(dstDB, db); err != nil {
		return fmt.Errorf("copy database: %s", err)
	}
	return nil
}

// Serialize returns a byte slice representation of the SQLite database. For
// an ordinary on-disk database file, the serialization is just a copy of the
// disk file. For an in-memory database or a "TEMP" database, the serialization
// is the same sequence of bytes which would be written to disk if that database
// were backed up to disk.
func (db *DB) Serialize() ([]byte, error) {
	conn, err := db.db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var b []byte
	f := func(driverConn interface{}) error {
		c := driverConn.(*sqlite3.SQLiteConn)
		b = c.Serialize("")
		if b == nil {
			return fmt.Errorf("failed to serialize database")
		}
		return nil
	}

	if err := conn.Raw(f); err != nil {
		return nil, err
	}
	return b, nil
}

// Dump writes a consistent snapshot of the database in SQL text format.
// This function can be called when changes to the database are in flight.
func (db *DB) Dump(w io.Writer) error {
	conn, err := db.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	// Convenience function to convert string query to protobuf.
	commReq := func(query string) *command.Request {
		return &command.Request{
			Statements: []*command.Statement{
				{
					Sql: query,
				},
			},
		}
	}

	if _, err := w.Write([]byte("PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n")); err != nil {
		return err
	}

	// Get the schema.
	query := `SELECT "name", "type", "sql" FROM "sqlite_master"
              WHERE "sql" NOT NULL AND "type" == 'table' ORDER BY "name"`
	rows, err := db.queryWithConn(commReq(query), false, conn)
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
		r, err := db.queryWithConn(commReq(fmt.Sprintf(`PRAGMA table_info("%s")`, tableIndent)),
			false, conn)
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
		r, err = db.queryWithConn(commReq(query), false, conn)

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
	rows, err = db.queryWithConn(commReq(query), false, conn)
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

func copyDatabase(dst *DB, src *DB) error {
	dstConn, err := dst.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer dstConn.Close()
	srcConn, err := src.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer srcConn.Close()

	var dstSQLiteConn *sqlite3.SQLiteConn

	// Define the backup function.
	bf := func(driverConn interface{}) error {
		srcSQLiteConn := driverConn.(*sqlite3.SQLiteConn)

		bk, err := dstSQLiteConn.Backup("main", srcSQLiteConn, "main")
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

	return dstConn.Raw(
		func(driverConn interface{}) error {
			dstSQLiteConn = driverConn.(*sqlite3.SQLiteConn)
			return srcConn.Raw(bf)
		})
}

// parametersToValues maps values in the proto params to SQL driver values.
func parametersToValues(parameters []*command.Parameter) ([]interface{}, error) {
	if parameters == nil {
		return nil, nil
	}

	values := make([]interface{}, len(parameters))
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
func normalizeRowValues(row []interface{}, types []string) []interface{} {
	for i, v := range row {
		if isTextType(types[i]) {
			val, ok := v.([]byte)
			if ok {
				row[i] = string(val)
			}
		}
	}
	return row
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

func randomInMemoryDB() string {
	var output strings.Builder
	chars := "abcdedfghijklmnopqrstABCDEFGHIJKLMNOP"
	for i := 0; i < 20; i++ {
		random := rand.Intn(len(chars))
		randomChar := chars[random]
		output.WriteString(string(randomChar))
	}
	return fmt.Sprintf("file:/%s?vfs=memdb", output.String())
}
