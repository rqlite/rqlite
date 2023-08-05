// Package db exposes a lightweight abstraction over the SQLite code.
// It performs some basic mapping of lower-level types to rqlite types.
package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"expvar"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rqlite/go-sqlite3"
	"github.com/rqlite/rqlite/command"
)

const (
	SQLiteHeaderSize = 32

	bkDelay                  = 250
	defaultCheckpointTimeout = 30 * time.Second
)

const (
	numCheckpoints      = "checkpoints"
	numCheckpointErrors = "checkpoint_errors"
	checkpointDuration  = "checkpoint_duration_ns"
	numExecutions       = "executions"
	numExecutionErrors  = "execution_errors"
	numQueries          = "queries"
	numQueryErrors      = "query_errors"
	numRequests         = "requests"
	numETx              = "execute_transactions"
	numQTx              = "query_transactions"
	numRTx              = "request_transactions"
)

var (
	ErrWALReplayDirectoryMismatch = errors.New("WAL file(s) not in same directory as database file")
)

// DBVersion is the SQLite version.
var DBVersion string

// stats captures stats for the DB layer.
var stats *expvar.Map

func init() {
	rand.Seed(time.Now().UnixNano())
	DBVersion, _, _ = sqlite3.Version()
	stats = expvar.NewMap("db")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numCheckpoints, 0)
	stats.Add(numCheckpointErrors, 0)
	stats.Add(checkpointDuration, 0)
	stats.Add(numExecutions, 0)
	stats.Add(numExecutionErrors, 0)
	stats.Add(numQueries, 0)
	stats.Add(numQueryErrors, 0)
	stats.Add(numRequests, 0)
	stats.Add(numETx, 0)
	stats.Add(numQTx, 0)
	stats.Add(numRTx, 0)
}

// DB is the SQL database.
type DB struct {
	path      string // Path to database file, if running on-disk.
	walPath   string // Path to WAL file, if running on-disk and WAL is enabled.
	memory    bool   // In-memory only.
	fkEnabled bool   // Foreign key constraints enabled
	wal       bool

	rwDB *sql.DB // Database connection for database reads and writes.
	roDB *sql.DB // Database connection database reads.

	rwDSN string // DSN used for read-write connection
	roDSN string // DSN used for read-only connections
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

// IsValidSQLiteFile checks that the supplied path looks like a SQLite file.
func IsValidSQLiteFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	b := make([]byte, 16)
	if _, err := f.Read(b); err != nil {
		return false
	}

	return IsValidSQLiteData(b)
}

// IsValidSQLiteData checks that the supplied data looks like a SQLite data.
// See https://www.sqlite.org/fileformat.html
func IsValidSQLiteData(b []byte) bool {
	return len(b) > 13 && string(b[0:13]) == "SQLite format"
}

// IsValidSQLiteWALFile checks that the supplied path looks like a SQLite
// WAL file. See https://www.sqlite.org/fileformat2.html#walformat
func IsValidSQLiteWALFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	b := make([]byte, 4)
	if _, err := f.Read(b); err != nil {
		return false
	}

	return IsValidSQLiteWALData(b)
}

// IsValidSQLiteWALData checks that the supplied data looks like a SQLite
// WAL file.
func IsValidSQLiteWALData(b []byte) bool {
	if len(b) < 4 {
		return false
	}

	header1 := []byte{0x37, 0x7f, 0x06, 0x82}
	header2 := []byte{0x37, 0x7f, 0x06, 0x83}
	header := b[:4]
	return bytes.Equal(header, header1) || bytes.Equal(header, header2)
}

// IsWALModeEnabledSQLiteFile checks that the supplied path looks like a SQLite
// with WAL mode enabled.
func IsWALModeEnabledSQLiteFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	b := make([]byte, 20)
	if _, err := f.Read(b); err != nil {
		return false
	}

	return IsWALModeEnabled(b)
}

// IsWALModeEnabled checks that the supplied data looks like a SQLite data
// with WAL mode enabled.
func IsWALModeEnabled(b []byte) bool {
	return len(b) >= 20 && b[18] == 2 && b[19] == 2
}

// IsDELETEModeEnabledSQLiteFile checks that the supplied path looks like a SQLite
// with DELETE mode enabled.
func IsDELETEModeEnabledSQLiteFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	b := make([]byte, 20)
	if _, err := f.Read(b); err != nil {
		return false
	}

	return IsDELETEModeEnabled(b)
}

// IsDELETEModeEnabled checks that the supplied path looks like a SQLite file
// with DELETE mode enabled.
func IsDELETEModeEnabled(b []byte) bool {
	return len(b) >= 20 && b[18] == 1 && b[19] == 1
}

// RemoveFiles removes the SQLite database file, and any associated WAL and SHM files.
func RemoveFiles(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(path + "-wal"); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(path + "-shm"); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// CheckIntegrity runs a PRAGMA integrity_check on the database at the given path.
// If full is true, a full integrity check is performed, otherwise a quick check.
func CheckIntegrity(path string, full bool) (bool, error) {
	db, err := Open(path, false, false)
	if err != nil {
		return false, err
	}
	defer db.Close()

	sql := "PRAGMA quick_check"
	if full {
		sql = "PRAGMA integrity_check"
	}

	rows, err := db.rwDB.Query(sql)
	if err != nil {
		return false, err
	}

	var result string
	if !rows.Next() {
		return false, nil
	}
	if err := rows.Scan(&result); err != nil {
		return false, err
	}
	return result == "ok", nil
}

// ReplayWAL replays the given WAL files into the database at the given path,
// in the order given by the slice. The supplied WAL files must be in the same
// directory as the database file and are removed as a result of the replay operation.
// The "real" WAL file is also removed. If deleteMode is true, the database file
// will be in DELETE mode after the replay operation, otherwise it will be in WAL
// mode.
func ReplayWAL(path string, wals []string, deleteMode bool) error {
	for _, wal := range wals {
		if filepath.Dir(wal) != filepath.Dir(path) {
			return ErrWALReplayDirectoryMismatch
		}
	}

	if !IsValidSQLiteFile(path) {
		return fmt.Errorf("invalid database file %s", path)
	}

	for _, wal := range wals {
		if !IsValidSQLiteWALFile(wal) {
			return fmt.Errorf("invalid WAL file %s", wal)
		}
		if err := os.Rename(wal, path+"-wal"); err != nil {
			return fmt.Errorf("rename WAL %s: %s", wal, err.Error())
		}
		db, err := Open(path, false, true)
		if err != nil {
			return err
		}
		if err := db.Checkpoint(defaultCheckpointTimeout); err != nil {
			return fmt.Errorf("checkpoint WAL %s: %s", wal, err.Error())
		}
		if err := db.Close(); err != nil {
			return err
		}
	}

	if deleteMode {
		db, err := Open(path, false, false)
		if err != nil {
			return err
		}
		if db.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Open opens a file-based database, creating it if it does not exist. After this
// function returns, an actual SQLite file will always exist.
func Open(dbPath string, fkEnabled, wal bool) (*DB, error) {
	rwDSN := fmt.Sprintf("file:%s?_fk=%s", dbPath, strconv.FormatBool(fkEnabled))
	rwDB, err := sql.Open("sqlite3", rwDSN)
	if err != nil {
		return nil, fmt.Errorf("open: %s", err.Error())
	}

	// Set synchronous to OFF, to improve performance. The SQLite docs state that
	// this risks database corruption in the event of a crash, but that's OK, as
	// rqlite blows away the database on startup and always rebuilds it from the
	// Raft log.
	if _, err := rwDB.Exec("PRAGMA synchronous=OFF"); err != nil {
		return nil, fmt.Errorf("sync OFF: %s", err.Error())
	}

	mode := "WAL"
	if !wal {
		mode = "DELETE"
	}
	if _, err := rwDB.Exec(fmt.Sprintf("PRAGMA journal_mode=%s", mode)); err != nil {
		return nil, fmt.Errorf("journal mode to %s: %s", mode, err.Error())
	}

	roOpts := []string{
		"mode=ro",
		fmt.Sprintf("_fk=%s", strconv.FormatBool(fkEnabled)),
	}

	roDSN := fmt.Sprintf("file:%s?%s", dbPath, strings.Join(roOpts, "&"))
	roDB, err := sql.Open("sqlite3", roDSN)
	if err != nil {
		return nil, err
	}

	// Force creation of on-disk database file.
	if err := rwDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping on-disk database: %s", err.Error())
	}

	// Set some reasonable connection pool behaviour.
	rwDB.SetConnMaxIdleTime(30 * time.Second)
	rwDB.SetConnMaxLifetime(0)
	roDB.SetConnMaxIdleTime(30 * time.Second)
	roDB.SetConnMaxLifetime(0)

	return &DB{
		path:      dbPath,
		walPath:   dbPath + "-wal",
		fkEnabled: fkEnabled,
		wal:       wal,
		rwDB:      rwDB,
		roDB:      roDB,
		rwDSN:     rwDSN,
		roDSN:     roDSN,
	}, nil
}

// OpenInMemory returns a new in-memory database.
func OpenInMemory(fkEnabled bool) (*DB, error) {
	inMemPath := fmt.Sprintf("file:/%s", randomString())

	rwOpts := []string{
		"mode=rw",
		"vfs=memdb",
		"_txlock=immediate",
		fmt.Sprintf("_fk=%s", strconv.FormatBool(fkEnabled)),
	}

	rwDSN := fmt.Sprintf("%s?%s", inMemPath, strings.Join(rwOpts, "&"))
	rwDB, err := sql.Open("sqlite3", rwDSN)
	if err != nil {
		return nil, err
	}

	// Ensure there is only one connection and it never closes.
	// If it closed, in-memory database could be lost.
	rwDB.SetConnMaxIdleTime(0)
	rwDB.SetConnMaxLifetime(0)
	rwDB.SetMaxIdleConns(1)
	rwDB.SetMaxOpenConns(1)

	roOpts := []string{
		"mode=ro",
		"vfs=memdb",
		"_txlock=deferred",
		fmt.Sprintf("_fk=%s", strconv.FormatBool(fkEnabled)),
	}

	roDSN := fmt.Sprintf("%s?%s", inMemPath, strings.Join(roOpts, "&"))
	roDB, err := sql.Open("sqlite3", roDSN)
	if err != nil {
		return nil, err
	}

	// Ensure database is basically healthy.
	if err := rwDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping in-memory database: %s", err.Error())
	}

	return &DB{
		memory:    true,
		path:      ":memory:",
		fkEnabled: fkEnabled,
		rwDB:      rwDB,
		roDB:      roDB,
		rwDSN:     rwDSN,
		roDSN:     roDSN,
	}, nil
}

// LoadIntoMemory loads an in-memory database with that at the path.
// Not safe to call while other operations are happening with the
// source database.
func LoadIntoMemory(dbPath string, fkEnabled, wal bool) (*DB, error) {
	dstDB, err := OpenInMemory(fkEnabled)
	if err != nil {
		return nil, err
	}

	srcDB, err := Open(dbPath, fkEnabled, wal)
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

// DeserializeIntoMemory loads an in-memory database with that contained
// in the byte slide. The byte slice must not be changed or garbage-collected
// until after this function returns.
func DeserializeIntoMemory(b []byte, fkEnabled bool) (retDB *DB, retErr error) {
	// Get a plain-ol' in-memory database.
	tmpDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("DeserializeIntoMemory: %s", err.Error())
	}
	defer tmpDB.Close()

	tmpConn, err := tmpDB.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	// tmpDB will still be using memory in Go space, so tmpDB needs to be explicitly
	// copied to a new database, which we create now.
	retDB, err = OpenInMemory(fkEnabled)
	if err != nil {
		return nil, fmt.Errorf("DeserializeIntoMemory: %s", err.Error())
	}
	defer func() {
		// Don't leak a database if deserialization fails.
		if retDB != nil && retErr != nil {
			retDB.Close()
		}
	}()

	if err := tmpConn.Raw(func(driverConn interface{}) error {
		srcConn := driverConn.(*sqlite3.SQLiteConn)
		err2 := srcConn.Deserialize(b, "")
		if err2 != nil {
			return fmt.Errorf("DeserializeIntoMemory: %s", err2.Error())
		}
		defer srcConn.Close()

		// Now copy from tmp database to the database this function will return.
		dbConn, err3 := retDB.rwDB.Conn(context.Background())
		if err3 != nil {
			return fmt.Errorf("DeserializeIntoMemory: %s", err3.Error())
		}
		defer dbConn.Close()

		return dbConn.Raw(func(driverConn interface{}) error {
			dstConn := driverConn.(*sqlite3.SQLiteConn)
			return copyDatabaseConnection(dstConn, srcConn)
		})

	}); err != nil {
		return nil, err
	}

	return retDB, nil
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	if err := db.rwDB.Close(); err != nil {
		return err
	}
	return db.roDB.Close()
}

// Stats returns status and diagnostics for the database.
func (db *DB) Stats() (map[string]interface{}, error) {
	copts, err := db.CompileOptions()
	if err != nil {
		return nil, err
	}
	memStats, err := db.memStats()
	if err != nil {
		return nil, err
	}
	connPoolStats := map[string]interface{}{
		"ro": db.ConnectionPoolStats(db.roDB),
		"rw": db.ConnectionPoolStats(db.rwDB),
	}
	dbSz, err := db.Size()
	if err != nil {
		return nil, err
	}
	pragmas, err := db.pragmas()
	if err != nil {
		return nil, err
	}
	stats := map[string]interface{}{
		"version":         DBVersion,
		"compile_options": copts,
		"mem_stats":       memStats,
		"db_size":         dbSz,
		"rw_dsn":          db.rwDSN,
		"ro_dsn":          db.roDSN,
		"conn_pool_stats": connPoolStats,
		"pragmas":         pragmas,
	}

	stats["path"] = db.path
	if !db.memory {
		if stats["size"], err = db.FileSize(); err != nil {
			return nil, err
		}
		if db.wal {
			if stats["wal_size"], err = db.WALSize(); err != nil {
				return nil, err
			}
		}
	}
	return stats, nil
}

// Size returns the size of the database in bytes. "Size" is defined as
// page_count * schema.page_size.
func (db *DB) Size() (int64, error) {
	rows, err := db.QueryStringStmt(`SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()`)
	if err != nil {
		return 0, err
	}
	if rows[0].Error != "" {
		return 0, fmt.Errorf(rows[0].Error)
	}

	return rows[0].Values[0].Parameters[0].GetI(), nil
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

// WALSize returns the size of the SQLite WAL file on disk. If running in
// WAL mode is not enabled, this function returns 0.
func (db *DB) WALSize() (int64, error) {
	if !db.wal {
		return 0, nil
	}
	fi, err := os.Stat(db.walPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return 0, err
		}
		return 0, nil
	}
	return fi.Size(), nil
}

// Checkpoint performs a WAL checkpoint. If the checkpoint does not complete
// within the given duration, an error is returned.
func (db *DB) Checkpoint(dur time.Duration) (err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			stats.Add(numCheckpointErrors, 1)
		} else {
			stats.Get(checkpointDuration).(*expvar.Int).Set(time.Since(start).Nanoseconds())
			stats.Add(numCheckpoints, 1)
		}
	}()

	var ok int
	var nPages int
	var nMoved int

	f := func() error {
		err := db.rwDB.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&ok, &nPages, &nMoved)
		if err != nil {
			return err
		}
		if ok != 0 {
			return fmt.Errorf("failed to completely checkpoint WAL")
		}
		return nil
	}

	// Try fast path
	if err := f(); err == nil {
		return nil
	}

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := f(); err == nil {
				return nil
			}
		case <-time.After(dur):
			return fmt.Errorf("checkpoint timeout")
		}
	}
}

// DisableCheckpointing disables the automatic checkpointing that occurs when
// the WAL reaches a certain size. This is key for full control of snapshotting.
// and can be useful for testing.
func (db *DB) DisableCheckpointing() error {
	_, err := db.rwDB.Exec("PRAGMA wal_autocheckpoint=-1")
	return err
}

// EnableCheckpointing enables the automatic checkpointing that occurs when
// the WAL reaches a certain size.
func (db *DB) EnableCheckpointing() error {
	_, err := db.rwDB.Exec("PRAGMA wal_autocheckpoint=1000")
	return err
}

// InMemory returns whether this database is in-memory.
func (db *DB) InMemory() bool {
	return db.memory
}

// FKEnabled returns whether Foreign Key constraints are enabled.
func (db *DB) FKEnabled() bool {
	return db.fkEnabled
}

// WALEnabled returns whether WAL mode is enabled.
func (db *DB) WALEnabled() bool {
	return db.wal
}

// Path returns the path of this database.
func (db *DB) Path() string {
	return db.path
}

// CompileOptions returns the SQLite compilation options.
func (db *DB) CompileOptions() ([]string, error) {
	res, err := db.QueryStringStmt("PRAGMA compile_options")
	if err != nil {
		return nil, err
	}
	if len(res) != 1 {
		return nil, fmt.Errorf("compile options result wrong size (%d)", len(res))
	}

	copts := make([]string, len(res[0].Values))
	for i := range copts {
		if len(res[0].Values[i].Parameters) != 1 {
			return nil, fmt.Errorf("compile options values wrong size (%d)", len(res))
		}
		copts[i] = res[0].Values[i].Parameters[0].GetS()
	}
	return copts, nil
}

// ConnectionPoolStats returns database pool statistics
func (db *DB) ConnectionPoolStats(sqlDB *sql.DB) *PoolStats {
	s := sqlDB.Stats()
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
func (db *DB) ExecuteStringStmt(query string) ([]*command.ExecuteResult, error) {
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
func (db *DB) Execute(req *command.Request, xTime bool) ([]*command.ExecuteResult, error) {
	stats.Add(numExecutions, int64(len(req.Statements)))
	conn, err := db.rwDB.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return db.executeWithConn(req, xTime, conn)
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func (db *DB) executeWithConn(req *command.Request, xTime bool, conn *sql.Conn) ([]*command.ExecuteResult, error) {
	var err error

	var execer execer
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

	var allResults []*command.ExecuteResult

	// handleError sets the error field on the given result. It returns
	// whether the caller should continue processing or break.
	handleError := func(result *command.ExecuteResult, err error) bool {
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

		result, err := db.executeStmtWithConn(stmt, xTime, execer)
		if err != nil {
			if handleError(result, err) {
				continue
			}
			break
		}
		allResults = append(allResults, result)
	}

	if tx != nil {
		err = tx.Commit()
	}
	return allResults, err
}

func (db *DB) executeStmtWithConn(stmt *command.Statement, xTime bool, e execer) (*command.ExecuteResult, error) {
	result := &command.ExecuteResult{}
	start := time.Now()

	parameters, err := parametersToValues(stmt.Parameters)
	if err != nil {
		result.Error = err.Error()
		return result, nil
	}

	r, err := e.ExecContext(context.Background(), stmt.Sql, parameters...)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}

	if r == nil {
		return result, nil
	}

	lid, err := r.LastInsertId()
	if err != nil {
		result.Error = err.Error()
		return result, err
	}
	result.LastInsertId = lid

	ra, err := r.RowsAffected()
	if err != nil {
		result.Error = err.Error()
		return result, err
	}
	result.RowsAffected = ra
	if xTime {
		result.Time = time.Since(start).Seconds()
	}
	return result, nil
}

// QueryStringStmt executes a single query that return rows, but don't modify database.
func (db *DB) QueryStringStmt(query string) ([]*command.QueryRows, error) {
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
func (db *DB) Query(req *command.Request, xTime bool) ([]*command.QueryRows, error) {
	stats.Add(numQueries, int64(len(req.Statements)))
	conn, err := db.roDB.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return db.queryWithConn(req, xTime, conn)
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func (db *DB) queryWithConn(req *command.Request, xTime bool, conn *sql.Conn) ([]*command.QueryRows, error) {
	var err error

	var queryer queryer
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

	var allRows []*command.QueryRows
	for _, stmt := range req.Statements {
		sql := stmt.Sql
		if sql == "" {
			continue
		}

		var rows *command.QueryRows
		var err error

		readOnly, err := db.StmtReadOnlyWithConn(sql, conn)
		if err != nil {
			stats.Add(numQueryErrors, 1)
			rows = &command.QueryRows{
				Error: err.Error(),
			}
			allRows = append(allRows, rows)
			continue
		}
		if !readOnly {
			stats.Add(numQueryErrors, 1)
			rows = &command.QueryRows{
				Error: "attempt to change database via query operation",
			}
			allRows = append(allRows, rows)
			continue
		}

		rows, err = db.queryStmtWithConn(stmt, xTime, queryer)
		if err != nil {
			stats.Add(numQueryErrors, 1)
			rows = &command.QueryRows{
				Error: err.Error(),
			}
		}
		allRows = append(allRows, rows)
	}

	if tx != nil {
		err = tx.Commit()
	}
	return allRows, err
}

func (db *DB) queryStmtWithConn(stmt *command.Statement, xTime bool, q queryer) (*command.QueryRows, error) {
	rows := &command.QueryRows{}
	start := time.Now()

	parameters, err := parametersToValues(stmt.Parameters)
	if err != nil {
		stats.Add(numQueryErrors, 1)
		rows.Error = err.Error()
		return rows, nil
	}

	rs, err := q.QueryContext(context.Background(), stmt.Sql, parameters...)
	if err != nil {
		stats.Add(numQueryErrors, 1)
		rows.Error = err.Error()
		return rows, nil
	}
	defer rs.Close()

	columns, err := rs.Columns()
	if err != nil {
		return nil, err
	}

	types, err := rs.ColumnTypes()
	if err != nil {
		return nil, err
	}
	xTypes := make([]string, len(types))
	for i := range types {
		xTypes[i] = strings.ToLower(types[i].DatabaseTypeName())
	}
	needsQueryTypes := containsEmptyType(xTypes)

	for rs.Next() {
		dest := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(dest))
		for i := range ptrs {
			ptrs[i] = &dest[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			return nil, err
		}
		params, err := normalizeRowValues(dest, xTypes)
		if err != nil {
			return nil, err
		}
		rows.Values = append(rows.Values, &command.Values{
			Parameters: params,
		})

		// One-time population of any empty types. Best effort, ignore
		// error.
		if needsQueryTypes {
			populateEmptyTypes(xTypes, params)
			needsQueryTypes = false
		}
	}

	// Check for errors from iterating over rows.
	if err := rs.Err(); err != nil {
		stats.Add(numQueryErrors, 1)
		rows.Error = err.Error()
		return rows, nil
	}

	if xTime {
		rows.Time = time.Since(start).Seconds()
	}

	rows.Columns = columns
	rows.Types = xTypes
	return rows, nil
}

// RequestStringStmts processes a request that can contain both executes and queries.
func (db *DB) RequestStringStmts(stmts []string) ([]*command.ExecuteQueryResponse, error) {
	req := &command.Request{}
	for _, q := range stmts {
		req.Statements = append(req.Statements, &command.Statement{
			Sql: q,
		})
	}
	return db.Request(req, false)
}

// Request processes a request that can contain both executes and queries.
func (db *DB) Request(req *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, error) {
	stats.Add(numRequests, int64(len(req.Statements)))
	conn, err := db.rwDB.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var queryer queryer
	var execer execer
	var tx *sql.Tx
	if req.Transaction {
		stats.Add(numRTx, 1)
		tx, err = conn.BeginTx(context.Background(), nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback() // Will be ignored if tx is committed
		queryer = tx
		execer = tx
	} else {
		queryer = conn
		execer = conn
	}

	// abortOnError indicates whether the caller should continue
	// processing or break.
	abortOnError := func(err error) bool {
		if err != nil && tx != nil {
			tx.Rollback()
			tx = nil
			return true
		}
		return false
	}

	var eqResponse []*command.ExecuteQueryResponse
	for _, stmt := range req.Statements {
		ss := stmt.Sql
		if ss == "" {
			continue
		}

		ro, err := db.StmtReadOnlyWithConn(ss, conn)
		if err != nil {
			eqResponse = append(eqResponse, &command.ExecuteQueryResponse{
				Result: &command.ExecuteQueryResponse_Error{
					Error: err.Error(),
				},
			})
			continue
		}

		if ro {
			rows, opErr := db.queryStmtWithConn(stmt, xTime, queryer)
			eqResponse = append(eqResponse, createEQQueryResponse(rows, opErr))
			if abortOnError(opErr) {
				break
			}
		} else {
			result, opErr := db.executeStmtWithConn(stmt, xTime, execer)
			eqResponse = append(eqResponse, createEQExecuteResponse(result, opErr))
			if abortOnError(opErr) {
				break
			}
		}
	}

	if tx != nil {
		err = tx.Commit()
	}
	return eqResponse, err
}

// Backup writes a consistent snapshot of the database to the given file.
// The resultant SQLite database file will be in DELETE mode. This function
// can be called when changes to the database are in flight.
func (db *DB) Backup(path string) error {
	dstDB, err := Open(path, false, false)
	if err != nil {
		return err
	}
	defer dstDB.Close()

	if err := copyDatabase(dstDB, db); err != nil {
		return fmt.Errorf("backup database: %s", err)
	}

	// Source database might be in WAL mode.
	_, err = dstDB.ExecuteStringStmt("PRAGMA journal_mode=DELETE")
	if err != nil {
		return err
	}

	return dstDB.Close()
}

// Copy copies the contents of the database to the given database. All other
// attributes of the given database remain untouched e.g. whether it's an
// on-disk database, except the database will be placed in DELETE mode.
// This function can be called when changes to the source database are in flight.
func (db *DB) Copy(dstDB *DB) error {
	if err := copyDatabase(dstDB, db); err != nil {
		return fmt.Errorf("copy database: %s", err)
	}
	_, err := dstDB.ExecuteStringStmt("PRAGMA journal_mode=DELETE")
	return err
}

// Serialize returns a byte slice representation of the SQLite database. For
// an ordinary on-disk database file, the serialization is just a copy of the
// disk file. For an in-memory database or a "TEMP" database, the serialization
// is the same sequence of bytes which would be written to disk if that database
// were backed up to disk. If the database is in WAL mode, a temporary on-disk
// copy is made, and it is this copy that is serialized. This function must not
// be called while any writes are happening to the database.
func (db *DB) Serialize() ([]byte, error) {
	if !db.memory {
		if db.wal {
			tmpFile, err := os.CreateTemp("", "rqlite-serialize")
			if err != nil {
				return nil, err
			}
			defer os.Remove(tmpFile.Name())
			defer tmpFile.Close()

			if err := db.Backup(tmpFile.Name()); err != nil {
				return nil, err
			}
			newDB, err := Open(tmpFile.Name(), db.fkEnabled, false)
			if err != nil {
				return nil, err
			}
			defer newDB.Close()
			return newDB.Serialize()
		}
		// Simply read and return the SQLite file.
		b, err := os.ReadFile(db.path)
		if err != nil {
			return nil, err
		}
		return b, nil
	}

	conn, err := db.roDB.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var b []byte
	if err := conn.Raw(func(raw interface{}) error {
		var err error
		b, err = raw.(*sqlite3.SQLiteConn).Serialize("")
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to serialize database: %s", err.Error())
	}

	return b, nil
}

// Dump writes a consistent snapshot of the database in SQL text format.
// This function can be called when changes to the database are in flight.
func (db *DB) Dump(w io.Writer) error {
	conn, err := db.roDB.Conn(context.Background())
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
		table := v.Parameters[0].GetS()
		var stmt string

		if table == "sqlite_sequence" {
			stmt = `DELETE FROM "sqlite_sequence";`
		} else if table == "sqlite_stat1" {
			stmt = `ANALYZE "sqlite_master";`
		} else if strings.HasPrefix(table, "sqlite_") {
			continue
		} else {
			stmt = v.Parameters[2].GetS()
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
			columnNames = append(columnNames, fmt.Sprintf(`'||quote("%s")||'`, w.Parameters[1].GetS()))
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
			y := fmt.Sprintf("%s;\n", x.Parameters[0].GetS())
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
		if _, err := w.Write([]byte(fmt.Sprintf("%s;\n", v.Parameters[2].GetS()))); err != nil {
			return err
		}
	}

	if _, err := w.Write([]byte("COMMIT;\n")); err != nil {
		return err
	}

	return nil
}

// StmtReadOnly returns whether the given SQL statement is read-only.
// As per https://www.sqlite.org/c3ref/stmt_readonly.html, this function
// may not return 100% correct results, but should cover most scenarios.
func (db *DB) StmtReadOnly(sql string) (bool, error) {
	conn, err := db.roDB.Conn(context.Background())
	if err != nil {
		return false, err
	}
	defer conn.Close()
	return db.StmtReadOnlyWithConn(sql, conn)
}

// StmtReadOnlyWithConn returns whether the given SQL statement is read-only, using
// the given connection.
func (db *DB) StmtReadOnlyWithConn(sql string, conn *sql.Conn) (bool, error) {
	var readOnly bool
	f := func(driverConn interface{}) error {
		c := driverConn.(*sqlite3.SQLiteConn)
		drvStmt, err := c.Prepare(sql)
		if err != nil {
			return err
		}
		defer drvStmt.Close()
		sqliteStmt := drvStmt.(*sqlite3.SQLiteStmt)
		readOnly = sqliteStmt.Readonly()
		return nil
	}

	if err := conn.Raw(f); err != nil {
		return false, err
	}
	return readOnly, nil
}

func (db *DB) pragmas() (map[string]interface{}, error) {
	conns := map[string]*sql.DB{
		"rw": db.rwDB,
		"ro": db.roDB,
	}

	connsMap := make(map[string]interface{})
	for k, v := range conns {
		pragmasMap := make(map[string]string)
		for _, p := range []string{
			"synchronous",
			"journal_mode",
			"foreign_keys",
			"wal_autocheckpoint",
		} {
			var s string
			if err := v.QueryRow(fmt.Sprintf("PRAGMA %s", p)).Scan(&s); err != nil {
				return nil, err
			}
			pragmasMap[p] = s
		}
		connsMap[k] = pragmasMap
	}

	return connsMap, nil
}

func (db *DB) memStats() (map[string]int64, error) {
	ms := make(map[string]int64)
	for _, p := range []string{
		"max_page_count",
		"page_count",
		"page_size",
		"hard_heap_limit",
		"soft_heap_limit",
		"cache_size",
		"freelist_count",
	} {
		res, err := db.QueryStringStmt(fmt.Sprintf("PRAGMA %s", p))
		if err != nil {
			return nil, err
		}
		if res[0].Error != "" {
			return nil, fmt.Errorf(res[0].Error)
		}
		ms[p] = res[0].Values[0].Parameters[0].GetI()
	}
	return ms, nil
}

func createEQQueryResponse(rows *command.QueryRows, err error) *command.ExecuteQueryResponse {
	if err != nil {
		return &command.ExecuteQueryResponse{
			Result: &command.ExecuteQueryResponse_Q{
				Q: &command.QueryRows{
					Error: err.Error(),
				},
			},
		}
	}
	return &command.ExecuteQueryResponse{
		Result: &command.ExecuteQueryResponse_Q{
			Q: rows,
		},
	}
}

func createEQExecuteResponse(execResult *command.ExecuteResult, err error) *command.ExecuteQueryResponse {
	if err != nil {
		return &command.ExecuteQueryResponse{
			Result: &command.ExecuteQueryResponse_E{
				E: &command.ExecuteResult{
					Error: err.Error(),
				},
			},
		}
	}
	return &command.ExecuteQueryResponse{
		Result: &command.ExecuteQueryResponse_E{
			E: execResult,
		},
	}
}

func copyDatabase(dst *DB, src *DB) error {
	dstConn, err := dst.rwDB.Conn(context.Background())
	if err != nil {
		return err
	}
	defer dstConn.Close()
	srcConn, err := src.roDB.Conn(context.Background())
	if err != nil {
		return err
	}
	defer srcConn.Close()

	var dstSQLiteConn *sqlite3.SQLiteConn

	// Define the backup function.
	bf := func(driverConn interface{}) error {
		srcSQLiteConn := driverConn.(*sqlite3.SQLiteConn)
		return copyDatabaseConnection(dstSQLiteConn, srcSQLiteConn)
	}

	return dstConn.Raw(
		func(driverConn interface{}) error {
			dstSQLiteConn = driverConn.(*sqlite3.SQLiteConn)
			return srcConn.Raw(bf)
		})
}

func copyDatabaseConnection(dst, src *sqlite3.SQLiteConn) error {
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

// parametersToValues maps values in the proto params to SQL driver values.
func parametersToValues(parameters []*command.Parameter) ([]interface{}, error) {
	if parameters == nil {
		return nil, nil
	}

	values := make([]interface{}, len(parameters))
	for i := range parameters {
		switch w := parameters[i].GetValue().(type) {
		case *command.Parameter_I:
			values[i] = sql.Named(parameters[i].GetName(), w.I)
		case *command.Parameter_D:
			values[i] = sql.Named(parameters[i].GetName(), w.D)
		case *command.Parameter_B:
			values[i] = sql.Named(parameters[i].GetName(), w.B)
		case *command.Parameter_Y:
			values[i] = sql.Named(parameters[i].GetName(), w.Y)
		case *command.Parameter_S:
			values[i] = sql.Named(parameters[i].GetName(), w.S)
		case nil:
			values[i] = sql.Named(parameters[i].GetName(), nil)
		default:
			return nil, fmt.Errorf("unsupported type: %T", w)
		}
	}
	return values, nil
}

// populateEmptyTypes populates any empty types with the type of the parameter.
// This is necessary because the SQLite driver doesn't return the type of the
// column in some cases e.g. it's an expression, so we use the actual types
// of the returned data to fill in the blanks.
func populateEmptyTypes(types []string, params []*command.Parameter) error {
	for i := range types {
		if types[i] == "" {
			switch params[i].GetValue().(type) {
			case *command.Parameter_I:
				types[i] = "integer"
			case *command.Parameter_D:
				types[i] = "real"
			case *command.Parameter_B:
				types[i] = "boolean"
			case *command.Parameter_Y:
				types[i] = "blob"
			case *command.Parameter_S:
				types[i] = "text"
			default:
				return fmt.Errorf("unsupported type: %T", params[i].GetValue())
			}
		}
	}
	return nil
}

// normalizeRowValues performs some normalization of values in the returned rows.
// Text values come over (from sqlite-go) as []byte instead of strings
// for some reason, so we have explicitly converted (but only when type
// is "text" so we don't affect BLOB types)
func normalizeRowValues(row []interface{}, types []string) ([]*command.Parameter, error) {
	values := make([]*command.Parameter, len(types))
	for i, v := range row {
		switch val := v.(type) {
		case int:
			values[i] = &command.Parameter{
				Value: &command.Parameter_I{
					I: int64(val)},
				Name: "",
			}
		case int64:
			values[i] = &command.Parameter{
				Value: &command.Parameter_I{
					I: val,
				},
			}
		case float64:
			values[i] = &command.Parameter{
				Value: &command.Parameter_D{
					D: val,
				},
			}
		case bool:
			values[i] = &command.Parameter{
				Value: &command.Parameter_B{
					B: val,
				},
			}
		case string:
			values[i] = &command.Parameter{
				Value: &command.Parameter_S{
					S: val,
				},
			}
		case []byte:
			if isTextType(types[i]) {
				values[i].Value = &command.Parameter_S{
					S: string(val),
				}
			} else {
				values[i] = &command.Parameter{
					Value: &command.Parameter_Y{
						Y: val,
					},
				}
			}
		case time.Time:
			rfc3339, err := val.MarshalText()
			if err != nil {
				return nil, err
			}
			values[i] = &command.Parameter{
				Value: &command.Parameter_S{
					S: string(rfc3339),
				},
			}
		case nil:
			continue
		default:
			return nil, fmt.Errorf("unhandled column type: %T %v", val, val)
		}
	}
	return values, nil
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

func randomString() string {
	var output strings.Builder
	chars := "abcdedfghijklmnopqrstABCDEFGHIJKLMNOP"

	for i := 0; i < 20; i++ {
		random := rand.Intn(len(chars))
		randomChar := chars[random]
		output.WriteByte(randomChar)
	}
	return output.String()
}

func containsEmptyType(slice []string) bool {
	for _, str := range slice {
		if str == "" {
			return true
		}
	}
	return false
}
