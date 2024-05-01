// Package db exposes a lightweight abstraction over the SQLite code.
// It performs some basic mapping of lower-level types to rqlite types.
package db

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rqlite/go-sqlite3"
	command "github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/db/humanize"
)

const (
	SQLiteHeaderSize = 32
	bkDelay          = 250
	durToOpenLog     = 2 * time.Second
)

const (
	openDuration              = "open_duration_ms"
	numCheckpoints            = "checkpoints"
	numCheckpointErrors       = "checkpoint_errors"
	numCheckpointedPages      = "checkpointed_pages"
	numCheckpointedMoves      = "checkpointed_moves"
	checkpointDuration        = "checkpoint_duration_ms"
	numExecutions             = "executions"
	numExecutionErrors        = "execution_errors"
	numExecutionsForceQueries = "executions_force_queries"
	numQueries                = "queries"
	numQueryErrors            = "query_errors"
	numRequests               = "requests"
	numETx                    = "execute_transactions"
	numQTx                    = "query_transactions"
	numRTx                    = "request_transactions"
	numBackupStepErrors       = "backup_step_errors"
	numBackupStepDones        = "backup_step_dones"
	numBackupSleeps           = "backup_sleeps"
)

var (
	// ErrWALReplayDirectoryMismatch is returned when the WAL file(s) are not in the same
	// directory as the database file.
	ErrWALReplayDirectoryMismatch = errors.New("WAL file(s) not in same directory as database file")

	// ErrQueryTimeout is returned when a query times out.
	ErrQueryTimeout = errors.New("query timeout")

	// ErrExecuteTimeout is returned when an execute times out.
	ErrExecuteTimeout = errors.New("execute timeout")
)

// CheckpointMode is the mode in which a checkpoint runs.
type CheckpointMode int

const (
	// CheckpointRestart instructs the checkpoint to run in restart mode.
	CheckpointRestart CheckpointMode = iota
	// CheckpointTruncate instructs the checkpoint to run in truncate mode.
	CheckpointTruncate
)

var (
	checkpointPRAGMAs = map[CheckpointMode]string{
		CheckpointRestart:  "PRAGMA wal_checkpoint(RESTART)",
		CheckpointTruncate: "PRAGMA wal_checkpoint(TRUNCATE)",
	}
)

// DBVersion is the SQLite version.
var DBVersion string

// stats captures stats for the DB layer.
var stats *expvar.Map

func init() {
	DBVersion, _, _ = sqlite3.Version()
	stats = expvar.NewMap("db")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(openDuration, 0)
	stats.Add(numCheckpoints, 0)
	stats.Add(numCheckpointErrors, 0)
	stats.Add(numCheckpointedPages, 0)
	stats.Add(numCheckpointedMoves, 0)
	stats.Add(checkpointDuration, 0)
	stats.Add(numExecutions, 0)
	stats.Add(numExecutionErrors, 0)
	stats.Add(numExecutionsForceQueries, 0)
	stats.Add(numQueries, 0)
	stats.Add(numQueryErrors, 0)
	stats.Add(numRequests, 0)
	stats.Add(numETx, 0)
	stats.Add(numQTx, 0)
	stats.Add(numRTx, 0)
	stats.Add(numBackupStepErrors, 0)
	stats.Add(numBackupStepDones, 0)
	stats.Add(numBackupSleeps, 0)
}

// DB is the SQL database.
type DB struct {
	path      string // Path to database file.
	walPath   string // Path to WAL file.
	fkEnabled bool   // Foreign key constraints enabled
	wal       bool

	rwDB *sql.DB // Database connection for database reads and writes.
	roDB *sql.DB // Database connection database reads.

	rwDSN string // DSN used for read-write connection
	roDSN string // DSN used for read-only connections

	logger *log.Logger
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

// Open opens a file-based database, creating it if it does not exist. After this
// function returns, an actual SQLite file will always exist.
func Open(dbPath string, fkEnabled, wal bool) (retDB *DB, retErr error) {
	logger := log.New(log.Writer(), "[db] ", log.LstdFlags)
	startTime := time.Now()
	defer func() {
		if retErr != nil {
			return
		}
		if dur := time.Since(startTime); dur > durToOpenLog {
			logger.Printf("opened database %s in %s", dbPath, dur)
		}
		stats.Get(openDuration).(*expvar.Int).Set(time.Since(startTime).Milliseconds())
	}()

	/////////////////////////////////////////////////////////////////////////
	// Main RW connection
	rwDSN := MakeDSN(dbPath, ModeReadWrite, fkEnabled, wal)
	rwDB, err := sql.Open("sqlite3", rwDSN)
	if err != nil {
		return nil, fmt.Errorf("open: %s", err.Error())
	}

	// Critical that rqlite has full control over the checkpointing process.
	if _, err := rwDB.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		return nil, fmt.Errorf("disable autocheckpointing: %s", err.Error())
	}

	/////////////////////////////////////////////////////////////////////////
	// Read-only connection
	roDSN := MakeDSN(dbPath, ModeReadOnly, fkEnabled, wal)
	roDB, err := sql.Open("sqlite3", roDSN)
	if err != nil {
		return nil, err
	}

	// Force creation of database file.
	if err := rwDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping on-disk database: %s", err.Error())
	}

	// Set connection pool behaviour.
	rwDB.SetConnMaxLifetime(0)
	rwDB.SetMaxOpenConns(1) // Key to ensure a new connection doesn't enable checkpointing
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
		logger:    logger,
	}, nil
}

// LastModified returns the last modified time of the database file, or the WAL file,
// whichever is most recent.
func (db *DB) LastModified() (time.Time, error) {
	dbTime, err := db.DBLastModified()
	if err != nil {
		return time.Time{}, err
	}
	walTime, err := db.WALLastModified()
	if err != nil {
		return time.Time{}, err
	}
	if dbTime.After(walTime) {
		return dbTime, nil
	}
	return walTime, nil
}

// DBLastModified returns the last modified time of the database file.
func (db *DB) DBLastModified() (time.Time, error) {
	return lastModified(db.path)
}

// DBSum returns the MD5 checksum of the database file.
func (db *DB) DBSum() (string, error) {
	return md5sum(db.path)
}

// WALLastModified returns the last modified time of the WAL file.
func (db *DB) WALLastModified() (time.Time, error) {
	return lastModified(db.walPath)
}

// WALSum returns the MD5 checksum of the WAL file.
func (db *DB) WALSum() (string, error) {
	return md5sum(db.walPath)
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
		"version":          DBVersion,
		"compile_options":  copts,
		"mem_stats":        memStats,
		"db_size":          dbSz,
		"db_size_friendly": humanize.Bytes(uint64(dbSz)),
		"rw_dsn":           db.rwDSN,
		"ro_dsn":           db.roDSN,
		"conn_pool_stats":  connPoolStats,
		"pragmas":          pragmas,
	}

	lm, err := db.LastModified()
	if err == nil {
		stats["last_modified"] = lm
	}

	stats["path"] = db.path
	if stats["size"], err = db.FileSize(); err != nil {
		return nil, err
	}
	if db.wal {
		if stats["wal_size"], err = db.WALSize(); err != nil {
			return nil, err
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
	return fileSize(db.path)
}

// WALSize returns the size of the SQLite WAL file on disk. If running in
// WAL mode is not enabled, this function returns 0.
func (db *DB) WALSize() (int64, error) {
	if !db.wal {
		return 0, nil
	}
	sz, err := fileSize(db.walPath)
	if err == nil || os.IsNotExist(err) {
		return sz, nil
	}
	return 0, err
}

// SetBusyTimeout sets the busy timeout for the database. If a timeout is
// is less than zero it is not set.
func (db *DB) SetBusyTimeout(rwMs, roMs int) (err error) {
	if rwMs >= 0 {
		_, err := db.rwDB.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", rwMs))
		if err != nil {
			return err
		}
	}
	if roMs >= 0 {
		_, err = db.roDB.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", roMs))
		if err != nil {
			return err
		}
	}
	return nil
}

// BusyTimeout returns the current busy timeout value.
func (db *DB) BusyTimeout() (rwMs, roMs int, err error) {
	err = db.rwDB.QueryRow("PRAGMA busy_timeout").Scan(&rwMs)
	if err != nil {
		return 0, 0, err
	}
	err = db.roDB.QueryRow("PRAGMA busy_timeout").Scan(&roMs)
	if err != nil {
		return 0, 0, err
	}
	return rwMs, roMs, nil
}

// Checkpoint checkpoints the WAL file. If the WAL file is not enabled, this
// function is a no-op.
func (db *DB) Checkpoint(mode CheckpointMode) error {
	return db.CheckpointWithTimeout(mode, 0)
}

// CheckpointWithTimeout performs a WAL checkpoint. If the checkpoint does not
// run to completion within the given duration, an error is returned. If the
// duration is 0, the busy timeout is not modified before executing the
// checkpoint.
func (db *DB) CheckpointWithTimeout(mode CheckpointMode, dur time.Duration) (err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			stats.Add(numCheckpointErrors, 1)
		} else {
			stats.Get(checkpointDuration).(*expvar.Int).Set(time.Since(start).Milliseconds())
			stats.Add(numCheckpoints, 1)
		}
	}()

	if dur > 0 {
		rwBt, _, err := db.BusyTimeout()
		if err != nil {
			return fmt.Errorf("failed to get busy_timeout on checkpointing connection: %s", err.Error())
		}
		if err := db.SetBusyTimeout(int(dur.Milliseconds()), -1); err != nil {
			return fmt.Errorf("failed to set busy_timeout on checkpointing connection: %s", err.Error())
		}
		defer func() {
			// Reset back to default
			if err := db.SetBusyTimeout(rwBt, -1); err != nil {
				db.logger.Printf("failed to reset busy_timeout on checkpointing connection: %s", err.Error())
			}
		}()
	}

	var ok int
	var nPages int
	var nMoved int
	if err := db.rwDB.QueryRow(checkpointPRAGMAs[mode]).Scan(&ok, &nPages, &nMoved); err != nil {
		return fmt.Errorf("error checkpointing WAL: %s", err.Error())
	}
	stats.Add(numCheckpointedPages, int64(nPages))
	stats.Add(numCheckpointedMoves, int64(nMoved))
	if ok != 0 {
		return fmt.Errorf("failed to completely checkpoint WAL (%d ok, %d pages, %d moved)",
			ok, nPages, nMoved)
	}
	return nil
}

// DisableCheckpointing disables the automatic checkpointing that occurs when
// the WAL reaches a certain size. This is key for full control of snapshotting.
// and can be useful for testing.
func (db *DB) DisableCheckpointing() error {
	_, err := db.rwDB.Exec("PRAGMA wal_autocheckpoint=0")
	return err
}

// EnableCheckpointing enables the automatic checkpointing that occurs when
// the WAL reaches a certain size.
func (db *DB) EnableCheckpointing() error {
	_, err := db.rwDB.Exec("PRAGMA wal_autocheckpoint=1000")
	return err
}

// GetCheckpointing returns the current checkpointing setting.
func (db *DB) GetCheckpointing() (int, error) {
	var rwN int
	err := db.rwDB.QueryRow("PRAGMA wal_autocheckpoint").Scan(&rwN)
	if err != nil {
		return 0, err
	}
	return rwN, err
}

// Vacuum runs a VACUUM on the database.
func (db *DB) Vacuum() error {
	_, err := db.rwDB.Exec("VACUUM")
	return err
}

// VacuumInto VACUUMs the database into the file at path
func (db *DB) VacuumInto(path string) error {
	_, err := db.rwDB.Exec(fmt.Sprintf("VACUUM INTO '%s'", path))
	return err
}

// IntegrityCheck runs a PRAGMA integrity_check on the database.
// If full is true, a full integrity check is performed, otherwise
// a quick check. It returns after hitting the first integrity
// failure, if any.
func (db *DB) IntegrityCheck(full bool) ([]*command.QueryRows, error) {
	if full {
		return db.QueryStringStmt("PRAGMA integrity_check(1)")
	}
	return db.QueryStringStmt("PRAGMA quick_check(1)")
}

// SetSynchronousMode sets the synchronous mode of the database.
func (db *DB) SetSynchronousMode(mode string) error {
	if mode != "OFF" && mode != "NORMAL" && mode != "FULL" && mode != "EXTRA" {
		return fmt.Errorf("invalid synchronous mode %s", mode)
	}
	if _, err := db.rwDB.Exec(fmt.Sprintf("PRAGMA synchronous=%s", mode)); err != nil {
		return fmt.Errorf("failed to set synchronous mode to %s: %s", mode, err.Error())
	}
	return nil
}

// GetSynchronousMode returns the current synchronous mode.
func (db *DB) GetSynchronousMode() (int, error) {
	var rwN int
	err := db.rwDB.QueryRow("PRAGMA synchronous").Scan(&rwN)
	if err != nil {
		return 0, err
	}
	return rwN, err
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

// WALPath returns the path to the WAL file for this database.
func (db *DB) WALPath() string {
	if !db.wal {
		return ""
	}
	return db.walPath
}

var compileOptions []string // Memoized compile options

// CompileOptions returns the SQLite compilation options.
func (db *DB) CompileOptions() ([]string, error) {
	if compileOptions != nil {
		return compileOptions, nil
	}
	res, err := db.QueryStringStmt("PRAGMA compile_options")
	if err != nil {
		return nil, err
	}
	if len(res) != 1 {
		return nil, fmt.Errorf("compile options result wrong size (%d)", len(res))
	}

	compileOptions = make([]string, len(res[0].Values))
	for i := range compileOptions {
		if len(res[0].Values[i].Parameters) != 1 {
			return nil, fmt.Errorf("compile options values wrong size (%d)", len(res))
		}
		compileOptions[i] = res[0].Values[i].Parameters[0].GetS()
	}
	return compileOptions, nil
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

// ExecuteStringStmtWithTimeout executes a single query that modifies the database.
// It also sets a timeout for the query. This is primarily a convenience function.
func (db *DB) ExecuteStringStmtWithTimeout(query string, timeout time.Duration) ([]*command.ExecuteQueryResponse, error) {
	r := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: query,
			},
		},
		DbTimeout: int64(timeout),
	}
	return db.Execute(r, false)
}

// ExecuteStringStmt executes a single query that modifies the database. This is
// primarily a convenience function.
func (db *DB) ExecuteStringStmt(query string) ([]*command.ExecuteQueryResponse, error) {
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
func (db *DB) Execute(req *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, error) {
	stats.Add(numExecutions, int64(len(req.Statements)))
	conn, err := db.rwDB.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx := context.Background()
	if req.DbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.DbTimeout))
		defer cancel()
	}
	return db.executeWithConn(ctx, req, xTime, conn)
}

type execerQueryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (db *DB) executeWithConn(ctx context.Context, req *command.Request, xTime bool, conn *sql.Conn) ([]*command.ExecuteQueryResponse, error) {
	var err error

	var eqer execerQueryer
	var tx *sql.Tx
	if req.Transaction {
		stats.Add(numETx, 1)
		tx, err = conn.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer func() {
			if tx != nil {
				tx.Rollback() // Will be ignored if tx is committed
			}
		}()
		eqer = tx
	} else {
		eqer = conn
	}

	var allResults []*command.ExecuteQueryResponse

	// handleError sets the error field on the given result. It returns
	// whether the caller should continue processing or break.
	handleError := func(result *command.ExecuteQueryResponse, err error) bool {
		stats.Add(numExecutionErrors, 1)
		result.Result = &command.ExecuteQueryResponse_Error{
			Error: err.Error(),
		}
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

		result, err := db.executeStmtWithConn(ctx, stmt, xTime, eqer, time.Duration(req.DbTimeout))
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

func (db *DB) executeStmtWithConn(ctx context.Context, stmt *command.Statement, xTime bool, eq execerQueryer, timeout time.Duration) (res *command.ExecuteQueryResponse, retErr error) {
	defer func() {
		if retErr != nil {
			retErr = rewriteContextTimeout(retErr, ErrExecuteTimeout)
			if res != nil {
				res.Result = &command.ExecuteQueryResponse_Error{
					Error: retErr.Error(),
				}
			}
		}
	}()
	response := &command.ExecuteQueryResponse{}
	start := time.Now()

	parameters, err := parametersToValues(stmt.Parameters)
	if err != nil {
		response.Result = &command.ExecuteQueryResponse_Error{
			Error: err.Error(),
		}
		return response, nil
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if stmt.ForceQuery {
		stats.Add(numExecutionsForceQueries, 1)
		rows, err := db.queryStmtWithConn(ctx, stmt, xTime, eq)
		if err != nil {
			response.Result = &command.ExecuteQueryResponse_Error{
				Error: err.Error(),
			}
			return response, nil
		}
		response.Result = &command.ExecuteQueryResponse_Q{
			Q: rows,
		}
	} else {
		result, err := eq.ExecContext(ctx, stmt.Sql, parameters...)
		if err != nil {
			response.Result = &command.ExecuteQueryResponse_Error{
				Error: err.Error(),
			}
			return response, err
		}
		if result == nil {
			return response, nil
		}

		lid, err := result.LastInsertId()
		if err != nil {
			response.Result = &command.ExecuteQueryResponse_Error{
				Error: err.Error(),
			}
			return response, err
		}

		ra, err := result.RowsAffected()
		if err != nil {
			response.Result = &command.ExecuteQueryResponse_Error{
				Error: err.Error(),
			}
			return response, err
		}
		tf := float64(0)
		if xTime {
			tf = time.Since(start).Seconds()
		}

		response.Result = &command.ExecuteQueryResponse_E{
			E: &command.ExecuteResult{
				LastInsertId: lid,
				RowsAffected: ra,
				Time:         tf,
			},
		}
	}
	return response, nil
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

// QueryStringStmtWithTimeout executes a single query that return rows, but don't modify database.
// It also sets a timeout for the query.
func (db *DB) QueryStringStmtWithTimeout(query string, tx bool, timeout time.Duration) ([]*command.QueryRows, error) {
	r := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: query,
			},
		},
		Transaction: tx,
		DbTimeout:   int64(timeout),
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

	ctx := context.Background()
	if req.DbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.DbTimeout))
		defer cancel()
	}
	return db.queryWithConn(ctx, req, xTime, conn)
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func (db *DB) queryWithConn(ctx context.Context, req *command.Request, xTime bool, conn *sql.Conn) ([]*command.QueryRows, error) {
	var err error

	var queryer queryer
	var tx *sql.Tx
	if req.Transaction {
		stats.Add(numQTx, 1)
		tx, err = conn.BeginTx(ctx, nil)
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

		rows, err = db.queryStmtWithConn(ctx, stmt, xTime, queryer)
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

func (db *DB) queryStmtWithConn(ctx context.Context, stmt *command.Statement, xTime bool, q queryer) (retRows *command.QueryRows, retErr error) {
	defer func() {
		if retErr != nil {
			retErr = rewriteContextTimeout(retErr, ErrQueryTimeout)
			if retRows != nil {
				retRows.Error = retErr.Error()
			}
		}
	}()
	rows := &command.QueryRows{}
	start := time.Now()

	parameters, err := parametersToValues(stmt.Parameters)
	if err != nil {
		stats.Add(numQueryErrors, 1)
		rows.Error = err.Error()
		return rows, nil
	}

	rs, err := q.QueryContext(ctx, stmt.Sql, parameters...)
	if err != nil {
		stats.Add(numQueryErrors, 1)
		rows.Error = err.Error()
		return rows, err
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
		return rows, err
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

// RequestStringStmtsWithTimeout processes a request that can contain both executes and queries.
func (db *DB) RequestStringStmtsWithTimeout(stmts []string, timeout time.Duration) ([]*command.ExecuteQueryResponse, error) {
	req := &command.Request{}
	for _, q := range stmts {
		req.Statements = append(req.Statements, &command.Statement{
			Sql: q,
		})
	}
	req.DbTimeout = int64(timeout)
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

	ctx := context.Background()
	if req.DbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.DbTimeout))
		defer cancel()
	}

	var eq execerQueryer
	var tx *sql.Tx
	if req.Transaction {
		stats.Add(numRTx, 1)
		tx, err = conn.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback() // Will be ignored if tx is committed
		eq = tx
	} else {
		eq = conn
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
			rows, opErr := db.queryStmtWithConn(ctx, stmt, xTime, eq)
			eqResponse = append(eqResponse, createEQQueryResponse(rows, opErr))
			if abortOnError(opErr) {
				break
			}
		} else {
			result, opErr := db.executeStmtWithConn(ctx, stmt, xTime, eq, time.Duration(req.DbTimeout))
			eqResponse = append(eqResponse, result)
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
func (db *DB) Backup(path string, vacuum bool) error {
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

	if vacuum {
		if err := dstDB.Vacuum(); err != nil {
			return err
		}
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
// disk file. If the database is in WAL mode, a temporary on-disk
// copy is made, and it is this copy that is serialized. This function must not
// be called while any writes are happening to the database.
func (db *DB) Serialize() ([]byte, error) {
	if db.wal {
		tmpFile, err := os.CreateTemp("", "rqlite-serialize")
		if err != nil {
			return nil, err
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		if err := db.Backup(tmpFile.Name(), false); err != nil {
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

// Dump writes a consistent snapshot of the database in SQL text format.
// This function can be called when changes to the database are in flight.
func (db *DB) Dump(w io.Writer) error {
	conn, err := db.roDB.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx := context.Background()

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
	rows, err := db.queryWithConn(ctx, commReq(query), false, conn)
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
		r, err := db.queryWithConn(ctx, commReq(fmt.Sprintf(`PRAGMA table_info("%s")`, tableIndent)),
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
		r, err = db.queryWithConn(ctx, commReq(query), false, conn)

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
	rows, err = db.queryWithConn(ctx, commReq(query), false, conn)
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
			"busy_timeout",
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
			stats.Add(numBackupStepErrors, 1)
			bk.Finish()
			return err
		}
		if done {
			stats.Add(numBackupStepDones, 1)
			break
		}
		stats.Add(numBackupSleeps, 1)
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
				values[i] = &command.Parameter{
					Value: &command.Parameter_S{
						S: string(val),
					},
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

func containsEmptyType(slice []string) bool {
	for _, str := range slice {
		if str == "" {
			return true
		}
	}
	return false
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func fileSize(path string) (int64, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if stat.Mode().IsDir() {
		return 0, fmt.Errorf("not a file")
	}
	return stat.Size(), nil
}

func md5sum(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func lastModified(path string) (t time.Time, retError error) {
	defer func() {
		if os.IsNotExist(retError) {
			retError = nil
		}
	}()
	fd, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return time.Time{}, err
	}
	defer fd.Close()
	if err := fd.Sync(); err != nil {
		return time.Time{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}

func rewriteContextTimeout(err, retErr error) error {
	if err == context.DeadlineExceeded {
		return retErr
	}
	return err
}
