// Package db exposes a lightweight abstraction over the SQLite code.
// It performs some basic mapping of lower-level types to rqlite types.
package db

import (
	"context"
	"database/sql"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db/humanize"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/internal/rsum"
)

const (
	// SQLiteHeaderSize is the number of leading bytes of a SQLite file that
	// identify it as a SQLite database.
	SQLiteHeaderSize = 32

	// OptimizeDefault is the PRAGMA optimize mask used for routine optimization.
	OptimizeDefault = 0xFFFE

	// OptimizeAll is the PRAGMA optimize mask which also runs ANALYZE on all
	// tables, used the first time a database is optimized.
	OptimizeAll = 0x10002

	bkDelay               = 50 * time.Millisecond
	checkpointBusyTimeout = 250 * time.Millisecond
	durToOpenLog          = 2 * time.Second
)

const (
	openDuration                 = "open_duration_ms"
	numCheckpoints               = "checkpoints"
	numCheckpointErrors          = "checkpoint_errors"
	numCheckpointedMoves         = "checkpointed_moves"
	checkpointDuration           = "checkpoint_duration_ms"
	createCompactedWALDuration   = "create_compacted_wal_duration_ms"
	preCompactWALSize            = "precompact_wal_size"
	compactedWALSize             = "compacted_wal_size"
	numCheckpointTotal           = "checkpoint_total"
	numCheckpointBusyErrors      = "checkpoint_busy_errors"
	numCheckpointInvariantErrors = "checkpoint_invariant_errors"
	numCheckpointPartial         = "checkpoint_partial"
	numCheckpointWALTruncated    = "checkpoint_wal_truncated"
	numExecutions                = "executions"
	numExecutionErrors           = "execution_errors"
	numExecutionsForceQueries    = "executions_force_queries"
	numQueries                   = "queries"
	numQueryErrors               = "query_errors"
	numRequests                  = "requests"
	numETx                       = "execute_transactions"
	numQTx                       = "query_transactions"
	numRTx                       = "request_transactions"
	numBackupStepErrors          = "backup_step_errors"
	numBackupStepDones           = "backup_step_dones"
	numBackupSleeps              = "backup_sleeps"
	numPreupdates                = "preupdates"
	numPreupdatesErrors          = "preupdates_errors"
	numPreupdatesCBErrors        = "preupdates_callback_errors"
	numUpdateHooks               = "update_hooks"
	numUpdateHooksCBErrors       = "update_hooks_callback_errors"
	numUpdateHooksErrors         = "update_hooks_errors"
	numCommitHooks               = "commit_hooks"
	cdcDroppedEvents             = "dropped_cdc_events"
)

var (
	// ErrQueryTimeout is returned when a query times out.
	ErrQueryTimeout = errors.New("query timeout")

	// ErrExecuteTimeout is returned when an execute times out.
	ErrExecuteTimeout = errors.New("execute timeout")

	// ErrQueryWrite is returned when an attempt is made to change the database
	// via a query.
	ErrQueryWrite = errors.New("attempt to change database via query operation")
)

// CheckpointMode is the mode in which a checkpoint runs. The
// numbers correspond to the values used in the SQLite PRAGMA
// wal_checkpoint command simply for clarity and ease of use.
type CheckpointMode int

const (
	// CheckpointRestart instructs the checkpoint to run in restart mode.
	CheckpointRestart CheckpointMode = 2
	// CheckpointTruncate instructs the checkpoint to run in truncate mode.
	CheckpointTruncate CheckpointMode = 3
)

var (
	checkpointPRAGMAs = map[CheckpointMode]string{
		CheckpointRestart:  "PRAGMA wal_checkpoint(RESTART)",
		CheckpointTruncate: "PRAGMA wal_checkpoint(TRUNCATE)",
	}
)

// IntegrityCheckMode selects between PRAGMA quick_check and PRAGMA integrity_check.
type IntegrityCheckMode int

const (
	// IntegrityCheckQuick selects PRAGMA quick_check: faster, but skips
	// UNIQUE constraint and index content verification.
	IntegrityCheckQuick IntegrityCheckMode = iota

	// IntegrityCheckFull selects PRAGMA integrity_check, the more thorough check.
	IntegrityCheckFull
)

// IntegrityResult is the outcome of a single integrity check.
//
// OK is true iff SQLite returned its sentinel "ok" row. When OK is false,
// Issues holds the per-problem descriptions SQLite reported, in order,
// up to the maxIssues bound passed to IntegrityCheck.
type IntegrityResult struct {
	OK     bool
	Issues []string
}

// DBVersion is the SQLite version.
var DBVersion string

// stats captures stats for the DB layer.
var stats *expvar.Map

func init() {
	DBVersion, _, _ = sqlite3.Version()
	stats = expvar.NewMap("db")
	ResetStats()
}

func recordDuration(stat string, startT time.Time) {
	stats.Get(stat).(*expvar.Int).Set(time.Since(startT).Milliseconds())
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(openDuration, 0)
	stats.Add(numCheckpoints, 0)
	stats.Add(numCheckpointErrors, 0)
	stats.Add(numCheckpointedMoves, 0)
	stats.Add(checkpointDuration, 0)
	stats.Add(createCompactedWALDuration, 0)
	stats.Add(preCompactWALSize, 0)
	stats.Add(compactedWALSize, 0)
	stats.Add(numCheckpointTotal, 0)
	stats.Add(numCheckpointBusyErrors, 0)
	stats.Add(numCheckpointInvariantErrors, 0)
	stats.Add(numCheckpointPartial, 0)
	stats.Add(numCheckpointWALTruncated, 0)
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
	stats.Add(numPreupdates, 0)
	stats.Add(numPreupdatesErrors, 0)
	stats.Add(numPreupdatesCBErrors, 0)
	stats.Add(numUpdateHooks, 0)
	stats.Add(numUpdateHooksCBErrors, 0)
	stats.Add(numUpdateHooksErrors, 0)
	stats.Add(numCommitHooks, 0)
	stats.Add(cdcDroppedEvents, 0)
}

// Config describes how a database is opened: one connection policy for the
// writer, one for the readers, and the settings they share.
type Config struct {
	// Writer is the policy for the read-write connection.
	Writer ConnConfig

	// Readers is the policy for the read-only connections.
	Readers ConnConfig

	// WAL, if true, opens the database in WAL journal mode, otherwise in
	// DELETE mode.
	WAL bool

	// MaxReaders is the maximum number of read-only connections. Zero means
	// no limit.
	MaxReaders int
}

// NewConfig returns a configuration which opens the writer and the readers
// with the same connection policy, except that the readers cannot attach
// other database files.
func NewConfig(cfg ConnConfig, wal bool) *Config {
	readerCfg := cfg
	readerCfg.DisableAttach = true
	return &Config{
		Writer:  cfg,
		Readers: readerCfg,
		WAL:     wal,
	}
}

// clone copies the configuration and its extension paths. Hook targets and
// the query logger remain shared with the caller.
func (cfg Config) clone() *Config {
	cfg.Writer.Extensions = slices.Clone(cfg.Writer.Extensions)
	cfg.Readers.Extensions = slices.Clone(cfg.Readers.Extensions)
	return &cfg
}

// DB is the SQL database.
type DB struct {
	cfg     *Config
	path    string // Path to database file.
	walPath string // Path to WAL file.

	writer  *writer     // The one read-write connection.
	readers *readerPool // The read-only connections.

	allOptimized   bool
	allOptimizedMu sync.Mutex

	logger *log.Logger
}

// PoolStats represents connection pool statistics.
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

// CheckpointMeta contains metadata about a WAL checkpoint operation.
type CheckpointMeta struct {
	// Code is the return code for the operation. Zero if successful.
	Code int

	// Pages is the total number of frames in the WAL.
	Pages int

	// Moved is the total number of checkpointed frames in the log file
	// (including any that were already checkpointed before Checkpoint
	// was called).
	Moved int
}

// String returns a string representation of the CheckpointMeta.
func (cm *CheckpointMeta) String() string {
	return fmt.Sprintf("Code=%d, Pages=%d, Moved=%d", cm.Code, cm.Pages, cm.Moved)
}

// Success returns true if the checkpoint completed successfully.
func (cm *CheckpointMeta) Success() bool {
	return cm.Code == 0
}

// Open opens a file-based database using the default connection policy.
func Open(dbPath string, fkEnabled, wal bool) (retDB *DB, retErr error) {
	return OpenWithConfig(dbPath, NewConfig(ConnConfig{ForeignKeys: fkEnabled}, wal))
}

// OpenWithConfig opens a file-based database, creating it if it does not exist.
// After this function returns, an actual SQLite file will always exist. If the
// database is opened in WAL mode, the WAL files will also be created if they
// do not exist. The configuration and its extension paths are copied on open.
func OpenWithConfig(dbPath string, cfg *Config) (retDB *DB, retErr error) {
	cfg = cfg.clone()
	logger := log.New(log.Writer(), "[db] ", log.LstdFlags)
	startTime := time.Now()
	defer func() {
		if retErr != nil {
			return
		}
		if dur := time.Since(startTime); dur > durToOpenLog {
			logger.Printf("opened database %s in %s", dbPath, dur)
		}
		recordDuration(openDuration, startTime)
	}()

	writer, err := openWriter(&cfg.Writer, dbPath, cfg.WAL, logger)
	if err != nil {
		return nil, err
	}
	readers := openReaderPool(&cfg.Readers, dbPath, cfg.WAL, logger)
	readers.setMaxConns(cfg.MaxReaders)

	// Make it clear which extensions have been loaded, if any.
	extensions := extensionNames(cfg.Writer.Extensions)
	if len(extensions) > 0 {
		logger.Printf("loaded extensions: %s", strings.Join(extensions, ", "))
	}

	return &DB{
		cfg:     cfg,
		path:    dbPath,
		walPath: dbPath + "-wal",
		writer:  writer,
		readers: readers,
		logger:  logger,
	}, nil
}

// SetMaxReadOnlyConns sets the maximum number of read-only connections to the
// database. See https://pkg.go.dev/database/sql#DB.SetMaxIdleConns for full
// details.
func (db *DB) SetMaxReadOnlyConns(n int) {
	db.readers.setMaxConns(n)
}

// AcquireWriter borrows the read-write connection. There is only one, so
// the call waits until it is free or ctx is done, and while it is held
// nothing else can write to the database. The caller must close it.
func (db *DB) AcquireWriter(ctx context.Context) (*Conn, error) {
	return db.writer.acquire(ctx)
}

// AcquireReader borrows a read-only connection. The caller must close it.
func (db *DB) AcquireReader(ctx context.Context) (*Conn, error) {
	return db.readers.acquire(ctx)
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

// DBLastModified returns the last modified time of the main database
// file.
func (db *DB) DBLastModified() (time.Time, error) {
	return fsutil.LastModified(db.path)
}

// DBSum returns the MD5 checksum of the database file.
func (db *DB) DBSum() (string, error) {
	return rsum.MD5(db.path)
}

// WALLastModified returns the last modified time of the WAL file.
func (db *DB) WALLastModified() (time.Time, error) {
	return fsutil.LastModified(db.walPath)
}

// WALSum returns the MD5 checksum of the WAL file.
func (db *DB) WALSum() (string, error) {
	return rsum.MD5(db.walPath)
}

// Close closes the underlying database connections. Close the read-only
// connections first so if there is any clean-up -- perhaps a checkpoint
// on close -- it can be done by the read-write connection.
func (db *DB) Close() error {
	if err := db.readers.close(); err != nil {
		return err
	}
	return db.writer.close()
}

// Stats returns status and diagnostics for the database.
func (db *DB) Stats() (map[string]any, error) {
	copts, err := db.CompileOptions()
	if err != nil {
		return nil, err
	}
	memStats, err := db.memStats()
	if err != nil {
		return nil, err
	}
	connPoolStats := map[string]any{
		"ro": db.readers.poolStats(),
		"rw": db.writer.poolStats(),
	}
	dbSz, err := db.Size()
	if err != nil {
		return nil, err
	}
	pragmas, err := db.pragmas()
	if err != nil {
		return nil, err
	}
	txStatus, err := db.txStatus()
	if err != nil {
		return nil, err
	}
	stats := map[string]any{
		"extensions":       db.ExtensionNames(),
		"version":          DBVersion,
		"compile_options":  copts,
		"mem_stats":        memStats,
		"db_size":          dbSz,
		"db_size_friendly": humanize.Bytes(uint64(dbSz)),
		"rw_dsn":           db.writer.dsn,
		"ro_dsn":           db.readers.dsn,
		"conn_pool_stats":  connPoolStats,
		"pragmas":          pragmas,
		"transaction":      txStatus,
	}

	lm, err := db.LastModified()
	if err == nil {
		stats["last_modified"] = lm
	}

	stats["path"] = db.path
	if stats["size"], err = db.FileSize(); err != nil {
		return nil, err
	}
	if db.cfg.WAL {
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
	if len(rows) < 1 {
		return 0, fmt.Errorf("no rows returned")
	}
	if rows[0].Error != "" {
		return 0, errors.New(rows[0].Error)
	}
	return rows[0].Values[0].Parameters[0].GetI(), nil
}

// FileSize returns the size of the SQLite file on disk.
func (db *DB) FileSize() (int64, error) {
	return fsutil.FileSize(db.path)
}

// WALSize returns the size of the SQLite WAL file on disk. If WAL mode is
// not enabled, this function returns 0. If the WAL file does
// not exist, even if the database is in WAL mode, 0 is also returned.
func (db *DB) WALSize() (int64, error) {
	if !db.cfg.WAL {
		return 0, nil
	}
	sz, err := fsutil.FileSize(db.walPath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return 0, err
		}
		sz = 0
		err = nil
	}
	return sz, err
}

// ExtensionNames returns the names of the SQLite extensions loaded into the database.
func (db *DB) ExtensionNames() []string {
	return extensionNames(db.cfg.Writer.Extensions)
}

// SetBusyTimeout sets the busy timeout for the database. If a timeout is
// less than zero it is not set.
func (db *DB) SetBusyTimeout(rwMs, roMs int) (err error) {
	if rwMs >= 0 {
		if err := db.writer.setBusyTimeout(rwMs); err != nil {
			return err
		}
	}
	if roMs >= 0 {
		if err := db.readers.setBusyTimeout(roMs); err != nil {
			return err
		}
	}
	return nil
}

// RWBusyTimeout returns the current busy timeout value for the read-write
// database connection.
func (db *DB) RWBusyTimeout() (int, error) {
	return db.writer.busyTimeout()
}

// ROBusyTimeout returns the current busy timeout value for the read-only
// database connection.
func (db *DB) ROBusyTimeout() (int, error) {
	return db.readers.busyTimeout()
}

// BusyTimeout returns the current busy timeout values for the read-write and
// read-only database connections.
func (db *DB) BusyTimeout() (rwMs, roMs int, err error) {
	rwMs, err = db.RWBusyTimeout()
	if err != nil {
		return 0, 0, err
	}
	roMs, err = db.ROBusyTimeout()
	if err != nil {
		return 0, 0, err
	}
	return rwMs, roMs, nil
}

// Checkpoint checkpoints the WAL file. If WAL mode is not enabled, this
// function is a no-op.
//
// This operation will temporarily set the database synchronous mode to FULL
// for the duration of the checkpoint, restoring it to its previous value afterwards.
func (db *DB) Checkpoint(mode CheckpointMode) (*CheckpointMeta, error) {
	return db.CheckpointWithTimeout(mode, checkpointBusyTimeout)
}

// CheckpointTruncateWithTimeout performs a WAL checkpoint in TRUNCATE mode.
// The caller must guarantee that no write transactions will commit for the
// duration of the call. If all readers release their locks within dur,
// the WAL is truncated and nil is returned. If readers hold locks for
// the entire duration, an error is returned.
func (db *DB) CheckpointTruncateWithTimeout(dur time.Duration) (err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			stats.Add(numCheckpointErrors, 1)
		} else {
			recordDuration(checkpointDuration, start)
			stats.Add(numCheckpoints, 1)
		}
	}()

	conn, err := db.AcquireWriter(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	rwBt, err := conn.BusyTimeout()
	if err != nil {
		return fmt.Errorf("failed to get busy_timeout: %s", err.Error())
	}
	if err := conn.SetBusyTimeout(int(dur.Milliseconds())); err != nil {
		return fmt.Errorf("failed to set busy_timeout: %s", err.Error())
	}
	defer func() {
		if err := conn.SetBusyTimeout(rwBt); err != nil {
			db.logger.Printf("failed to reset busy_timeout: %s", err.Error())
		}
	}()

	currMode, err := conn.SynchronousMode()
	if err != nil {
		return fmt.Errorf("failed to get synchronous mode: %s", err.Error())
	}
	if err := conn.SetSynchronousMode(SynchronousFull); err != nil {
		return fmt.Errorf("failed to set synchronous mode to FULL: %s", err.Error())
	}
	defer func() {
		if err := conn.SetSynchronousMode(currMode); err != nil {
			db.logger.Fatalf("failed to reset synchronous mode to %s: %s", currMode, err.Error())
		}
	}()

	meta, err := conn.Checkpoint(CheckpointTruncate)
	if err != nil {
		return fmt.Errorf("error checkpointing WAL: %s", err.Error())
	}
	if !meta.Success() {
		return fmt.Errorf("checkpoint did not complete within %s", dur)
	}
	stats.Add(numCheckpointedMoves, int64(meta.Moved))
	return nil
}

// CheckpointWithTimeout performs a WAL checkpoint. If the checkpoint does not
// run to completion within the given duration, an error is returned. If the
// duration is 0, the busy timeout is not modified before executing the
// checkpoint.
//
// This operation will temporarily set the database synchronous mode to FULL for the
// duration of the checkpoint, restoring it to its previous value afterwards.
func (db *DB) CheckpointWithTimeout(mode CheckpointMode, dur time.Duration) (meta *CheckpointMeta, err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			stats.Add(numCheckpointErrors, 1)
		} else {
			recordDuration(checkpointDuration, start)
			stats.Add(numCheckpoints, 1)
		}
	}()

	conn, err := db.AcquireWriter(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if dur > 0 {
		rwBt, err := conn.BusyTimeout()
		if err != nil {
			return nil, fmt.Errorf("failed to get busy_timeout on checkpointing connection: %s", err.Error())
		}
		if err := conn.SetBusyTimeout(int(dur.Milliseconds())); err != nil {
			return nil, fmt.Errorf("failed to set busy_timeout on checkpointing connection: %s", err.Error())
		}
		defer func() {
			// Reset back to default
			if err := conn.SetBusyTimeout(rwBt); err != nil {
				db.logger.Printf("failed to reset busy_timeout on checkpointing connection: %s", err.Error())
			}
		}()
	}

	// Temporarily move to Synchronous=FULL for the duration of the checkpoint.
	currMode, err := conn.SynchronousMode()
	if err != nil {
		return nil, fmt.Errorf("failed to get current synchronous mode: %s", err.Error())
	}
	if err := conn.SetSynchronousMode(SynchronousFull); err != nil {
		return nil, fmt.Errorf("failed to set synchronous mode to FULL: %s", err.Error())
	}
	defer func() {
		if err := conn.SetSynchronousMode(currMode); err != nil {
			db.logger.Fatalf("failed to reset synchronous mode to %s: %s", currMode, err.Error())
		}
	}()

	meta, err = conn.Checkpoint(mode)
	if err != nil {
		return nil, fmt.Errorf("error checkpointing WAL: %s", err.Error())
	}
	stats.Add(numCheckpointedMoves, int64(meta.Moved))
	return meta, nil
}

// GetCheckpointing returns the current checkpointing setting.
func (db *DB) GetCheckpointing() (int, error) {
	conn, err := db.AcquireWriter(context.Background())
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return conn.AutoCheckpoint()
}

// Optimize runs a default PRAGMA OPTIMIZE on the database. If it's the first
// time this function is called on this database, it will run a full optimization.
func (db *DB) Optimize() error {
	db.allOptimizedMu.Lock()
	defer db.allOptimizedMu.Unlock()
	if !db.allOptimized {
		db.allOptimized = true
		return db.OptimizeWithMask(OptimizeAll)
	}
	return db.OptimizeWithMask(OptimizeDefault)
}

// OptimizeWithMask runs a PRAGMA OPTIMIZE on the database, using the given mask.
func (db *DB) OptimizeWithMask(mask int) error {
	return db.execOnWriter(fmt.Sprintf("PRAGMA optimize=0x%x", mask))
}

// Vacuum runs a VACUUM on the database.
func (db *DB) Vacuum() error {
	return db.execOnWriter("VACUUM")
}

// VacuumInto VACUUMs the database into the file at path.
func (db *DB) VacuumInto(path string) error {
	return db.execOnWriter(fmt.Sprintf("VACUUM INTO '%s'", strings.ReplaceAll(path, "'", "''")))
}

// execOnWriter runs a statement that returns no rows on the read-write
// connection.
func (db *DB) execOnWriter(query string) error {
	conn, err := db.AcquireWriter(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.exec(context.Background(), query)
}

// IntegrityCheck runs PRAGMA integrity_check (IntegrityCheckFull) or
// PRAGMA quick_check (IntegrityCheckQuick) on the database. maxIssues
// caps how many problem rows SQLite is asked to emit before it stops;
// pass 0 to use SQLite's default of 100.
//
// A non-nil error is returned only for execution failures (driver, I/O,
// etc.). A corrupt database is a successful call with OK == false and
// Issues populated.
func (db *DB) IntegrityCheck(mode IntegrityCheckMode, maxIssues int) (IntegrityResult, error) {
	pragma := "quick_check"
	if mode == IntegrityCheckFull {
		pragma = "integrity_check"
	}
	stmt := "PRAGMA " + pragma
	if maxIssues > 0 {
		stmt = fmt.Sprintf("PRAGMA %s(%d)", pragma, maxIssues)
	}

	rows, err := db.QueryStringStmt(stmt)
	if err != nil {
		return IntegrityResult{}, err
	}
	if len(rows) != 1 {
		return IntegrityResult{}, fmt.Errorf("integrity check returned %d result sets, want 1", len(rows))
	}
	r := rows[0]
	if r.Error != "" {
		return IntegrityResult{}, fmt.Errorf("integrity check: %s", r.Error)
	}
	if len(r.Values) == 1 && len(r.Values[0].Parameters) == 1 &&
		r.Values[0].Parameters[0].GetS() == "ok" {
		return IntegrityResult{OK: true}, nil
	}
	issues := make([]string, 0, len(r.Values))
	for _, v := range r.Values {
		if len(v.Parameters) == 0 {
			continue
		}
		issues = append(issues, v.Parameters[0].GetS())
	}
	return IntegrityResult{OK: false, Issues: issues}, nil
}

// VerifyIntegrity runs a PRAGMA quick_check that stops after the first problem.
// It is shorthand for IntegrityCheck(IntegrityCheckQuick, 1) for callers that only
// need a yes/no answer.
//
// If IntegrityResult.OK is false and err is nil this function guarantees
// that res.Issues will contain exactly 1 issue.
func (db *DB) VerifyIntegrity() (IntegrityResult, error) {
	res, err := db.IntegrityCheck(IntegrityCheckQuick, 1)
	if err != nil {
		return IntegrityResult{}, err
	}
	if !res.OK && len(res.Issues) != 1 {
		return IntegrityResult{}, fmt.Errorf("verification failed, but no issue available")
	}
	return res, nil
}

// SetSynchronousMode sets the synchronous mode of the database.
func (db *DB) SetSynchronousMode(mode SynchronousMode) error {
	conn, err := db.AcquireWriter(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.SetSynchronousMode(mode)
}

// GetSynchronousMode returns the current synchronous mode.
func (db *DB) GetSynchronousMode() (SynchronousMode, error) {
	conn, err := db.AcquireWriter(context.Background())
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return conn.SynchronousMode()
}

// FKEnabled returns whether Foreign Key constraints are enabled.
func (db *DB) FKEnabled() bool {
	return db.cfg.Writer.ForeignKeys
}

// WALEnabled returns whether WAL mode is enabled.
func (db *DB) WALEnabled() bool {
	return db.cfg.WAL
}

// Path returns the path of this database.
func (db *DB) Path() string {
	return db.path
}

// WALPath returns the path to the WAL file for this database.
func (db *DB) WALPath() string {
	if !db.cfg.WAL {
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
			return nil, fmt.Errorf("compile options values wrong size (%d)", len(res[0].Values[i].Parameters))
		}
		compileOptions[i] = res[0].Values[i].Parameters[0].GetS()
	}
	return compileOptions, nil
}

// ColumnNames returns the names of all columns in the given table.
func (db *DB) ColumnNames(table string) ([]string, error) {
	conn, err := db.AcquireReader(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	rows, err := conn.queryStmt(context.Background(), &command.Statement{
		Sql: fmt.Sprintf(`SELECT * FROM "%s" LIMIT 0`, strings.ReplaceAll(table, `"`, `""`)),
	}, false, conn.conn)
	if err != nil {
		return nil, err
	}
	return rows.Columns, nil
}

// TableColumnTypes returns the declared types of all columns in the given table.
func (db *DB) TableColumnTypes(table string) (map[string]string, error) {
	conn, err := db.AcquireReader(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	rows, err := conn.queryStmt(context.Background(), &command.Statement{
		Sql: fmt.Sprintf("PRAGMA table_info(\"%s\")", strings.ReplaceAll(table, `"`, `""`)),
	}, false, conn.conn)
	if err != nil {
		return nil, err
	}
	if len(rows.Values) < 1 {
		return nil, fmt.Errorf("no such table: %s", table)
	}
	colTypes := make(map[string]string, len(rows.Values))
	for _, v := range rows.Values {
		if len(v.Parameters) < 3 {
			return nil, fmt.Errorf("unexpected result from PRAGMA table_info")
		}
		colTypes[v.Parameters[1].GetS()] = v.Parameters[2].GetS()
	}
	return colTypes, nil
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
	return db.ExecuteWithContext(context.Background(), req, xTime)
}

// ExecuteWithContext executes queries that modify the database, using the given context.
// Any timeout set in the request is also applied, so the effective deadline is the
// earlier of the context's deadline and the request's timeout.
func (db *DB) ExecuteWithContext(ctx context.Context, req *command.Request, xTime bool) (results []*command.ExecuteQueryResponse, retErr error) {
	defer func() { retErr = classifyError(retErr) }()

	// The connection applies the request timeout too, but only once it has
	// been acquired. Applying it here also bounds the wait for the writer.
	if req.DbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.DbTimeout))
		defer cancel()
	}
	stats.Add(numExecutions, int64(len(req.Statements)))
	conn, err := db.AcquireWriter(ctx)
	if err != nil {
		return nil, err
	}
	// Close the connection, preserving any fatal error.
	defer func() { preserveFatalError(&retErr, conn.Close()) }()
	return conn.Execute(ctx, req, xTime)
}

// QueryStringStmt executes a single query that returns rows, but doesn't modify the database.
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

// QueryStringStmtWithTimeout executes a single query that returns rows, but doesn't modify the database.
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
	return db.QueryWithContext(context.Background(), req, xTime)
}

// QueryWithContext executes queries that return rows, but don't modify the database.
// Any timeout set in the request is also applied, so the effective deadline is the
// earlier of the context's deadline and the request's timeout.
func (db *DB) QueryWithContext(ctx context.Context, req *command.Request, xTime bool) ([]*command.QueryRows, error) {
	// The connection applies the request timeout too, but only once it has
	// been acquired. Applying it here also bounds the wait for a reader.
	if req.DbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.DbTimeout))
		defer cancel()
	}
	stats.Add(numQueries, int64(len(req.Statements)))
	conn, err := db.AcquireReader(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.Query(ctx, req, xTime)
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
// Node-local SQLite failures return a *FatalError and stop the request.
func (db *DB) Request(req *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, error) {
	return db.RequestWithContext(context.Background(), req, xTime)
}

// RequestWithContext processes a request that can contain both executes and queries,
// using the given context. Any timeout set in the request is also applied, so the
// effective deadline is the earlier of the context's deadline and the request's timeout.
func (db *DB) RequestWithContext(ctx context.Context, req *command.Request, xTime bool) (eqResponse []*command.ExecuteQueryResponse, retErr error) {
	defer func() { retErr = classifyError(retErr) }()
	// The connection applies the request timeout too, but only once it has
	// been acquired. Applying it here also bounds the wait for the writer.
	if req.DbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.DbTimeout))
		defer cancel()
	}
	stats.Add(numRequests, int64(len(req.Statements)))
	conn, err := db.AcquireWriter(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { preserveFatalError(&retErr, conn.Close()) }()
	return conn.Request(ctx, req, xTime)
}

// Backup writes a consistent snapshot of the database to the given file.
// The resultant SQLite database file will be in DELETE mode. This function
// can be called when changes to the database are in flight.
//
// The function returns the size of the file at path once the Backup completes
// regardless of whether an error is returned or not.
func (db *DB) Backup(path string, vacuum bool) (sz int64, retErr error) {
	defer func() {
		n, err := fsutil.FileSizeExists(path)
		sz = n
		if err != nil {
			retErr = err
		}
	}()

	var dstDB *DB
	dstDB, retErr = Open(path, false, false)
	if retErr != nil {
		return
	}
	defer dstDB.Close()

	if retErr = dstDB.copyFrom(db); retErr != nil {
		return
	}

	// Source database might be in WAL mode.
	_, retErr = dstDB.ExecuteStringStmt("PRAGMA journal_mode=DELETE")
	if retErr != nil {
		return
	}

	if vacuum {
		if retErr = dstDB.Vacuum(); retErr != nil {
			return
		}
	}
	retErr = dstDB.Close()
	return
}

// Copy copies the contents of the database to the given database. All other
// attributes of the given database remain untouched e.g. whether it's an
// on-disk database, except the database will be placed in DELETE mode.
// This function can be called when changes to the source database are in flight.
func (db *DB) Copy(dstDB *DB) error {
	if err := dstDB.copyFrom(db); err != nil {
		return fmt.Errorf("copy database: %s", err)
	}
	_, err := dstDB.ExecuteStringStmt("PRAGMA journal_mode=DELETE")
	return err
}

// copyFrom copies the contents of src into this database, reading from one
// of src's read-only connections and writing through this database's
// read-write connection.
func (db *DB) copyFrom(src *DB) error {
	dst, err := db.AcquireWriter(context.Background())
	if err != nil {
		return err
	}
	defer dst.Close()
	srcConn, err := src.AcquireReader(context.Background())
	if err != nil {
		return err
	}
	defer srcConn.Close()
	return dst.CopyFrom(srcConn)
}

// Serialize returns a byte slice representation of the SQLite database. For
// an ordinary on-disk database file, the serialization is just a copy of the
// disk file. If the database is in WAL mode, a temporary on-disk
// copy is made, and it is this copy that is serialized. This function must not
// be called while any writes are happening to the database.
func (db *DB) Serialize() ([]byte, error) {
	if db.cfg.WAL {
		tmpFile, err := os.CreateTemp("", "rqlite-serialize")
		if err != nil {
			return nil, err
		}
		defer fsutil.Remove(tmpFile.Name())
		defer tmpFile.Close()

		if _, err := db.Backup(tmpFile.Name(), false); err != nil {
			return nil, err
		}
		newDB, err := Open(tmpFile.Name(), db.FKEnabled(), false)
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
// It returns the number of bytes written to w, even when it returns an error.
//
// When table names are given, the dump carries those tables and their data, plus
// the indexes and triggers that belong to them. Views are written out as they
// are, filtered or not, so a view that reads from a table the dump does not
// carry is created but cannot be queried. The dump itself still loads, since
// SQLite resolves the tables a view reads from when the view is used, not when it
// is created.
func (db *DB) Dump(w io.Writer, tableNames ...string) (int, error) {
	conn, err := db.AcquireReader(context.Background())
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return conn.Dump(w, tableNames...)
}

// StmtReadOnly returns whether the given SQL statement is read-only.
// As per https://www.sqlite.org/c3ref/stmt_readonly.html, this function
// may not return 100% correct results, but should cover most scenarios.
func (db *DB) StmtReadOnly(sql string) (bool, error) {
	conn, err := db.AcquireReader(context.Background())
	if err != nil {
		return false, err
	}
	defer conn.Close()
	return conn.StmtReadOnly(sql)
}

func (db *DB) pragmas() (map[string]any, error) {
	pragmaNames := []string{
		"synchronous",
		"journal_mode",
		"foreign_keys",
		"wal_autocheckpoint",
		"busy_timeout",
	}

	rw, err := db.writer.pragmas(pragmaNames)
	if err != nil {
		return nil, err
	}
	ro, err := db.readers.pragmas(pragmaNames)
	if err != nil {
		return nil, err
	}
	return map[string]any{"rw": rw, "ro": ro}, nil
}

// txStatus returns whether there is an active transaction on each database
// connection.
func (db *DB) txStatus() (map[string]any, error) {
	rw, err := db.writer.inTransaction()
	if err != nil {
		return nil, err
	}
	ro, err := db.readers.inTransaction()
	if err != nil {
		return nil, err
	}
	return map[string]any{"rw": rw, "ro": ro}, nil
}

func (db *DB) memStats() (map[string]int64, error) {
	ms := make(map[string]int64)
	// Batch all PRAGMA queries into a single request to avoid acquiring
	// a separate connection for each PRAGMA.
	pragmas := []string{
		"max_page_count",
		"page_count",
		"page_size",
		"hard_heap_limit",
		"soft_heap_limit",
		"cache_size",
		"freelist_count",
	}
	stmts := make([]*command.Statement, len(pragmas))
	for i, p := range pragmas {
		stmts[i] = &command.Statement{
			Sql: fmt.Sprintf("PRAGMA %s", p),
		}
	}
	req := &command.Request{Statements: stmts}
	res, err := db.Query(req, false)
	if err != nil {
		return nil, err
	}
	for i, p := range pragmas {
		if res[i].Error != "" {
			return nil, errors.New(res[i].Error)
		}
		ms[p] = res[i].Values[0].Parameters[0].GetI()
	}
	return ms, nil
}

// schemaObject is an index, a trigger or a view held by the database schema.
type schemaObject struct {
	name    string
	typ     string
	tblName string
	sql     string
}

// schemaObjects is a set of schema objects held by the database.
type schemaObjects []schemaObject

// Filter returns the objects that belong to the given tables: an index or a
// trigger is kept when the table it belongs to is one of them, and views are
// always kept, since a view says nothing about the objects it reads from. See
// Dump for what that means for a filtered dump. With no table names every object
// is kept.
func (s schemaObjects) Filter(tables []string) schemaObjects {
	if len(tables) == 0 {
		return s
	}

	selected := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		selected[strings.ToLower(t)] = struct{}{}
	}

	filtered := make(schemaObjects, 0, len(s))
	for _, o := range s {
		if o.typ != "view" {
			if _, ok := selected[strings.ToLower(o.tblName)]; !ok {
				continue
			}
		}
		filtered = append(filtered, o)
	}
	return filtered
}

func createEQQueryResponse(rows *command.QueryRows, err error) *command.ExecuteQueryResponse {
	if err != nil {
		return &command.ExecuteQueryResponse{
			Result: &command.ExecuteQueryResponse_Q{
				Q: newQueryRowsError(err),
			},
		}
	}
	return &command.ExecuteQueryResponse{
		Result: &command.ExecuteQueryResponse_Q{
			Q: rows,
		},
	}
}

// parametersToValues maps values in the proto params to SQL driver values.
func parametersToValues(parameters []*command.Parameter) ([]any, error) {
	if parameters == nil {
		return nil, nil
	}

	values := make([]any, len(parameters))
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
			return nil, fmt.Errorf("unsupported type %T for parameter named: %s", w, parameters[i].GetName())
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
			case nil:
				types[i] = ""
			default:
				return fmt.Errorf("unsupported type: %T", params[i].GetValue())
			}
		}
	}
	return nil
}

// normalizeRowParameters performs some normalization of values in the returned rows.
// Text values come over (from sqlite-go) as []byte instead of strings
// for some reason, so we explicitly convert them (but only when the type
// is "text", so we don't affect BLOB types).
func normalizeRowParameters(row []any, types []string) ([]*command.Parameter, error) {
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

func normalizeCDCValues(row []any) (*command.CDCRow, error) {
	cdcRow := &command.CDCRow{
		Values: make([]*command.CDCValue, len(row)),
	}
	for i, v := range row {
		switch val := v.(type) {
		case int:
			cdcRow.Values[i] = &command.CDCValue{
				Value: &command.CDCValue_I{
					I: int64(val)},
			}
		case int64:
			cdcRow.Values[i] = &command.CDCValue{
				Value: &command.CDCValue_I{
					I: val,
				},
			}
		case float64:
			cdcRow.Values[i] = &command.CDCValue{
				Value: &command.CDCValue_D{
					D: val,
				},
			}
		case bool:
			cdcRow.Values[i] = &command.CDCValue{
				Value: &command.CDCValue_B{
					B: val,
				},
			}
		case string:
			cdcRow.Values[i] = &command.CDCValue{
				Value: &command.CDCValue_S{
					S: val,
				},
			}
		case []byte:
			cdcRow.Values[i] = &command.CDCValue{
				Value: &command.CDCValue_Y{
					Y: val,
				},
			}
		case time.Time:
			rfc3339, err := val.MarshalText()
			if err != nil {
				return nil, err
			}
			cdcRow.Values[i] = &command.CDCValue{
				Value: &command.CDCValue_S{
					S: string(rfc3339),
				},
			}
		case nil:
			cdcRow.Values[i] = &command.CDCValue{
				Value: nil,
			}
		default:
			return nil, fmt.Errorf("unhandled column type: %T %v", val, val)
		}
	}
	return cdcRow, nil
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
	return slices.Contains(slice, "")
}

func rewriteContextTimeout(err, retErr error) error {
	if err == context.DeadlineExceeded {
		return retErr
	}
	return err
}
