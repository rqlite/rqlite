package db

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// SwappableDB is a wrapper around DB that allows the underlying database to be swapped out
// in a thread-safe manner.
type SwappableDB struct {
	db            *DB
	drv           *Driver
	checkpointMgr *CheckpointManager
	dbMu          sync.RWMutex
}

// OpenSwappable returns a new SwappableDB instance, which opens the database at the given path,
// using the given driver. If drv is nil then the default driver is used. If fkEnabled is true,
// foreign key constraints are enabled. If wal is true, the WAL journal mode is enabled.
func OpenSwappable(dbPath string, drv *Driver, fkEnabled, wal bool, maxROConns int) (*SwappableDB, error) {
	if drv == nil {
		drv = DefaultDriver()
	}
	db, err := OpenWithDriver(drv, dbPath, fkEnabled, wal)
	if err != nil {
		return nil, err
	}
	db.SetMaxReadOnlyConns(maxROConns)

	mgr, err := NewCheckpointManager(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create checkpoint manager: %s", err)
	}
	return &SwappableDB{
		db:            db,
		drv:           drv,
		checkpointMgr: mgr,
	}, nil
}

// Swap swaps the underlying database with that at the given path. The Swap operation
// may fail on some platforms if the file at path is open by another process. It is
// the caller's responsibility to ensure the file at path is not in use.
//
// The existing database files are moved aside rather than deleted, and are removed
// only once the incoming database has been opened. If any step fails, the original
// files are moved back into place.
func (s *SwappableDB) Swap(path string, fkConstraints, walEnabled bool) error {
	if !IsValidSQLiteFile(path) {
		return fmt.Errorf("invalid SQLite data")
	}

	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close: %s", err)
	}

	dbPath := s.db.Path()
	asidePath := swapAsidePath(dbPath)
	if err := moveFilesAside(dbPath, asidePath); err != nil {
		return fmt.Errorf("failed to move current database files aside: %s", err)
	}
	restoreOnExit := true
	defer func() {
		if restoreOnExit {
			restoreFiles(asidePath, dbPath) // best effort, no database is open
		}
	}()

	if err := os.Rename(path, dbPath); err != nil {
		return fmt.Errorf("failed to rename database: %s", err)
	}
	if err := fsutil.SyncDirMaybe(filepath.Dir(dbPath)); err != nil {
		return fmt.Errorf("failed to sync data directory: %s", err)
	}

	db, err := OpenWithDriver(s.drv, dbPath, fkConstraints, walEnabled)
	if err != nil {
		return fmt.Errorf("open SQLite file failed: %s", err)
	}
	mgr, err := NewCheckpointManager(db)
	if err != nil {
		db.Close()
		return fmt.Errorf("failed to recreate checkpoint manager: %s", err)
	}
	s.db = db
	if err := s.checkpointMgr.Close(); err != nil {
		db.Close()
		return fmt.Errorf("failed to close checkpoint manager: %s", err)
	}
	s.checkpointMgr = mgr

	// The swap is complete. The old files are stale now, and if removing them
	// fails they are cleaned up at next startup by RecoverPendingSwap.
	restoreOnExit = false
	if err := RemoveFiles(asidePath); err != nil {
		// Ignore. Failing the swap over stale file cleanup is not worth it.
	}
	return nil
}

// swapAsidePath returns the path the database files are moved to while a swap is
// in progress.
func swapAsidePath(dbPath string) string {
	return dbPath + ".swap-old"
}

// moveFilesAside moves the SQLite database at dbPath, along with any WAL and SHM
// files, so that their names begin with asidePath. Stale files at asidePath are
// removed first. If any move fails, moves already made are rolled back.
func moveFilesAside(dbPath, asidePath string) error {
	if err := RemoveFiles(asidePath); err != nil {
		return err
	}
	var moved []string
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if !fsutil.FileExists(dbPath + suffix) {
			continue
		}
		if err := os.Rename(dbPath+suffix, asidePath+suffix); err != nil {
			for _, s := range moved {
				os.Rename(asidePath+s, dbPath+s)
			}
			return err
		}
		moved = append(moved, suffix)
	}
	return nil
}

// restoreFiles moves the files previously set aside at asidePath back to dbPath.
// Any files already at dbPath are removed first.
func restoreFiles(asidePath, dbPath string) error {
	if err := moveFilesAside(asidePath, dbPath); err != nil {
		return err
	}
	return fsutil.SyncDirMaybe(filepath.Dir(dbPath))
}

// RecoverPendingSwap cleans up state left behind by a Swap interrupted by a crash.
// If discard is true, any set-aside files are removed. Otherwise the original files
// are restored if the database file is missing, or removed as stale if it is present.
func RecoverPendingSwap(dbPath string, discard bool) error {
	asidePath := swapAsidePath(dbPath)
	if !fsutil.FileExists(asidePath) {
		return nil
	}
	if discard || fsutil.FileExists(dbPath) {
		if err := RemoveFiles(asidePath); err != nil {
			return err
		}
		return fsutil.SyncDirMaybe(filepath.Dir(dbPath))
	}
	return restoreFiles(asidePath, dbPath)
}

// Close closes the underlying database.
func (s *SwappableDB) Close() error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Close()
}

// Stats returns the underlying database's stats.
func (s *SwappableDB) Stats() (map[string]any, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Stats()
}

// Request calls Request on the underlying database.
func (s *SwappableDB) Request(req *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Request(req, xTime)
}

// RequestWithContext calls RequestWithContext on the underlying database.
func (s *SwappableDB) RequestWithContext(ctx context.Context, req *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.RequestWithContext(ctx, req, xTime)
}

// Execute calls Execute on the underlying database.
func (s *SwappableDB) Execute(ex *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Execute(ex, xTime)
}

// ExecuteWithContext calls ExecuteWithContext on the underlying database.
func (s *SwappableDB) ExecuteWithContext(ctx context.Context, ex *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.ExecuteWithContext(ctx, ex, xTime)
}

// Query calls Query on the underlying database.
func (s *SwappableDB) Query(q *command.Request, xTime bool) ([]*command.QueryRows, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Query(q, xTime)
}

// QueryWithContext calls QueryWithContext on the underlying database.
func (s *SwappableDB) QueryWithContext(ctx context.Context, q *command.Request, xTime bool) ([]*command.QueryRows, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.QueryWithContext(ctx, q, xTime)
}

// QueryStringStmt calls QueryStringStmt on the underlying database.
func (s *SwappableDB) QueryStringStmt(query string) ([]*command.QueryRows, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.QueryStringStmt(query)
}

// VacuumInto calls VacuumInto on the underlying database.
func (s *SwappableDB) VacuumInto(path string) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.VacuumInto(path)
}

// Backup calls Backup on the underlying database.
func (s *SwappableDB) Backup(path string, vacuum bool) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Backup(path, vacuum)
}

// Serialize calls Serialize on the underlying database.
func (s *SwappableDB) Serialize() ([]byte, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Serialize()
}

// StmtReadOnly calls StmtReadOnly on the underlying database.
func (s *SwappableDB) StmtReadOnly(sql string) (bool, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.StmtReadOnly(sql)
}

// Optimize calls Optimize on the underlying database.
func (s *SwappableDB) Optimize() error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Optimize()
}

// SetSynchronousMode calls SetSynchronousMode on the underlying database.
func (s *SwappableDB) SetSynchronousMode(mode SynchronousMode) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.SetSynchronousMode(mode)
}

// Path calls Path on the underlying database.
func (s *SwappableDB) Path() string {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Path()
}

// Dump calls Dump on the underlying database.
func (s *SwappableDB) Dump(w io.Writer, tableNames ...string) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Dump(w, tableNames...)
}

// Vacuum calls Vacuum on the underlying database.
func (s *SwappableDB) Vacuum() error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Vacuum()
}

// FKEnabled calls FKEnabled on the underlying database.
func (s *SwappableDB) FKEnabled() bool {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.FKEnabled()
}

// WALEnabled calls WALEnabled on the underlying database.
func (s *SwappableDB) WALEnabled() bool {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.WALEnabled()
}

// DBLastModified calls DBLastModified on the underlying database.
func (s *SwappableDB) DBLastModified() (time.Time, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.DBLastModified()
}

// FileSize calls FileSize on the underlying database.
func (s *SwappableDB) FileSize() (int64, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.FileSize()
}

// WALSize calls WALSize on the underlying database.
func (s *SwappableDB) WALSize() (int64, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.WALSize()
}

// RegisterPreUpdateHook registers a pre-update hook on the underlying database.
func (s *SwappableDB) RegisterPreUpdateHook(hook PreUpdateHookCallback, tblRe *regexp.Regexp, rowIDsOnly bool) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.RegisterPreUpdateHook(hook, tblRe, rowIDsOnly)
}

// RegisterCommitHook registers a commit hook on the underlying database.
func (s *SwappableDB) RegisterCommitHook(hook CommitHookCallback) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.RegisterCommitHook(hook)
}

// ColumnNames returns the column names for the given table from the underlying database.
func (s *SwappableDB) ColumnNames(table string) ([]string, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.ColumnNames(table)
}

// Checkpoint performs a checkpoint of the underlying database.
func (s *SwappableDB) Checkpoint(w io.Writer, timeout time.Duration) (*CheckpointManagerMeta, int64, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.checkpointMgr.Checkpoint(w, timeout)
}
