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

// Restore the original database after a failed swap.
func (s *SwappableDB) restoreAfterSwapFailure(dbPath, tempPath string, fkConstraints, walEnabled bool) error {
	// Remove the failed replacement database.
	if err := RemoveFiles(dbPath); err != nil {
		return fmt.Errorf("failed to remove failed replacement: %s", err)
	}

	// Restore the original database files.
	if err := restoreDatabaseFiles(dbPath, tempPath); err != nil {
		return fmt.Errorf("failed to restore original database files: %s", err)
	}

	// Reopen the original database so SwappableDB is usable again.
	db, err := OpenWithDriver(s.drv, dbPath, fkConstraints, walEnabled)
	if err != nil {
		return fmt.Errorf("failed to reopen original database: %s", err)
	}

	// Recreate the checkpoint manager because the previous DB connection
	// was closed before the swap started.
	mgr, err := NewCheckpointManager(db)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to recreate checkpoint manager: %s", err)
	}

	s.db = db
	s.checkpointMgr = mgr

	return nil
}

// Swap swaps the underlying database with that at the given path. The Swap operation
// may fail on some platforms if the file at path is open by another process. It is
// the caller's responsibility to ensure the file at path is not in use.
func (s *SwappableDB) Swap(path string, fkConstraints, walEnabled bool) error {
	if !IsValidSQLiteFile(path) {
		return fmt.Errorf("invalid SQLite data")
	}

	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	dbPath := s.db.Path()

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close: %s", err)
	}

	tempPath, err := createSwapTempPath(dbPath)
	if err != nil {
		// No files have been moved yet; reopen the original database.
		db, reopenErr := OpenWithDriver(s.drv, dbPath, fkConstraints, walEnabled)
		if reopenErr != nil {
			return fmt.Errorf(
				"failed to create swap temporary path: %s; failed to reopen original database: %s",
				err,
				reopenErr,
			)
		}
		mgr, mgrErr := NewCheckpointManager(db)
		if mgrErr != nil {
			_ = db.Close()
			return fmt.Errorf(
				"failed to create swap temporary path: %s; failed to recreate checkpoint manager: %s",
				err,
				mgrErr,
			)
		}
		s.db = db
		s.checkpointMgr = mgr
		return fmt.Errorf("failed to create swap temporary path: %s", err)
	}

	if err := renameDatabaseFiles(dbPath, tempPath); err != nil {
		// Some files may have been partially moved; restore what we can and reopen.
		if restoreErr := s.restoreAfterSwapFailure(dbPath, tempPath, fkConstraints, walEnabled); restoreErr != nil {
			return fmt.Errorf("failed to rename existing database files: %s; rollback failed: %s", err, restoreErr)
		}
		return fmt.Errorf("failed to rename existing database files: %s", err)
	}

	if err := os.Rename(path, dbPath); err != nil {
		// Original files are in tempPath; restore them and reopen the original database.
		if restoreErr := s.restoreAfterSwapFailure(dbPath, tempPath, fkConstraints, walEnabled); restoreErr != nil {
			return fmt.Errorf("failed to rename database: %s; rollback failed: %s", err, restoreErr)
		}
		return fmt.Errorf("failed to rename database: %s", err)
	}

	db, err := OpenWithDriver(s.drv, dbPath, fkConstraints, walEnabled)
	if err != nil {
		if restoreErr := s.restoreAfterSwapFailure(dbPath, tempPath, fkConstraints, walEnabled); restoreErr != nil {
			return fmt.Errorf("open SQLite file failed: %s; rollback failed: %s", err, restoreErr)
		}

		return fmt.Errorf("open SQLite file failed: %s", err)
	}

	newCheckpointMgr, err := NewCheckpointManager(db)
	if err != nil {
		_ = db.Close()

		if restoreErr := s.restoreAfterSwapFailure(dbPath, tempPath, fkConstraints, walEnabled); restoreErr != nil {
			return fmt.Errorf("failed to create checkpoint manager: %s; rollback failed: %s", err, restoreErr)
		}

		return fmt.Errorf("failed to create checkpoint manager: %s", err)
	}

	oldCheckpointMgr := s.checkpointMgr

	if err := oldCheckpointMgr.Close(); err != nil {
		_ = newCheckpointMgr.Close()
		_ = db.Close()

		if restoreErr := s.restoreAfterSwapFailure(dbPath, tempPath, fkConstraints, walEnabled); restoreErr != nil {
			return fmt.Errorf("failed to close old checkpoint manager: %s; rollback failed: %s", err, restoreErr)
		}

		return fmt.Errorf("failed to close old checkpoint manager: %s", err)
	}

	s.db = db
	s.checkpointMgr = newCheckpointMgr

	if err := RemoveFiles(tempPath); err != nil {
		return fmt.Errorf("swap succeeded, but failed to remove old database files: %s", err)
	}

	return nil
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

// Move database files to temporary paths.
func renameDatabaseFiles(path, tempPath string) error {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		src := path + suffix
		dst := tempPath + suffix

		if _, err := os.Stat(src); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		if err := os.Rename(src, dst); err != nil {
			return err
		}
	}

	return nil
}

// Restore database files from temporary paths.
func restoreDatabaseFiles(path, tempPath string) error {
	var firstErr error

	for _, suffix := range []string{"", "-wal", "-shm"} {
		src := tempPath + suffix
		dst := path + suffix

		if _, err := os.Stat(src); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		if err := os.Rename(src, dst); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Create a unique temporary swap path.
func createSwapTempPath(dbPath string) (string, error) {
	dir := filepath.Dir(dbPath)
	base := filepath.Base(dbPath)

	file, err := os.CreateTemp(dir, base+"-swap-*")
	if err != nil {
		return "", err
	}

	tempPath := file.Name()

	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return "", err
	}

	if err := os.Remove(tempPath); err != nil {
		return "", err
	}

	return tempPath, nil
}
