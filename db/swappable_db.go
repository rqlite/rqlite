package db

import (
	"context"
	"errors"
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

	// Only a successfully installed replacement makes its leftover stash safe to remove.
	stashCleanupPending bool
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
// The existing database files are stashed until the replacement is open. On
// failure, Swap attempts to restore and reopen the original database. Cleanup or
// rollback errors are returned as well; callers must not assume that every error
// leaves the database usable or restores connection-local state such as hooks.
func (s *SwappableDB) Swap(path string, fkConstraints, walEnabled bool) (retErr error) {
	if !IsValidSQLiteFile(path) {
		return fmt.Errorf("invalid SQLite data")
	}

	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	oldDB := s.db
	dbPath := oldDB.Path()
	if s.stashCleanupPending {
		if err := RemoveStashedFiles(dbPath); err != nil {
			return fmt.Errorf("failed to remove stash from completed swap: %w", err)
		}
		s.stashCleanupPending = false
	}
	srcInfo, err := os.Stat(path)
	if err != nil {
		return err
	}
	dstInfo, err := os.Stat(dbPath)
	if err != nil {
		return err
	}
	if os.SameFile(srcInfo, dstInfo) {
		return fmt.Errorf("cannot swap a database with itself")
	}
	syncMode, err := oldDB.GetSynchronousMode()
	if err != nil {
		return fmt.Errorf("failed to read original synchronous mode: %w", err)
	}
	maxROConns := oldDB.roDB.Stats().MaxOpenConnections
	if err := s.checkpointMgr.Close(); err != nil {
		return fmt.Errorf("failed to close checkpoint manager: %w", err)
	}
	if err := oldDB.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	stashed := false
	var incoming *DB
	defer func() {
		if retErr == nil {
			return
		}
		if incoming != nil {
			if err := incoming.Close(); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("failed to close replacement database: %w", err))
				return // Do not move files while the replacement may still be open.
			}
		}
		if stashed {
			if err := RemoveFiles(dbPath); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("failed to remove replacement files: %w", err))
				return
			}
			if err := PopFiles(dbPath); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("failed to restore original files: %w", err))
				return
			}
			if err := fsutil.SyncDirMaybe(filepath.Dir(dbPath)); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("failed to sync restored files: %w", err))
			}
		} else if errors.Is(retErr, errSQLiteFilesRollback) {
			return // An incomplete stash rollback must not be opened as a database.
		}

		// Use the original driver and settings, which may differ from those of
		// the failed replacement. Opening only the on-disk files is not enough:
		// callers still hold this SwappableDB and need its handles replaced.
		restored, err := OpenWithDriver(oldDB.drv, dbPath, oldDB.fkEnabled, oldDB.wal)
		if err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed to reopen original database: %w", err))
			return
		}
		restored.SetMaxReadOnlyConns(maxROConns)
		if err := restored.SetSynchronousMode(syncMode); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed to restore synchronous mode: %w", err), restored.Close())
			return
		}
		mgr, err := NewCheckpointManager(restored)
		if err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed to recreate original checkpoint manager: %w", err), restored.Close())
			return
		}
		s.db, s.checkpointMgr = restored, mgr
	}()

	if err := StashFiles(dbPath); err != nil {
		return fmt.Errorf("failed to stash database files: %w", err)
	}
	stashed = true
	if err := fsutil.SyncDirMaybe(filepath.Dir(dbPath)); err != nil {
		return fmt.Errorf("failed to sync stashed files: %w", err)
	}
	if err := os.Rename(path, dbPath); err != nil {
		return fmt.Errorf("failed to rename database: %w", err)
	}
	if err := fsutil.SyncDirMaybe(filepath.Dir(dbPath)); err != nil {
		return fmt.Errorf("failed to sync replacement files: %w", err)
	}

	incoming, err = OpenWithDriver(s.drv, dbPath, fkConstraints, walEnabled)
	if err != nil {
		return fmt.Errorf("open SQLite file failed: %w", err)
	}
	incoming.SetMaxReadOnlyConns(maxROConns)
	mgr, err := NewCheckpointManager(incoming)
	if err != nil {
		return fmt.Errorf("failed to recreate checkpoint manager: %w", err)
	}
	s.db, s.checkpointMgr = incoming, mgr

	// The replacement is installed. A cleanup failure must not trigger rollback:
	// some of the stashed files may already have been removed.
	if err := RemoveStashedFiles(dbPath); err != nil {
		s.stashCleanupPending = true
		incoming.logger.Printf("failed to remove stashed database files after swap: %s", err)
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
