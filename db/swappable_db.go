package db

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	command "github.com/rqlite/rqlite/v8/command/proto"
)

// SwappableDB is a wrapper around DB that allows the underlying database to be swapped out
// in a thread-safe manner.
type SwappableDB struct {
	db   *DB
	drv  *Driver
	dbMu sync.RWMutex
}

// OpenSwappable returns a new SwappableDB instance, which opens the database at the given path,
// using the given driver. If drv is nil then the default driver is used. If fkEnabled is true,
// foreign key constraints are enabled. If wal is true, the WAL journal mode is enabled.
func OpenSwappable(dbPath string, drv *Driver, fkEnabled, wal bool) (*SwappableDB, error) {
	if drv == nil {
		drv = DefaultDriver()
	}
	db, err := OpenWithDriver(drv, dbPath, fkEnabled, wal)
	if err != nil {
		return nil, err
	}
	return &SwappableDB{
		db:  db,
		drv: drv,
	}, nil
}

// generateTempPath generates a temporary file path for the given database path.
func generateTempPath(dbPath string) string {
	dir := filepath.Dir(dbPath)
	base := filepath.Base(dbPath)
	return filepath.Join(dir, "."+base+".tmp")
}

// renameToTemp renames the database files (main, WAL, SHM) to temporary names.
// Returns the temporary paths that were created, or an error if any rename fails.
func renameToTemp(dbPath string) (tempPaths []string, err error) {
	tempPath := generateTempPath(dbPath)

	// Try to rename main database file
	if fileExists(dbPath) {
		if err := os.Rename(dbPath, tempPath); err != nil {
			return nil, fmt.Errorf("failed to rename database file to temp: %s", err)
		}
		tempPaths = append(tempPaths, tempPath)
	}

	// Try to rename WAL file
	walPath := dbPath + "-wal"
	walTempPath := tempPath + "-wal"
	if fileExists(walPath) {
		if err := os.Rename(walPath, walTempPath); err != nil {
			// Rollback main database file rename
			if len(tempPaths) > 0 {
				os.Rename(tempPath, dbPath)
			}
			return nil, fmt.Errorf("failed to rename WAL file to temp: %s", err)
		}
		tempPaths = append(tempPaths, walTempPath)
	}

	// Try to rename SHM file
	shmPath := dbPath + "-shm"
	shmTempPath := tempPath + "-shm"
	if fileExists(shmPath) {
		if err := os.Rename(shmPath, shmTempPath); err != nil {
			// Rollback previous renames
			if len(tempPaths) > 1 {
				os.Rename(walTempPath, walPath)
			}
			if len(tempPaths) > 0 {
				os.Rename(tempPath, dbPath)
			}
			return nil, fmt.Errorf("failed to rename SHM file to temp: %s", err)
		}
		tempPaths = append(tempPaths, shmTempPath)
	}

	return tempPaths, nil
}

// restoreFromTemp restores the database files from their temporary names back to original names.
func restoreFromTemp(dbPath string, tempPaths []string) error {
	tempPath := generateTempPath(dbPath)

	// Restore in reverse order to handle dependencies
	for i := len(tempPaths) - 1; i >= 0; i-- {
		tempFile := tempPaths[i]
		var originalPath string

		if tempFile == tempPath {
			originalPath = dbPath
		} else if tempFile == tempPath+"-wal" {
			originalPath = dbPath + "-wal"
		} else if tempFile == tempPath+"-shm" {
			originalPath = dbPath + "-shm"
		} else {
			continue // Skip unknown temp files
		}

		if err := os.Rename(tempFile, originalPath); err != nil {
			return fmt.Errorf("failed to restore %s from temp: %s", originalPath, err)
		}
	}

	return nil
}

// removeTempFiles removes the temporary database files.
func removeTempFiles(tempPaths []string) {
	for _, tempFile := range tempPaths {
		os.Remove(tempFile) // Ignore errors since these are cleanup operations
	}
}

// Swap swaps the underlying database with that at the given path. The Swap operation
// may fail on some platforms if the file at path is open by another process. It is
// the caller's responsibility to ensure the file at path is not in use.
func (s *SwappableDB) Swap(path string, fkConstraints, walEnabled bool) (retErr error) {
	if !IsValidSQLiteFile(path) {
		return fmt.Errorf("invalid SQLite data")
	}

	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	// Store the current database configuration for potential recovery
	oldFKEnabled := s.db.FKEnabled()
	oldWALEnabled := s.db.WALEnabled()
	dbPath := s.db.Path()
	var tempPaths []string
	var newFileRenamed bool

	defer func() {
		if retErr != nil {
			// Cleanup on failure
			if newFileRenamed {
				// Remove the new database file if it was renamed into place
				os.Remove(dbPath)
			}
			if len(tempPaths) > 0 {
				// Restore the original files from temporary names
				restoreFromTemp(dbPath, tempPaths)
			}
			// Try to reopen the original database
			if db, reopenErr := OpenWithDriver(s.drv, dbPath, oldFKEnabled, oldWALEnabled); reopenErr == nil {
				s.db = db
			}
		} else {
			// Success - clean up the temporary files
			removeTempFiles(tempPaths)
		}
	}()

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close: %s", err)
	}

	// Rename existing database files to temporary names instead of deleting them
	var err error
	tempPaths, err = renameToTemp(dbPath)
	if err != nil {
		return fmt.Errorf("failed to rename existing files to temp: %s", err)
	}

	// Try to move the new database into place
	if err := os.Rename(path, dbPath); err != nil {
		return fmt.Errorf("failed to rename database: %s", err)
	}
	newFileRenamed = true

	// Try to open the new database
	db, err := OpenWithDriver(s.drv, dbPath, fkConstraints, walEnabled)
	if err != nil {
		return fmt.Errorf("open SQLite file failed: %s", err)
	}

	// Success! Update the database reference
	s.db = db
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

// Execute calls Execute on the underlying database.
func (s *SwappableDB) Execute(ex *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Execute(ex, xTime)
}

// Query calls Query on the underlying database.
func (s *SwappableDB) Query(q *command.Request, xTime bool) ([]*command.QueryRows, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Query(q, xTime)
}

// QueryStringStmt calls QueryStringStmt on the underlying database.
func (s *SwappableDB) QueryStringStmt(query string) ([]*command.QueryRows, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.QueryStringStmt(query)
}

// ExecuteStringStmt calls ExecuteStringStmt on the underlying database.
func (s *SwappableDB) ExecuteStringStmt(query string) ([]*command.ExecuteQueryResponse, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.ExecuteStringStmt(query)
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

// Checkpoint calls Checkpoint on the underlying database.
func (s *SwappableDB) Checkpoint(mode CheckpointMode) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Checkpoint(mode)
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
func (s *SwappableDB) Dump(w io.Writer) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Dump(w)
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
func (s *SwappableDB) RegisterPreUpdateHook(hook PreUpdateHookCallback, rowIDsOnly bool) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.RegisterPreUpdateHook(hook, rowIDsOnly)
}

// RegisterPreUpdateHook registers a commit hook on the underlying database.
func (s *SwappableDB) RegisterCommitHook(hook CommitHookCallback) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.RegisterCommitHook(hook)
}
