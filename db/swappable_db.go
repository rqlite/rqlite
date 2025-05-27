package db

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	command "github.com/rqlite/rqlite/v8/command/proto"
)

// SwappableDB is a wrapper around DB that allows the underlying database to be swapped out
// in a thread-safe manner.
type SwappableDB struct {
	db         *DB
	drv        *Driver
	dbMu       sync.RWMutex
	cdcEnabled bool // Store CDC enabled status for the swappable DB
}

// OpenSwappable returns a new SwappableDB instance, which opens the database at the given path,
// using the given driver. If drv is nil then the default driver is used. If fkEnabled is true,
// foreign key constraints are enabled. If wal is true, the WAL journal mode is enabled.
// cdcEnabled determines if Change Data Capture is active.
func OpenSwappable(dbPath string, drv *Driver, fkEnabled, wal, cdcEnabled bool) (*SwappableDB, error) {
	if drv == nil {
		drv = DefaultDriver()
	}
	// Pass cdcEnabled to OpenWithDriver
	db, err := OpenWithDriver(drv, dbPath, fkEnabled, wal, cdcEnabled)
	if err != nil {
		return nil, err
	}
	return &SwappableDB{
		db:         db,
		drv:        drv,
		cdcEnabled: cdcEnabled, // Store it
	}, nil
}

// Swap swaps the underlying database with that at the given path. The Swap operation
// may fail on some platforms if the file at path is open by another process. It is
// the caller's responsibility to ensure the file at path is not in use.
// The CDC enabled status from the SwappableDB instance is used for the new DB.
func (s *SwappableDB) Swap(path string, fkConstraints, walEnabled bool) error {
	if !IsValidSQLiteFile(path) {
		return fmt.Errorf("invalid SQLite data")
	}

	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close: %s", err)
	}
	if err := RemoveFiles(s.db.Path()); err != nil {
		return fmt.Errorf("failed to remove files: %s", err)
	}
	if err := os.Rename(path, s.db.Path()); err != nil {
		return fmt.Errorf("failed to rename database: %s", err)
	}

	// Pass the stored s.cdcEnabled to OpenWithDriver for the new DB instance
	db, err := OpenWithDriver(s.drv, s.db.Path(), fkConstraints, walEnabled, s.cdcEnabled)
	if err != nil {
		return fmt.Errorf("open SQLite file failed: %s", err)
	}
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
