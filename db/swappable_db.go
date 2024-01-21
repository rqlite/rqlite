package db

import (
	"fmt"
	"io"
	"os"
	"sync"

	command "github.com/rqlite/rqlite/v8/command/proto"
)

// SwappableDB is a wrapper around DB that allows the underlying database to be swapped out
// in a thread-safe manner.
type SwappableDB struct {
	db   *DB
	dbMu sync.RWMutex
}

// OpenSwappable returns a new SwappableDB instance, which opens the database at the given path.
func OpenSwappable(dbPath string, fkEnabled, wal bool) (*SwappableDB, error) {
	db, err := Open(dbPath, fkEnabled, wal)
	if err != nil {
		return nil, err
	}
	return &SwappableDB{db: db}, nil
}

// Swap swaps the underlying database with that at the given path.
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

	var db *DB
	db, err := Open(s.db.Path(), fkConstraints, walEnabled)
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
func (s *SwappableDB) Stats() (map[string]interface{}, error) {
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
func (s *SwappableDB) Execute(ex *command.Request, xTime bool) ([]*command.ExecuteResult, error) {
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
func (s *SwappableDB) Checkpoint() error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.db.Checkpoint()
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
