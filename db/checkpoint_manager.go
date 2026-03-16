package db

import (
	"errors"
	"expvar"
	"io"
	"log"
	"os"
	"time"

	"github.com/rqlite/rqlite/v10/db/wal"
)

var (
	// ErrDatabaseCheckpointFailed is returned when a checkpoint operation
	// fails to complete at the database level.
	ErrDatabaseCheckpointFailed = errors.New("database checkpoint failed")
)

// CheckpointManager manages checkpointing database across incremental snapshots.
type CheckpointManager struct {
	db      *DB
	dbPath  string
	walPath string

	logger *log.Logger
}

// NewCheckpointManager returns a new CheckpointManager for the given database.
func NewCheckpointManager(db *DB) (*CheckpointManager, error) {
	return &CheckpointManager{
		db:      db,
		dbPath:  db.Path(),
		walPath: db.WALPath(),
		logger:  log.New(log.Writer(), "[db-checkpoint] ", log.LstdFlags),
	}, nil
}

// Checkpoint performs a checkpoint(TRUNCATE) of the database
//
// If w is non-nil Checkpoint writes a compacted copy of the WAL to the given writer
// before performing the checkpoint. The checkpoint operation will block for at most
// the given timeout duration. If the checkpoint operation fails to complete within
// the timeout, an error is returned.
//
// If ErrDatabaseCheckpointFailed is returned, the checkpoint operation failed at the
// database level, and the caller should assume the database is in an unknown state.
func (cm *CheckpointManager) Checkpoint(w io.Writer, timeout time.Duration) (int64, error) {
	if w == nil {
		// Short-circuit if no writer provided, just checkpoint and truncate the database.
		return 0, cm.db.CheckpointTruncateWithTimeout(timeout)
	}

	walFD, err := os.Open(cm.walPath)
	if err != nil {
		return 0, err
	}
	defer walFD.Close()

	compactStartTime := time.Now()
	scanner, err := wal.NewFastCompactingScanner(walFD)
	if err != nil {
		return 0, err
	}
	ww, err := wal.NewWriter(scanner)
	if err != nil {
		return 0, err
	}
	n, err := ww.WriteTo(w)
	if err != nil {
		return 0, err
	}
	stats.Get(createCompactedWALDuration).(*expvar.Int).Set(time.Since(compactStartTime).Milliseconds())
	stats.Get(compactedWALSize).(*expvar.Int).Set(n)

	walSzPre, err := fileSize(cm.walPath)
	if err != nil {
		return 0, err
	}
	stats.Get(preCompactWALSize).(*expvar.Int).Set(walSzPre)
	if err := cm.db.CheckpointTruncateWithTimeout(timeout); err != nil {
		return 0, ErrDatabaseCheckpointFailed
	}
	return n, nil
}

// Close closes the CheckpointManager.
func (cm *CheckpointManager) Close() error {
	return nil
}
