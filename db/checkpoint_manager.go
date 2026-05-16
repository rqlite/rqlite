package db

import (
	"encoding/binary"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/rqlite/rqlite/v10/db/wal"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// Salt represents the two 32-bit salt values from a SQLite WAL header.
type Salt [2]uint32

// Equal reports whether s and other hold the same salt values.
func (s Salt) Equal(other Salt) bool {
	return s == other
}

// String returns a human-readable representation of s.
func (s Salt) String() string {
	return fmt.Sprintf("Salt(%d,%d)", s[0], s[1])
}

// RetryableError is an error that indicates whether the failed operation
// can be safely retried.
type RetryableError interface {
	error
	Retryable() bool
}

// CheckpointBusyError is returned when a checkpoint operation is blocked by
// a concurrent read operation, fails to complete within the specified timeout,
// but leaves the database in a state where a future checkpoint attempt may be
// safely retried.
type CheckpointBusyError struct {
	msg string
}

func (e *CheckpointBusyError) Error() string   { return e.msg }
func (e *CheckpointBusyError) Retryable() bool { return true }

// CheckpointInvariantError is returned when a checkpoint operation fails due
// to an internal invariant violation. This is not retryable.
type CheckpointInvariantError struct {
	msg string
}

func (e *CheckpointInvariantError) Error() string   { return e.msg }
func (e *CheckpointInvariantError) Retryable() bool { return false }

var (
	ErrDatabaseCheckpointBusy      = &CheckpointBusyError{msg: "database checkpoint busy"}
	ErrDatabaseCheckpointInvariant = &CheckpointInvariantError{msg: "database checkpoint invariant violation"}
)

// CheckpointManager manages checkpointing database across checkpoints.
//
// A key goal of the CheckpointManager is to ensure that a checkpoint attempt
// is not excessively blocked by a concurrent read. If a read is blocking a
// checkpoint the manager manages its state so that a failed checkpoint is
// handled gracefully by the next checkpoint attempt.
//
// The CheckpointManager is not safe for concurrent use, and not safe with for
// use with concurrent writes to the database. It is the caller's responsibility
// to ensure that checkpoint attempts are serialized and not concurrent with
// writes to the database.
type CheckpointManager struct {
	db      *DB
	dbPath  string
	walPath string

	// resetWatch carries forward state from a partial-checkpoint (all pages
	// moved but WAL not truncated) so the next attempt can detect whether
	// SQLite reset the WAL in the meantime and decide where to resume.
	resetWatch *WALResetWatch

	logger *log.Logger
}

// NewCheckpointManager returns a new CheckpointManager for the given database.
func NewCheckpointManager(db *DB) (*CheckpointManager, error) {
	return &CheckpointManager{
		db:         db,
		dbPath:     db.Path(),
		resetWatch: &WALResetWatch{},
		walPath:    db.WALPath(),
		logger:     log.New(log.Writer(), "[db-checkpoint] ", log.LstdFlags),
	}, nil
}

// CheckpointManagerMeta contains metadata about a checkpoint attempt made
// through CheckpointManager. It embeds CheckpointMeta (the DB-level result)
// and adds fields the manager itself observes.
type CheckpointManagerMeta struct {
	CheckpointMeta

	// WALReset is true if, on entry to this attempt, the manager detected
	// that the WAL had been reset since the previous attempt (salt mismatch
	// with the saved value). It is only meaningful when the manager was
	// tracking partial-checkpoint state from a prior call (i.e. the prior
	// call left nextFrameIdx > 0); otherwise it is always false.
	WALReset bool
}

// String returns a string representation of the CheckpointManagerMeta.
func (m *CheckpointManagerMeta) String() string {
	return fmt.Sprintf("%s, WALReset=%t", &m.CheckpointMeta, m.WALReset)
}

// Checkpoint performs a checkpoint(TRUNCATE) of the database
//
// If w is non-nil Checkpoint writes a compacted copy of the WAL to the given writer
// before performing the checkpoint. The checkpoint operation will block for at most
// the given timeout duration. If the checkpoint operation fails to complete within
// the timeout, an error is returned.
func (cm *CheckpointManager) Checkpoint(w io.Writer, timeout time.Duration) (*CheckpointManagerMeta, int64, error) {
	stats.Add(numCheckpointTotal, 1)

	walSzPre, err := fsutil.FileSize(cm.walPath)
	if err != nil {
		return nil, 0, fmt.Errorf("stat WAL file: %w", err)
	}
	stats.Get(preCompactWALSize).(*expvar.Int).Set(walSzPre)

	if walSzPre == 0 {
		cm.resetWatch.Disarm()
		return &CheckpointManagerMeta{}, 0, nil
	}

	if w == nil {
		// Short-circuit if no writer provided, just checkpoint and truncate the database.
		meta, err := cm.db.CheckpointWithTimeout(CheckpointTruncate, timeout)
		if err != nil {
			return nil, 0, err
		}
		mmeta := &CheckpointManagerMeta{CheckpointMeta: *meta}
		if !meta.Success() {
			return mmeta, 0, fmt.Errorf("checkpoint did not complete within %s", timeout)
		}
		cm.resetWatch.Disarm()
		return mmeta, 0, nil
	}

	/////////////////////////////////////////////////////////////////////////////////
	// Write the compacted WAL pages to the provided writer.
	walFD, err := os.Open(cm.walPath)
	if err != nil {
		return nil, 0, err
	}
	defer walFD.Close()

	// Record the salt in the WAL header before any changes take place. If we
	// were watching for a WAL reset, the watch tells us where to resume and
	// whether the WAL was in fact reset since it was armed.
	preChkSalt, err := readSaltAt(walFD)
	if err != nil {
		return nil, 0, fmt.Errorf("read WAL salt: %w", err)
	}
	startFrameIdx, walReset := cm.resetWatch.Check(preChkSalt)

	compactStartTime := time.Now()
	scanner, err := wal.NewCompactingFrameScanner(walFD, startFrameIdx, false)
	if err != nil {
		return nil, 0, fmt.Errorf("create compacting frame scanner: %w", err)
	}
	ww, err := wal.NewWriter(scanner)
	if err != nil {
		return nil, 0, fmt.Errorf("create WAL writer: %w", err)
	}
	n, err := ww.WriteTo(w)
	if err != nil {
		return nil, 0, fmt.Errorf("write WAL data: %w", err)
	}
	recordDuration(createCompactedWALDuration, compactStartTime)
	stats.Get(compactedWALSize).(*expvar.Int).Set(n)

	/////////////////////////////////////////////////////////////////////////////////
	// Now, attempt to perform a TRUNCATE checkpoint of the database.

	meta, err := cm.db.CheckpointWithTimeout(CheckpointTruncate, timeout)
	if err != nil {
		return nil, 0, fmt.Errorf("checkpoint: %w", err)
	}
	mmeta := &CheckpointManagerMeta{
		CheckpointMeta: *meta,
		WALReset:       walReset,
	}

	rc, pnLog, pnCkpt := meta.Code, meta.Pages, meta.Moved
	if rc == 0 {
		// WAL was reset. Next write will start at the beginning of the WAL file.
		stats.Add(numCheckpointWALTruncated, 1)
		cm.resetWatch.Disarm()
		return mmeta, n, nil
	}
	if pnCkpt < pnLog {
		// In this case future writes to the WAL will be appended, so just treat
		// this checkpoint as failed, nothing about our state needs to be updated.
		// Next time we will retry the checkpoint from the same offset and attempt
		// to move all pages.
		stats.Add(numCheckpointBusyErrors, 1)
		return mmeta, 0, ErrDatabaseCheckpointBusy
	} else if pnCkpt == pnLog {
		// In this case, the checkpoint failed, all pages were moved, but the WAL
		// file not truncated. We can use the WAL data, but it requires special
		// handling.
		//
		// We do not know where the next WAL frame will be written. That is only
		// revealed when the next write takes place: SQLite might append at the end
		// of the WAL file, or it might reset the WAL and write from the start. The
		// only way to tell on the next checkpoint attempt is to compare salt
		// values, so arm the watch with the salt and resume frame we will need.
		stats.Add(numCheckpointPartial, 1)
		cm.resetWatch.Arm(preChkSalt, int64(pnCkpt))
		return mmeta, 0, nil
	}
	stats.Add(numCheckpointInvariantErrors, 1)
	return mmeta, 0, ErrDatabaseCheckpointInvariant
}

// Close closes the CheckpointManager.
func (cm *CheckpointManager) Close() error {
	return nil
}

// readSaltAt reads the salt values from the WAL header at the given ReaderAt.
func readSaltAt(r io.ReaderAt) (Salt, error) {
	buf := make([]byte, 8)
	if _, err := r.ReadAt(buf, 16); err != nil {
		return Salt{}, err
	}
	return Salt{
		binary.BigEndian.Uint32(buf[0:]),
		binary.BigEndian.Uint32(buf[4:]),
	}, nil
}
