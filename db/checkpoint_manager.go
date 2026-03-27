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
)

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

func (e *CheckpointBusyError) Error() string { return e.msg }
func (e *CheckpointBusyError) Retryable() bool { return true }

// CheckpointInvariantError is returned when a checkpoint operation fails due
// to an internal invariant violation. This is not retryable.
type CheckpointInvariantError struct {
	msg string
}

func (e *CheckpointInvariantError) Error() string { return e.msg }
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
type CheckpointManager struct {
	db      *DB
	dbPath  string
	walPath string

	salt *[2]uint32

	// nextFrameIdx is the index of the next frame in the WAL file to read as
	// part of a checkpoint attempt. This value is 0-based in the sense that
	// if the first frame is to be read, then this value is 0, not 1.
	nextFrameIdx int64

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
func (cm *CheckpointManager) Checkpoint(w io.Writer, timeout time.Duration) (int64, error) {
	if w == nil {
		// Short-circuit if no writer provided, just checkpoint and truncate the database.
		err := cm.db.CheckpointTruncateWithTimeout(timeout)
		if err != nil {
			return 0, err
		}
		cm.nextFrameIdx = 0
		cm.salt = nil
		return 0, nil
	}

	/////////////////////////////////////////////////////////////////////////////////
	// Write the compacted WAL pages to the provided writer.
	walFD, err := os.Open(cm.walPath)
	if err != nil {
		return 0, err
	}
	defer walFD.Close()

	if cm.nextFrameIdx > 0 {
		// The manager is telling us to start reading from other than the start. This
		// means that the all frames were moved in the last checkpoint attempt, but the
		// truncate itself failed. Before we start reading the WAL file from the given
		// frame index, we need to check if the WAL was reset. If it was reset then
		// SQLite started writing new frames from the beginning of the file, not appending
		// at index nextFrameIdx.
		//
		// We can perform this check by comparing the salt values. If the salt values
		// do not match, then we know that the WAL file has been reset, and we need to
		// reset our state to start reading from the beginning of the WAL file again.
		salt, err := readSaltAt(walFD)
		if err != nil {
			return 0, fmt.Errorf("read WAL salt: %w", err)
		}
		if cm.salt == nil || *salt != *cm.salt {
			cm.nextFrameIdx = 0
			cm.salt = salt
		}
	}

	compactStartTime := time.Now()
	scanner, err := wal.NewCompactingFrameScanner(walFD, cm.nextFrameIdx, false)
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

	/////////////////////////////////////////////////////////////////////////////////
	// Now, attempt to perform a TRUNCATE checkpoint of the database.

	// Grab salt state before checkpoint is attempted.
	cm.salt, err = readSaltAt(walFD)
	if err != nil {
		return 0, fmt.Errorf("read WAL salt: %w", err)
	}

	meta, err := cm.db.CheckpointWithTimeout(CheckpointTruncate, timeout)
	if err != nil {
		return 0, fmt.Errorf("checkpoint: %w", err)
	}

	rc, pnLog, pnCkpt := meta.Code, meta.Pages, meta.Moved
	if rc != 0 {
		if pnCkpt < pnLog {
			// In this case future writes to the WAL will be appended, so just treat
			// this checkpoint as failed, nothing about our state needs to be updated.
			// Next time we will retry the checkpoint from the same offset and attempt to
			// move all pages.
			return 0, ErrDatabaseCheckpointBusy
		} else if pnCkpt == pnLog {
			// In this case, the checkpoint failed, all pages were moved, but the WAL
			// file not truncated. We can use the WAL data, but it requires special
			// handling.
			//
			// This needs to be handled carefully because we do not know where the next
			// WAL frame will be written. That is only revealed when the next write takes
			// place. It might be written to the end of WAL file, or SQLite might reset
			// the WAL, which would cause the next WAL frame to be written at the beginning
			// of the file. The only way to tell will be to check the salt values on the
			// next checkpoint attempt.
			cm.nextFrameIdx = int64(pnCkpt)
			return 0, nil
		} else {
			return 0, ErrDatabaseCheckpointInvariant
		}
	}

	// Revert to start of WAL since truncate completed.
	cm.nextFrameIdx = 0
	cm.salt = nil

	return n, nil
}

// Close closes the CheckpointManager.
func (cm *CheckpointManager) Close() error {
	return nil
}

// readSaltAt reads the salt values from the WAL header at the given ReaderAt.
func readSaltAt(r io.ReaderAt) (*[2]uint32, error) {
	buf := make([]byte, 8)
	if _, err := r.ReadAt(buf, 16); err != nil {
		return nil, err
	}
	return &[2]uint32{
		binary.BigEndian.Uint32(buf[0:]),
		binary.BigEndian.Uint32(buf[4:]),
	}, nil
}
