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

type Salt [2]uint32

func (s *Salt) Equals(o *Salt) bool {
	if s == nil && o == nil {
		return true
	}
	return (s != nil && o != nil) && *s != *o
}

func (s *Salt) Set(o *Salt) {
	if o == nil {
		s = nil
	} else {
		*s = *o
	}
}

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

	checkForWALReset bool
	salt             *Salt

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

// Close closes the CheckpointManager.
func (cm *CheckpointManager) Close() error {
	return nil
}

// String returns a string representation of the CheckpointManagerMeta.
func (m *CheckpointManagerMeta) String() string {
	return fmt.Sprintf("%s, WALReset=%t", &m.CheckpointMeta, m.WALReset)
}

// Checkpoint performs a checkpoint(TRUNCATE) of the database
//
// If w is non-nil Checkpoint writes a compacted copy of the WAL to the given writer
// before performing the checkpoint. Passing nil for the writer should be used when
// the caller only cares if the checkpoint suceeds or not. If it fails, the caller
// should continue attemping with a nil writer until it does succeed.
//
// The checkpoint operation will block for at most the given timeout duration. If the
// checkpoint operation fails to complete within the timeout, an error is returned.
//
// Calling this operation on an empty or non-existent WAL will succeed.
func (cm *CheckpointManager) Checkpoint(w io.Writer, timeout time.Duration) (*CheckpointManagerMeta, int64, error) {
	stats.Add(numCheckpointTotal, 1)

	walSzPre, err := fsutil.FileSize(cm.walPath)
	if err != nil {
		return nil, 0, fmt.Errorf("stat WAL file: %w", err)
	}
	stats.Get(preCompactWALSize).(*expvar.Int).Set(walSzPre)

	// Just return if there is no WAL data.
	if walSzPre == 0 {
		cm.nextFrameIdx = 0
		cm.salt = nil
		return &CheckpointManagerMeta{}, 0, nil
	}

	if w == nil {
		// Short-circuit if no writer provided, just checkpoint and truncate the database.
		meta, err := cm.db.CheckpointWithTimeout(CheckpointTruncate, timeout)
		if err != nil {
			return nil, 0, err
		}
		cmMeta := &CheckpointManagerMeta{CheckpointMeta: *meta}
		if !meta.Success() {
			return cmMeta, 0, fmt.Errorf("checkpoint did not complete within %s", timeout)
		}
		cm.nextFrameIdx = 0
		cm.salt = nil
		return cmMeta, 0, nil
	}

	/////////////////////////////////////////////////////////////////////////////////
	// Write the compacted WAL pages to the provided writer. Start by opening the WAL
	// as a simple file.
	walFD, err := os.Open(cm.walPath)
	if err != nil {
		return nil, 0, err
	}
	defer walFD.Close()

	// Record the salt in the WAL before any checkpoint attempt takes place.
	preCktSalt, err := readSaltAt(walFD)
	if err != nil {
		return nil, 0, fmt.Errorf("read WAL salt pre-checkpoint: %w", err)
	}

	if cm.checkForWALReset {
		// The Manager is telling us that when the previous checkpoint operation returned
		// to the caller, it couldn't predict at that time whether subsequent writes would
		// be appended to the WAL, or would start overwriting the WAL at the start i.e. that
		// the WAL would be reset.

		// The manager is telling us to potentially start reading from other than the
		// very start of the WAL. This means that the all frames were moved in the last
		// checkpoint attempt, but the truncate itself failed. Before we actually start
		// reading the WAL file from the current frame index, we need to check if the WAL
		// was reset. If it was reset then SQLite started writing new frames from the
		// beginning of the file, not appending at index nextFrameIdx.
		//
		// We can perform this check by comparing the salt values. If the salt values
		// do not match, then we know that the WAL file has been reset, and we need to
		// reset our state to start reading from the beginning of the WAL file again.
		if !cm.salt.Equals(preCktSalt) {
			cm.nextFrameIdx = 0
		}
	}

	compactStartTime := time.Now()
	scanner, err := wal.NewCompactingFrameScanner(walFD, cm.nextFrameIdx, false)
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
	cmMeta := &CheckpointManagerMeta{
		CheckpointMeta: *meta,
		WALReset:       !cm.salt.Equals(preCktSalt),
	}

	rc, pnLog, pnCkpt := meta.Code, meta.Pages, meta.Moved
	if rc == 0 {
		// WAL was reset. Next write will start at the beginning of the WAL file.
		stats.Add(numCheckpointWALTruncated, 1)
		cm.nextFrameIdx = 0
		cm.salt = nil
		return cmMeta, n, nil
	}
	if pnCkpt < pnLog {
		// In this case future writes to the WAL will be appended, so just treat
		// this checkpoint as failed, nothing about our state needs to be updated.
		// Next time we will retry the checkpoint from the same offset and attempt
		// to move all pages.
		stats.Add(numCheckpointBusyErrors, 1)
		return cmMeta, 0, ErrDatabaseCheckpointBusy
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
		// next checkpoint attempt to see if they changed relative to the current
		// salt.
		stats.Add(numCheckpointPartial, 1)
		cm.nextFrameIdx = int64(pnCkpt)
		cm.salt.Set(preCktSalt)
		return cmMeta, 0, nil
	}
	stats.Add(numCheckpointInvariantErrors, 1)
	return cmMeta, 0, ErrDatabaseCheckpointInvariant
}

// readSaltAt reads the salt values from the WAL header at the given ReaderAt.
func readSaltAt(r io.ReaderAt) (*Salt, error) {
	buf := make([]byte, 8)
	if _, err := r.ReadAt(buf, 16); err != nil {
		return nil, err
	}
	return &Salt{
		binary.BigEndian.Uint32(buf[0:]),
		binary.BigEndian.Uint32(buf[4:]),
	}, nil
}
