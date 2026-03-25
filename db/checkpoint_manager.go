package db

import (
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
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

// CheckpointManager manages checkpointing database across checkpoint attempts.
//
// A key goal of the CheckpointManager is to ensure that an checkpoint attempt
// is not excessively blocked by a concurrent read. If a read is blocking a
// checkpoint the manager manages its state so that a failed checkpoint is
// handled gracefully during by the next checkpoint attempt.
type CheckpointManager struct {
	db      *DB
	dbPath  string
	walPath string

	salt *[2]uint32

	// nextFrameIdx is the index of the next frame in the WAL file to read as
	// part of a checkpoint attempt. This value is 1-based in the sense that
	// if the first frame is to be read, then this value is 1, not zero.
	nextFrameIdx int

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
		cm.nextFrameIdx = 1
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

	if cm.nextFrameIdx > 1 {
		// The manager is telling us to start reading from a non-zero frame index. This
		// means that the all frames were moved in the last checkpoint attempt, but the
		// truncate itself failed. Before we start reading the WAL file from the given
		// frame index, we need to check if the WAL was reset. We can do this by
		// comparing the salt values. If the salt values do not match, then we know
		// that the WAL file has been reset, and we need to reset our state to start
		// reading from the beginning of the WAL file again.
		salt, err := readSaltAt(walFD)
		if err != nil {
			return 0, fmt.Errorf("read WAL salt: %w", err)
		}
		if cm.salt == nil || *salt != *cm.salt {
			cm.nextFrameIdx = 1
			cm.salt = salt
		}
	}

	// Get the page size from the WAL header, which we will need to calculate frame offsets.
	pageSize, err := readPageSizeAt(walFD)
	if err != nil {
		return 0, fmt.Errorf("read WAL page size: %w", err)
	}

	compactStartTime := time.Now()
	scanner, err := wal.NewCompactingSectionScanner(walFD, walOffset(cm.nextFrameIdx, pageSize), 0, false)
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

	/////////////////////////////////////////////////////////////////////////////////
	// Now, attempt to perform a TRUNCATE checkpoint of the database.

	// Grab salt state before WAL is truncated.
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
			// while this checkpoint failed, nothing about our state needs to be updated.
			// Next time we will retry the checkpoint from the same offset and attempt to
			// move all pages.
			cm.logger.Fatalf("just exit during testing for now (case 1)")

			return 0, fmt.Errorf("truncate checkpoint failed to move all pages")
		} else if pnCkpt == pnLog {
			// In this case, the checkpoint failed but all pages were moved, but the
			// WAL file not truncated. We can use the WAL data, but it requires special
			// handling.
			//
			// This needs to be handled carefully because we do not know where the next
			// WAL frame will be written. It might be written to the end of WAL file, or
			// SQLite might reset the WAL, which would cause the next WAL frame to be
			// written at the beginning of the file. The only way to tell will be to check
			// the salt values on the next checkpoint attempt.
			cm.nextFrameIdx = pnCkpt + 1
			cm.logger.Fatalf("just exit during testing for now (case 2)")
		} else {
			// This is not valid state. Give up.
			cm.logger.Fatalf("unexpected checkpoint state: pnCkpt (%d) > pnLog (%d)", pnCkpt, pnLog)
		}
	}

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

// readPageSizeAt reads the page size from the WAL header at the given ReaderAt.
func readPageSizeAt(r io.ReaderAt) (uint32, error) {
	buf := make([]byte, 4)
	if _, err := r.ReadAt(buf, 8); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf), nil
}

// walOffset calculates the byte offset in the WAL file for the given frame index and page size.
// The offset returned is the where the first byte of page data for the given frame index is
// located. Note that the frame index is one-based, so walOffset(1, pageSize) will return the
// offset of the first frame in the WAL file, which is immediately after the WAL header and frame
// header.
func walOffset(frameIdx int, pageSize uint32) int64 {
	return int64(wal.WALHeaderSize+int64(wal.WALFrameHeaderSize)) + int64(frameIdx-1)*(int64(wal.WALFrameHeaderSize)+int64(pageSize))
}
