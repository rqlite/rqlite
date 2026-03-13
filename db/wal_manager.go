package db

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rqlite/rqlite/v10/db/wal"
)

const (
	walMgrBusyTimeout = 100 * time.Millisecond
)

// ErrCheckpointInvariant is returned when a FULL checkpoint returns a non-zero
// return code but reports that all frames were checkpointed. This should not
// be possible and indicates unexpected SQLite behavior.
type ErrCheckpointInvariant struct {
	RC     int
	PnLog  int
	PnCkpt int
}

func (e ErrCheckpointInvariant) Error() string {
	return fmt.Sprintf("checkpoint invariant violation: rc=%d pnLog=%d pnCkpt=%d", e.RC, e.PnLog, e.PnCkpt)
}

// WALManager manages SQLite WAL checkpointing and incremental WAL frame
// delivery. It tracks a cursor into the WAL file across checkpoint calls,
// detecting WAL resets via salt comparison, and returns ephemeral WALWriter
// objects that stream frame ranges as complete, self-contained WAL files.
//
// WALManager is not safe for concurrent use.
type WALManager struct {
	db       *DB
	f        *os.File
	salt     [2]uint32
	offset   int64
	pageSize uint32
}

// NewWALManager creates a new WALManager for the given database. The WAL
// file does not need to exist at construction time; it will be opened and
// validated on the first call to Checkpoint.
func NewWALManager(db *DB) *WALManager {
	return &WALManager{
		db: db,
	}
}

// init opens and validates the WAL file on the first Checkpoint call.
func (m *WALManager) init() error {
	if m.f != nil {
		return nil
	}

	walPath := m.db.WALPath()
	if !IsValidSQLiteWALFile(walPath) {
		return fmt.Errorf("not a valid SQLite WAL file: %s", walPath)
	}

	f, err := os.Open(walPath)
	if err != nil {
		return err
	}

	salt, err := readSaltAt(f)
	if err != nil {
		f.Close()
		return fmt.Errorf("read WAL salt: %w", err)
	}

	pageSize, err := readPageSizeAt(f)
	if err != nil {
		f.Close()
		return fmt.Errorf("read WAL page size: %w", err)
	}

	m.f = f
	m.salt = salt
	m.pageSize = pageSize
	return nil
}

// Checkpoint performs a FULL WAL checkpoint and returns a WALWriter that
// streams the new WAL frames since the last call. The busy flag is true if
// not all frames could be copied back to the database file due to reader
// locks. The caller must fully consume the WALWriter before calling
// Checkpoint again. The WAL file must exist when Checkpoint is called.
func (m *WALManager) Checkpoint() (*WALWriter, bool, error) {
	if err := m.init(); err != nil {
		return nil, false, err
	}

	prevSalt := m.salt

	salt, err := readSaltAt(m.f)
	if err != nil {
		return nil, false, fmt.Errorf("read WAL salt: %w", err)
	}
	m.salt = salt

	meta, err := m.db.CheckpointWithTimeout(CheckpointFull, walMgrBusyTimeout)
	if err != nil {
		return nil, false, fmt.Errorf("checkpoint: %w", err)
	}

	rc, pnLog, pnCkpt := meta.Code, meta.Pages, meta.Moved
	if rc != 0 && pnCkpt == pnLog {
		return nil, false, ErrCheckpointInvariant{RC: rc, PnLog: pnLog, PnCkpt: pnCkpt}
	}
	busy := rc != 0

	// If the salt changed, the WAL was reset. Deliver all frames from the
	// beginning.
	if prevSalt != m.salt {
		m.offset = 0
	}

	startOffset := m.offset
	if startOffset == 0 {
		startOffset = wal.WALHeaderSize
	}

	frameSize := int64(wal.WALFrameHeaderSize) + int64(m.pageSize)
	endOffset := int64(wal.WALHeaderSize) + int64(pnLog)*frameSize
	m.offset = endOffset

	return &WALWriter{
		f:     m.f,
		start: startOffset,
		end:   endOffset,
	}, busy, nil
}

// Close releases the WAL file handle.
func (m *WALManager) Close() error {
	if m.f != nil {
		return m.f.Close()
	}
	return nil
}

// WALWriter streams a range of WAL frames as a complete, self-contained WAL
// file. It is ephemeral, created by each Checkpoint call and discarded after
// use.
type WALWriter struct {
	f     *os.File
	start int64
	end   int64
}

// WriteTo writes a complete WAL file containing the frames in this writer's
// range to dst.
func (w *WALWriter) WriteTo(dst io.Writer) (int64, error) {
	scanner, err := wal.NewCompactingSectionScanner(w.f, w.start, w.end)
	if err != nil {
		return 0, fmt.Errorf("create section scanner: %w", err)
	}
	writer, err := wal.NewWriter(scanner)
	if err != nil {
		return 0, fmt.Errorf("create WAL writer: %w", err)
	}
	return writer.WriteTo(dst)
}

// Empty reports whether the writer carries zero frames.
func (w *WALWriter) Empty() bool {
	return w.start >= w.end
}

// readSaltAt reads the salt values from the WAL header at the given ReaderAt.
func readSaltAt(r io.ReaderAt) ([2]uint32, error) {
	buf := make([]byte, 8)
	if _, err := r.ReadAt(buf, 16); err != nil {
		return [2]uint32{}, err
	}
	return [2]uint32{
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
