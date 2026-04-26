package snapshot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/rsum"
	"github.com/rqlite/rqlite/v10/snapshot/proto"
	"github.com/rqlite/rqlite/v10/snapshot/sidecar"
)

var (
	// ErrSinkOpen indicates that the sink is already open.
	ErrSinkOpen = errors.New("snapshot sink already open")

	// ErrSinkNotOpen indicates that the sink is not open.
	ErrSinkNotOpen = errors.New("snapshot sink not open")

	// ErrUnexpectedData indicates that the caller wrote more bytes than expected.
	ErrUnexpectedData = errors.New("no more data expected")

	// ErrIncomplete indicates Close() was called before all bytes were written.
	ErrIncomplete = errors.New("snapshot install incomplete")

	// ErrHeaderInvalid indicates the header is invalid.
	ErrHeaderInvalid = errors.New("snapshot install header invalid")

	// ErrInvalidSQLiteFile indicates the installed DB file is not a valid SQLite file.
	ErrInvalidSQLiteFile = errors.New("installed DB file is not a valid SQLite file")

	// ErrInvalidWALFile indicates the installed WAL file is not a valid SQLite WAL file.
	ErrInvalidWALFile = errors.New("installed WAL file is not a valid SQLite WAL file")
)

type installPhase int

const (
	installPhaseDB installPhase = iota
	installPhaseWAL
	installPhaseDone
)

// FullSink streams snapshot bytes into files described by a FullSnapshot header.
type FullSink struct {
	dir    string
	header *proto.FullSnapshot

	phase    installPhase
	walIndex int

	f         *os.File
	crcW      *rsum.CRC32Writer
	remaining uint64

	dbFile   string
	walFiles []string

	// CRC32 sums computed inline as bytes are written, captured when each
	// artifact is closed. Verified against the header in Close().
	dbCRC   uint32
	walCRCs []uint32

	opened bool
}

// NewFullSink creates a new FullSink object.
func NewFullSink(dir string, hdr *proto.FullSnapshot) *FullSink {
	s := &FullSink{
		dir:    dir,
		header: hdr,
		dbFile: filepath.Join(dir, "data.db"),
	}

	for i := range hdr.WalHeaders {
		walPath := filepath.Join(dir, fmt.Sprintf("data-%08d.wal", i))
		s.walFiles = append(s.walFiles, walPath)
	}
	s.walCRCs = make([]uint32, len(hdr.WalHeaders))
	return s
}

// Open opens the sink for writing.
func (s *FullSink) Open() error {
	if s.opened {
		return ErrSinkOpen
	}
	if err := s.validateHeader(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}

	s.opened = true
	s.phase = installPhaseDB
	s.walIndex = 0

	return s.openCurrent()
}

// Write writes data to the sink.
func (s *FullSink) Write(p []byte) (int, error) {
	if !s.opened {
		return 0, ErrSinkNotOpen
	}
	if s.phase == installPhaseDone {
		return 0, ErrUnexpectedData
	}

	var total int
	for len(p) > 0 {
		if s.phase == installPhaseDone {
			return total, ErrUnexpectedData
		}
		if s.f == nil {
			if err := s.openCurrent(); err != nil {
				return total, err
			}
			// If openCurrent() advanced to done (e.g. 0-byte artifacts), loop will handle it.
			if s.phase == installPhaseDone {
				continue
			}
		}

		if s.remaining == 0 {
			// Current artifact complete; advance.
			if err := s.advance(); err != nil {
				return total, err
			}
			continue
		}

		// Write up to remaining bytes.
		k := min(uint64(len(p)), s.remaining)
		chunk := p[:int(k)]

		var n int
		var err error
		n, err = s.crcW.Write(chunk)
		total += n
		s.remaining -= uint64(n)

		if err != nil {
			return total, err
		}
		// Handle short writes explicitly.
		if n != len(chunk) {
			return total, errors.New("short write")
		}

		p = p[int(k):]

		// If we exactly completed this artifact, advance and keep going (single Write may span files).
		if s.remaining == 0 {
			if err := s.advance(); err != nil {
				return total, err
			}
		}
	}

	return total, nil
}

// Close closes the sink. It fails if not all bytes were written, or if the
// CRC32 of the received data does not match the header.
func (s *FullSink) Close() error {
	if !s.opened {
		return ErrSinkNotOpen
	}
	defer func() {
		s.opened = false
	}()

	// If we still have bytes outstanding, this is incomplete.
	if s.phase != installPhaseDone {
		// Allow finalization if we're exactly at boundary.
		if s.f != nil && s.remaining == 0 {
			if err := s.advance(); err != nil {
				s.closeFile()
				return err
			}
		}
		if s.phase != installPhaseDone {
			s.closeFile()
			return ErrIncomplete
		}
	}

	if !db.IsValidSQLiteFile(s.dbFile) {
		s.closeFile()
		return ErrInvalidSQLiteFile
	}
	for i, walPath := range s.walFiles {
		if !db.IsValidSQLiteWALFile(walPath) {
			s.closeFile()
			return fmt.Errorf("WAL file %d invalid: %w", i, ErrInvalidWALFile)
		}
	}

	if err := s.closeFile(); err != nil {
		return err
	}

	// CRC32 sums were computed inline as bytes were written through the
	// sink, so verification just compares the captured sums to the header
	// — no second pass over the on-disk files is required.
	start := time.Now()
	if s.dbCRC != s.header.DbHeader.Crc32 {
		return fmt.Errorf("CRC32 mismatch for DB file: got %08x, expected %08x", s.dbCRC, s.header.DbHeader.Crc32)
	}
	for i, walPath := range s.walFiles {
		walCRC := s.walCRCs[i]
		if walCRC != s.header.WalHeaders[i].Crc32 {
			return fmt.Errorf("CRC32 mismatch for WAL file %d: got %08x, expected %08x", i, walCRC, s.header.WalHeaders[i].Crc32)
		}
		if err := sidecar.WriteFile(walPath+crcSuffix, walCRC); err != nil {
			return fmt.Errorf("writing CRC32 sidecar for WAL file %d: %w", i, err)
		}
	}
	if err := sidecar.WriteFile(s.dbFile+crcSuffix, s.dbCRC); err != nil {
		return fmt.Errorf("writing CRC32 sidecar for DB file: %w", err)
	}
	recordDuration(sinkFullCRC32Dur, start)
	return nil
}

// DBFile returns the path to the checkpointed DB file.
func (s *FullSink) DBFile() string {
	return s.dbFile
}

func (s *FullSink) validateHeader() error {
	if s.header == nil || s.header.DbHeader == nil {
		return ErrHeaderInvalid
	}
	return nil
}

func (s *FullSink) openCurrent() error {
	switch s.phase {
	case installPhaseDB:
		f, err := os.Create(filepath.Join(s.dir, dbfileName))
		if err != nil {
			return err
		}
		s.f = f
		s.crcW = rsum.NewCRC32Writer(f)
		s.remaining = s.header.DbHeader.SizeBytes
		return nil

	case installPhaseWAL:
		if s.walIndex >= len(s.header.WalHeaders) {
			s.phase = installPhaseDone
			return nil
		}
		f, err := os.Create(s.walFiles[s.walIndex])
		if err != nil {
			return err
		}
		s.f = f
		s.crcW = rsum.NewCRC32Writer(f)
		s.remaining = s.header.WalHeaders[s.walIndex].SizeBytes
		return nil

	case installPhaseDone:
		return nil

	default:
		return fmt.Errorf("unknown install phase: %d", s.phase)
	}
}

func (s *FullSink) advance() error {
	// Capture the running CRC32 of the artifact we're about to close so we
	// can verify it against the header without re-reading from disk.
	if s.crcW != nil {
		switch s.phase {
		case installPhaseDB:
			s.dbCRC = s.crcW.Sum32()
		case installPhaseWAL:
			s.walCRCs[s.walIndex] = s.crcW.Sum32()
		}
	}

	// Close current artifact if open.
	if err := s.closeFile(); err != nil {
		return err
	}

	switch s.phase {
	case installPhaseDB:
		// DB complete; move to WAL or done.
		if len(s.header.WalHeaders) == 0 {
			s.phase = installPhaseDone
			return nil
		}
		s.phase = installPhaseWAL
		s.walIndex = 0
		return s.openCurrent()

	case installPhaseWAL:
		// Current WAL complete; move to next WAL or done.
		s.walIndex++
		if s.walIndex >= len(s.header.WalHeaders) {
			s.phase = installPhaseDone
			return nil
		}
		return s.openCurrent()

	case installPhaseDone:
		return nil

	default:
		return fmt.Errorf("unknown install phase: %d", s.phase)
	}
}

func (s *FullSink) closeFile() error {
	if s.f == nil {
		return nil
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	if err := s.f.Close(); err != nil {
		return err
	}
	s.f = nil
	s.crcW = nil
	return nil
}
