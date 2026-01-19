package snapshot2

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v9/db"
	"github.com/rqlite/rqlite/v9/snapshot2/proto"
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

// FullSink streams snapshot bytes into files described by SnapshotHeader.
type FullSink struct {
	dir    string
	header *proto.SnapshotHeader

	phase    installPhase
	walIndex int

	f         *os.File
	remaining uint64

	dbFile   string
	walFiles []string

	opened bool
}

// NewFullSink creates a new FullSink object.
func NewFullSink(dir string, hdr *proto.SnapshotHeader) *FullSink {
	s := &FullSink{
		dir:    dir,
		header: hdr,
		dbFile: filepath.Join(dir, "data.db"),
	}

	for i := range hdr.WalHeaders {
		walPath := filepath.Join(dir, fmt.Sprintf("data-%08d.wal", i))
		s.walFiles = append(s.walFiles, walPath)
	}
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

		n, err := s.f.Write(chunk)
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

// Close closes the sink. It fails if not all bytes were written.
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
				_ = s.closeFile()
				return err
			}
		}
		if s.phase != installPhaseDone {
			_ = s.closeFile()
			return ErrIncomplete
		}
	}

	if !db.IsValidSQLiteFile(s.dbFile) {
		_ = s.closeFile()
		return ErrInvalidSQLiteFile
	}
	for i, walPath := range s.walFiles {
		if !db.IsValidSQLiteWALFile(walPath) {
			_ = s.closeFile()
			return fmt.Errorf("WAL file %d invalid: %w", i, ErrInvalidWALFile)
		}
	}

	// This is when we checkpoint all WALs into the SQLite file, and end up
	// with a single DB file representing the snapshot state.

	return s.closeFile()
}

// DBFile returns the path to the installed DB file.
func (s *FullSink) DBFile() string {
	return s.dbFile
}

// WALFiles returns the paths to the installed WAL files.
func (s *FullSink) WALFiles() []string {
	return s.walFiles
}

// NumWALFiles returns the number of WAL files.
func (s *FullSink) NumWALFiles() int {
	return len(s.walFiles)
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
		path := filepath.Join(s.dir, dbfileName)
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		s.f = f
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
		s.remaining = s.header.WalHeaders[s.walIndex].SizeBytes
		return nil

	case installPhaseDone:
		return nil

	default:
		return fmt.Errorf("unknown install phase: %d", s.phase)
	}
}

func (s *FullSink) advance() error {
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
	err := s.f.Close()
	s.f = nil
	return err
}
