package snapshot2

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

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

	// ErrManifestInvalid indicates the manifest is invalid.
	ErrManifestInvalid = errors.New("snapshot install manifest invalid")
)

type installPhase int

const (
	installPhaseDB installPhase = iota
	installPhaseWAL
	installPhaseDone
)

// InstallSink streams snapshot bytes into files described by SnapshotInstall.
type InstallSink struct {
	dir      string
	manifest *proto.SnapshotInstall

	phase    installPhase
	walIndex int

	f         *os.File
	remaining uint64

	opened bool
}

// NewInstallSink creates a new InstallSink object.
func NewInstallSink(dir string, m *proto.SnapshotInstall) *InstallSink {
	return &InstallSink{dir: dir, manifest: m}
}

// Open opens the sink for writing.
func (s *InstallSink) Open() error {
	if s.opened {
		return ErrSinkOpen
	}
	if err := s.validateManifest(); err != nil {
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
func (s *InstallSink) Write(p []byte) (int, error) {
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
func (s *InstallSink) Close() error {
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

	return s.closeFile()
}

func (s *InstallSink) validateManifest() error {
	if s.manifest == nil || s.manifest.DbFile == nil {
		return ErrManifestInvalid
	}
	return nil
}

func (s *InstallSink) openCurrent() error {
	switch s.phase {
	case installPhaseDB:
		path := filepath.Join(s.dir, "data.db")
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		s.f = f
		s.remaining = s.manifest.DbFile.SizeBytes
		return nil

	case installPhaseWAL:
		if s.walIndex >= len(s.manifest.WalFiles) {
			s.phase = installPhaseDone
			return nil
		}

		// Do not overwrite the same WAL file name.
		// Pick a deterministic sequence name; align this with your snapshot-store conventions.
		path := filepath.Join(s.dir, fmt.Sprintf("data-%08d.wal", s.walIndex))
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		s.f = f
		s.remaining = s.manifest.WalFiles[s.walIndex].SizeBytes
		return nil

	case installPhaseDone:
		return nil

	default:
		return fmt.Errorf("unknown install phase: %d", s.phase)
	}
}

func (s *InstallSink) advance() error {
	// Close current artifact if open.
	if err := s.closeFile(); err != nil {
		return err
	}

	switch s.phase {
	case installPhaseDB:
		// DB complete; move to WAL or done.
		if len(s.manifest.WalFiles) == 0 {
			s.phase = installPhaseDone
			return nil
		}
		s.phase = installPhaseWAL
		s.walIndex = 0
		return s.openCurrent()

	case installPhaseWAL:
		// Current WAL complete; move to next WAL or done.
		s.walIndex++
		if s.walIndex >= len(s.manifest.WalFiles) {
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

func (s *InstallSink) closeFile() error {
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
