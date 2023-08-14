package snapshot

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"
)

type Sink struct {
	str *Store
	dir string // Directory for the Snapshot

	walFilePath string

	dataFD *os.File // File the snapshot data is written to

	logger *log.Logger
	closed bool
}

func NewSink(str *Store, dir string, meta *Meta) *Sink {
	tDir := tmpName(dir)
	return &Sink{
		str:         str,
		dir:         tDir,
		walFilePath: filepath.Join(tDir, snapWALFile),
		logger:      log.New(os.Stderr, "[snapshot-sink] ", log.LstdFlags),
	}
}

// Open opens the snapshot.
func (s *Sink) Open() error {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return err
	}
	return nil
}

// Write writes the given bytes to the snapshot.
func (s *Sink) Write(p []byte) (n int, err error) {
	if s.dataFD == nil {
		f, err := os.CreateTemp(s.dir, "snapshot-data.tmp")
		if err != nil {
			return 0, err
		}
		s.dataFD = f
	}
	return s.dataFD.Write(p)
}

// Close closes the snapshot.
func (s *Sink) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	if s.dataFD != nil {
		defer s.dataFD.Close()
		if err := s.processSnapshotData(); err != nil {
			return err
		}
	}
	return nil
}

// stub out ID() and Cancel() on SnapshotSink
func (s *Sink) ID() string {
	return ""
}

func (s *Sink) Cancel() error {
	return nil
}

func (s *Sink) processSnapshotData() error {
	if _, err := s.dataFD.Seek(0, 0); err != nil {
		return err
	}

	buf := make([]byte, sizeofHeader)
	_, err := io.ReadFull(s.dataFD, buf)
	if err != nil {
		return fmt.Errorf("unexpected error reading header %v", err)
	}
	h, err := DecodeHeader(buf)
	if err != nil {
		return fmt.Errorf("unexpected error decoding header %v", err)
	}

	buf = make([]byte, h.SnapshotHeaderSize)
	_, err = io.ReadFull(s.dataFD, buf)
	if err != nil {
		return fmt.Errorf("unexpected error reading snapshot header %v", err)
	}
	fsmSnap := &FSMSnapshot{}
	err = proto.Unmarshal(buf, fsmSnap)
	if err != nil {
		return fmt.Errorf("unexpected error unmarshaling FSM snapshot: %v", err)
	}

	// Incremental snapshot?
	if incSnap := fsmSnap.GetIncrementalSnapshot(); incSnap != nil {
		s.logger.Printf("processing incremental snapshot")
		return os.WriteFile(s.walFilePath, incSnap.Data, 0644)
	}

	// Process a full snapshot. We need to write the WAL data to the
	// snapshot directory, and the base SQLite data to the parent directory.

	// Let's just easy path for now, where we don't worry about resetting the store in response
	// to a FULL snapshot. Just assume it's the first. Won't run for every long though, I have
	// tests with missing logs.

	// Write a marker file in the parent directory to indicate that we're restoring a full snapshot.
	if err := os.WriteFile(filepath.Join(s.dir, "RESTORING-<SNAP ID>"), nil, 0644); err != nil {
		return err
	}

	// Not sure if I need marker file. The following could happen:
	// - if there is a .tmp snapshot directory left, it will be cleaned up
	// - this would leave a SQLite directory (and some WAL files) in the directory without any snapshot? But what about old snapshots? that might given the impression
	// - that the SQLite is OK. This indicates older snapshot directories should be deleted if they exist. They are probably useless if a full snapshot is getting
	// - written to here, right? This implies RESTORING might be useful.
	// SIMPLY REMOVING EXISTING SQLITE file means that the node couldn't start up properly, since restart requires a snapshot
	// and whatever logs are available in the Raft log.

	// Installing a full snapshot can't result in data loss, bad.
	// So how about renaming any existing SQLite file to the name of the latest snapshot directory?
	// And then on restart if RESTORING- exists, then remove any base SQLite file, and restore the
	// named SQLite to its rightful place. Delete any snapshot dirs newer than name.
	// If RESTORING- is not there, but there is a SQLite named after a snapshot, then it must be named
	// after the second newest one. We know we can then delete that SQLite file, and any snapshots
	// of that name (and older).
	return nil
}
