package snapshot2

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
)

// Sink is a sink for writing snapshot data to a Snapshot store.
type Sink struct {
	dir  string
	meta *raft.SnapshotMeta

	snapDirPath    string
	snapTmpDirPath string
	dataFD         *os.File
	opened         bool
}

// NewSink creates a new Sink object.
func NewSink(dir string, meta *raft.SnapshotMeta) *Sink {
	return &Sink{
		dir:  dir,
		meta: meta,
	}
}

// Open opens the sink for writing.
func (s *Sink) Open() error {
	if s.opened {
		return nil
	}
	s.opened = true

	// Make temp snapshot directory
	s.snapDirPath = filepath.Join(s.dir, s.meta.ID)
	s.snapTmpDirPath = tmpName(s.snapDirPath)
	if err := os.MkdirAll(s.snapTmpDirPath, 0755); err != nil {
		return err
	}

	dataPath := filepath.Join(s.snapTmpDirPath, "data")
	dataFD, err := os.Create(dataPath)
	if err != nil {
		return err
	}
	s.dataFD = dataFD
	return nil
}

// ID returns the ID of the snapshot.
func (s *Sink) ID() string {
	return s.meta.ID
}

// Write writes snapshot data to the sink.
func (s *Sink) Write(p []byte) (n int, err error) {
	return s.dataFD.Write(p)
}

// Close closes the sink.
func (s *Sink) Close() error {
	if !s.opened {
		return nil
	}
	s.opened = false

	if err := s.dataFD.Close(); err != nil {
		return err
	}

	if err := writeMeta(s.snapTmpDirPath, s.meta); err != nil {
		return err
	}

	if err := os.Rename(s.snapTmpDirPath, s.snapDirPath); err != nil {
		return err
	}
	return syncDirMaybe(s.snapDirPath)
}

// Cancel cancels the sink.
func (s *Sink) Cancel() error {
	if !s.opened {
		return nil
	}
	s.opened = false
	if err := s.dataFD.Close(); err != nil {
		return err
	}
	s.dataFD = nil
	return os.RemoveAll(s.snapTmpDirPath)
}

// writeMeta is used to write the meta data in a given snapshot directory.
func writeMeta(dir string, meta *raft.SnapshotMeta) error {
	fh, err := os.Create(metaPath(dir))
	if err != nil {
		return fmt.Errorf("error creating meta file: %v", err)
	}
	defer fh.Close()

	// Write out as JSON
	enc := json.NewEncoder(fh)
	if err = enc.Encode(meta); err != nil {
		return fmt.Errorf("failed to encode meta: %v", err)
	}

	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}
