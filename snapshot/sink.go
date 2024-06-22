package snapshot

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"
)

// LockingSink is a wrapper around a SnapshotSink holds the CAS lock
// while the Sink is in use.
type LockingSink struct {
	raft.SnapshotSink
	str *ReferentialStore

	mu     sync.Mutex
	closed bool
}

// NewLockingSink returns a new LockingSink.
func NewLockingSink(sink raft.SnapshotSink, str *ReferentialStore) *LockingSink {
	return &LockingSink{
		SnapshotSink: sink,
		str:          str,
	}
}

// Close closes the sink, unlocking the Store for creation of a new sink.
func (s *LockingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.str.mrsw.EndWrite()
	return s.SnapshotSink.Close()
}

// Cancel cancels the sink, unlocking the Store for creation of a new sink.
func (s *LockingSink) Cancel() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.str.mrsw.EndWrite()
	return s.SnapshotSink.Cancel()
}

// Sink is a sink for writing snapshot data to a Snapshot store.
type Sink struct {
	str  *ReferentialStore
	meta *raft.SnapshotMeta

	snapDirPath    string
	snapTmpDirPath string
	dataFD         *os.File
	opened         bool
}

// NewSink creates a new Sink object.
func NewSink(str *ReferentialStore, meta *raft.SnapshotMeta) *Sink {
	return &Sink{
		str:  str,
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
	s.snapDirPath = filepath.Join(s.str.Dir(), s.meta.ID)
	s.snapTmpDirPath = tmpName(s.snapDirPath)
	if err := os.MkdirAll(s.snapTmpDirPath, 0755); err != nil {
		return err
	}

	dataPath := filepath.Join(s.snapTmpDirPath, stateFileName)
	dataFD, err := os.Create(dataPath)
	if err != nil {
		return err
	}
	s.dataFD = dataFD
	return nil
}

// Write writes snapshot data to the sink. The snapshot is not in place
// until Close is called.
func (s *Sink) Write(p []byte) (n int, err error) {
	return s.dataFD.Write(p)
}

// ID returns the ID of the snapshot being written.
func (s *Sink) ID() string {
	return s.meta.ID
}

// Cancel cancels the snapshot. Cancel must be called if the snapshot is not
// going to be closed.
func (s *Sink) Cancel() error {
	if !s.opened {
		return nil
	}
	s.opened = false

	if err := s.dataFD.Close(); err != nil {
		return err
	}
	return RemoveAllTmpSnapshotData(s.str.Dir())
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

	// Write meta data
	if err := s.writeMeta(s.snapTmpDirPath); err != nil {
		return err
	}

	// Indicate snapshot data been successfully persisted to disk by renaming
	// the temp directory to a non-temporary name.
	if err := os.Rename(s.snapTmpDirPath, s.snapDirPath); err != nil {
		return err
	}
	if err := syncDirMaybe(s.str.Dir()); err != nil {
		return err
	}

	_, err := s.str.Reap()
	return err
}

func (s *Sink) writeMeta(dir string) error {
	return writeMeta(dir, s.meta)
}
