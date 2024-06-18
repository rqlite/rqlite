package snapshot9

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
	return nil
}

// Write writes snapshot data to the sink. The snapshot is not in place
// until Close is called.
func (s *Sink) Write(p []byte) (n int, err error) {
	return 0, nil
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
	return nil
}

// Close closes the sink, and finalizes creation of the snapshot. It is critical
// that Close is called, or the snapshot will not be in place. It is OK to call
// Close without every calling Write. In that case the Snapshot will be finalized
// as usual, but will effectively be the same as the previously created snapshot.
func (s *Sink) Close() error {
	if !s.opened {
		return nil
	}
	s.opened = false
	return nil
}
