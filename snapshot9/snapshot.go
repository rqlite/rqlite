package snapshot9

import (
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
)

// LockingSnapshot is a snapshot which holds the Snapshot Store CAS while open.
type LockingSnapshot struct {
	rc  io.ReadCloser
	str *ReferentialStore

	mu     sync.Mutex
	closed bool
}

// NewLockingSink returns a new LockingSink.
func NewLockingSnapshot(rc io.ReadCloser, str *ReferentialStore) *LockingSnapshot {
	return &LockingSnapshot{
		rc:  rc,
		str: str,
	}
}

// Read reads from the snapshot.
func (l *LockingSnapshot) Read(p []byte) (n int, err error) {
	return l.rc.Read(p)
}

// Close closes the Snapshot and releases the Snapshot Store lock.
func (l *LockingSnapshot) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	defer l.str.mrsw.EndRead()
	return l.rc.Close()
}

// Snapshot represents a snapshot of the database state.
type Snapshot struct {
	rc     io.ReadCloser
	logger *log.Logger
}

// NewSnapshot creates a new snapshot.
func NewSnapshot(rc io.ReadCloser) *Snapshot {
	return &Snapshot{
		rc:     rc,
		logger: log.New(log.Writer(), "[snapshot] ", log.LstdFlags),
	}
}

// Persist writes the snapshot to the given sink.
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

// Release releases the snapshot.
func (s *Snapshot) Release() {
	// Necessary in case Persist() is never called.
	s.rc.Close()
}
