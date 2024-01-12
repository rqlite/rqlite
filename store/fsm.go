package store

import (
	"expvar"
	"io"
	"log"
	"time"

	"github.com/hashicorp/raft"
)

// FSM is a wrapper around the Store which implements raft.FSM.
type FSM struct {
	s *Store
}

// NewFSM returns a new FSM.
func NewFSM(s *Store) *FSM {
	return &FSM{s: s}
}

// Apply applies a Raft log entry to the Store.
func (f *FSM) Apply(l *raft.Log) interface{} {
	return f.s.fsmApply(l)
}

// Snapshot returns a Snapshot of the Store
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return f.s.fsmSnapshot()
}

// Restore restores the Store from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	return f.s.fsmRestore(rc)
}

// FSMSnapshot is a wrapper around raft.FSMSnapshot which adds instrumentation and
// logging.
type FSMSnapshot struct {
	raft.FSMSnapshot
	logger *log.Logger
}

// Persist writes the snapshot to the given sink.
func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) (retError error) {
	startT := time.Now()
	defer func() {
		if retError == nil {
			dur := time.Since(startT)
			stats.Get(snapshotPersistDuration).(*expvar.Int).Set(dur.Milliseconds())
			f.logger.Printf("persisted snapshot %s in %s", sink.ID(), time.Since(startT))
		}
	}()
	return f.FSMSnapshot.Persist(sink)
}

// Release is a no-op.
func (f *FSMSnapshot) Release() {
	f.FSMSnapshot.Release()
}
