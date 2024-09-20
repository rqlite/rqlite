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

// FSMSnapshot is a wrapper around raft.FSMSnapshot which adds an optional
// Finalizer, instrumentation, and logging.
type FSMSnapshot struct {
	Finalizer func() error
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
			if f.logger != nil {
				f.logger.Printf("persisted snapshot %s in %s", sink.ID(), dur)
			}
		}
	}()
	if err := f.FSMSnapshot.Persist(sink); err != nil {
		return err
	}
	if f.Finalizer != nil {
		return f.Finalizer()
	}
	return nil
}

// Release performs any final cleanup once the Snapshot has been persisted.
func (f *FSMSnapshot) Release() {
	f.FSMSnapshot.Release()
}
