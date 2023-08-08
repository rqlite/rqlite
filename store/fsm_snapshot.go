package store

import (
	"expvar"
	"log"
	"time"

	"github.com/hashicorp/raft"
)

// FSMSnapshot is a snapshot of the SQLite database.
type FSMSnapshot struct {
	startT time.Time
	logger *log.Logger

	data []byte
}

// NewFSMSnapshot creates a new FSMSnapshot.
func NewFSMSnapshot(logger *log.Logger) *FSMSnapshot {
	return &FSMSnapshot{
		startT: time.Now(),
		logger: logger,
	}
}

// Persist writes the snapshot to the given sink.
func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer func() {
		dur := time.Since(f.startT)
		stats.Get(snapshotPersistDuration).(*expvar.Int).Set(dur.Milliseconds())
		f.logger.Printf("snapshot and persist took %s", dur)
	}()

	err := func() error {
		if _, err := sink.Write(f.data); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release is a no-op.
func (f *FSMSnapshot) Release() {}
