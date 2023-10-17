package snapshot2

import (
	"expvar"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

// Snapshot represents a snapshot of the database state.
type Snapshot struct {
	rc io.ReadCloser
}

// NewSnapshot creates a new snapshot.
func NewSnapshot(rc io.ReadCloser) *Snapshot {
	return &Snapshot{
		rc: rc,
	}
}

// Persist writes the snapshot to the given sink.
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	startT := time.Now()

	n, err := io.Copy(sink, s.rc)
	if err != nil {
		sink.Cancel() // Best effort
		return err
	}
	if err := sink.Close(); err != nil {
		return err
	}

	dur := time.Since(startT)
	stats.Get(persistSize).(*expvar.Int).Set(n)
	stats.Get(persistDuration).(*expvar.Int).Set(dur.Milliseconds())
	return err
}

// Release releases the snapshot.
func (s *Snapshot) Release() {
	s.rc.Close()
}
