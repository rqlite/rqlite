package store

import (
	"expvar"
	"log"
	"time"

	"github.com/hashicorp/raft"
)

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
