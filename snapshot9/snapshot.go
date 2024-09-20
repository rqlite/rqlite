package snapshot9

import (
	"expvar"
	"io"
	"log"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/progress"
)

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
	defer s.rc.Close()
	startT := time.Now()

	cw := progress.NewCountingWriter(sink)
	cm := progress.StartCountingMonitor(func(n int64) {
		s.logger.Printf("persisted %d bytes", n)
	}, cw)
	n, err := func() (int64, error) {
		defer cm.StopAndWait()
		_, err := io.Copy(cw, s.rc)
		if err != nil {
			return cw.Count(), sink.Cancel()
		}
		return cw.Count(), sink.Close()
	}()
	if err != nil {
		return err
	}

	dur := time.Since(startT)
	stats.Get(persistSize).(*expvar.Int).Set(n)
	stats.Get(persistDuration).(*expvar.Int).Set(dur.Milliseconds())
	return err
}

// Release releases the snapshot.
func (s *Snapshot) Release() {
	// Necessary in case Persist() is never called.
	s.rc.Close()
}
