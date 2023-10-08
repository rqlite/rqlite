package snapshot2

import (
	"expvar"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

// Snapshot represents a snapshot of the database state.
type Snapshot struct {
	walData []byte
	files   []string
}

// NewWALSnapshot creates a new snapshot from a WAL.
func NewWALSnapshot(b []byte) *Snapshot {
	return &Snapshot{
		walData: b,
	}
}

// NewFullSnapshot creates a new snapshot from a SQLite file and WALs.
func NewFullSnapshot(files ...string) *Snapshot {
	return &Snapshot{
		files: files,
	}
}

// Persist writes the snapshot to the given sink.
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	startT := time.Now()
	stream, err := s.OpenStream()
	if err != nil {
		return err
	}
	defer stream.Close()

	n, err := io.Copy(sink, stream)
	if err != nil {
		return err
	}
	dur := time.Since(startT)
	stats.Get(persistSize).(*expvar.Int).Set(n)
	stats.Get(persistDuration).(*expvar.Int).Set(dur.Milliseconds())
	return err
}

// Release is a no-op.
func (s *Snapshot) Release() {}

// OpenStream returns a stream for reading the snapshot.
func (s *Snapshot) OpenStream() (*Stream, error) {
	return nil, nil
}
