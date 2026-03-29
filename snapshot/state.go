package snapshot

import (
	"expvar"
	"io"
	"log"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v10/internal/progress"
)

// LatestIndexTerm returns the index and term of the most recent snapshot
// in the given directory. If no snapshots are found, it returns 0, 0, nil.
func LatestIndexTerm(dir string) (uint64, uint64, error) {
	cat := &SnapshotCatalog{}
	sset, err := cat.Scan(dir)
	if err != nil {
		return 0, 0, err
	}
	newest, ok := sset.Newest()
	if !ok {
		return 0, 0, nil
	}
	return newest.raftMeta.Index, newest.raftMeta.Term, nil
}

// StateReader represents a snapshot of the database state.
type StateReader struct {
	rc     io.ReadCloser
	logger *log.Logger
}

// NewStateReader creates a new StateReader.
func NewStateReader(rc io.ReadCloser) *StateReader {
	return &StateReader{
		rc:     rc,
		logger: log.New(log.Writer(), "[snapshot] ", log.LstdFlags),
	}
}

// Persist writes the State to the given sink.
func (s *StateReader) Persist(sink raft.SnapshotSink) error {
	defer s.rc.Close()
	startT := time.Now()

	cw := progress.NewCountingWriter(sink)
	cm := progress.StartCountingMonitor(func(n int64) {
		s.logger.Printf("persisted %d bytes", n)
	}, cw)
	n, err := func() (int64, error) {
		defer cm.StopAndWait()
		return io.Copy(cw, s.rc)
	}()
	if err != nil {
		return err
	}

	stats.Get(persistSize).(*expvar.Int).Set(n)
	recordDuration(persistDuration, startT)
	return err
}

// Release releases the StateReader.
func (s *StateReader) Release() {
	// Ensure that the source data for the snapshot is closed regardless of
	// whether the snapshot is persisted or not.
	s.rc.Close()
}
