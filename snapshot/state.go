package snapshot

import (
	"expvar"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v9/internal/progress"
)

// RemoveAllTmpSnapshotData removes all temporary Snapshot data from the directory.
// This process is defined as follows: for every directory in dir, if the directory
// is a temporary directory, remove the directory. Then remove all other files
// that contain the name of a temporary directory, minus the temporary suffix,
// as prefix.
func RemoveAllTmpSnapshotData(dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	for _, d := range files {
		// If the directory is a temporary directory, remove it.
		if d.IsDir() && isTmpName(d.Name()) {
			files, err := filepath.Glob(filepath.Join(dir, nonTmpName(d.Name())) + "*")
			if err != nil {
				return err
			}

			fullTmpDirPath := filepath.Join(dir, d.Name())
			for _, f := range files {
				if f == fullTmpDirPath {
					// Delete the directory last as a sign the deletion is complete.
					continue
				}
				if err := os.Remove(f); err != nil {
					return err
				}
			}
			if err := os.RemoveAll(fullTmpDirPath); err != nil {
				return err
			}
		}
	}
	return nil
}

// LatestIndexTerm returns the index and term of the latest snapshot in the given directory.
func LatestIndexTerm(dir string) (uint64, uint64, error) {
	catalog := &SnapshotCatalog{}
	snapSet, err := catalog.Scan(dir)
	if err != nil {
		return 0, 0, err
	}
	snap, ok := snapSet.Newest()
	if !ok {
		return 0, 0, nil
	}
	meta := snap.Meta()
	return meta.Index, meta.Term, nil
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

	dur := time.Since(startT)
	stats.Get(persistSize).(*expvar.Int).Set(n)
	stats.Get(persistDuration).(*expvar.Int).Set(dur.Milliseconds())
	return err
}

// Release releases the StateReader.
func (s *StateReader) Release() {
	// Ensure that the source data for the snapshot is closed regardless of
	// whether the snapshot is persisted or not.
	s.rc.Close()
}
