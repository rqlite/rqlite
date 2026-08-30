package snapshot

import (
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/internal/progress"
)

// Clone copies the snapshot located in dir, and indicated by id, and
// installs it in dir, but with the new index and term.
//
// The data files, and their CRC32 sidecars, are copied verbatim, so the clone
// holds exactly the database state of the snapshot it was made from -- only its
// identity changes. A clone is therefore of the same type, Full or Incremental,
// as its source.
//
// Clone exists for testing: it makes it possible to build a Snapshot Store
// holding a snapshot at an arbitrary index without driving a node to that index
// first. Clone returns an error if a snapshot with the resulting ID already
// exists, so it never overwrites one.
func Clone(dir, id string, index, term uint64) error {
	srcPath := filepath.Join(dir, id)
	if !fsutil.DirExists(srcPath) {
		return fmt.Errorf("snapshot %q not found in %q", id, dir)
	}
	meta, err := readRaftMeta(metaPath(srcPath))
	if err != nil {
		return fmt.Errorf("reading meta of snapshot %q: %w", id, err)
	}

	newID := snapshotName(term, index)
	dstPath := filepath.Join(dir, newID)
	if fsutil.PathExists(dstPath) {
		return fmt.Errorf("snapshot %q already exists in %q", newID, dir)
	}

	// Build the clone under a temporary name, so a partially-written copy is
	// never picked up by a scan of the Snapshot Store.
	tmpPath := tmpName(dstPath)
	if err := os.RemoveAll(tmpPath); err != nil {
		return fmt.Errorf("removing stale temporary directory %q: %w", tmpPath, err)
	}
	defer os.RemoveAll(tmpPath)
	if err := fsutil.CopyDir(srcPath, tmpPath); err != nil {
		return fmt.Errorf("copying snapshot %q: %w", id, err)
	}

	// Only the metadata needs rewriting: the data files are unchanged, so their
	// CRC32 sidecars remain correct.
	meta.ID = newID
	meta.Index = index
	meta.Term = term
	if err := writeMeta(tmpPath, meta); err != nil {
		return fmt.Errorf("writing meta of snapshot %q: %w", newID, err)
	}

	if err := fsutil.SyncDirMaybe(tmpPath); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dstPath); err != nil {
		return fmt.Errorf("renaming snapshot %q into place: %w", newID, err)
	}
	return fsutil.SyncDirMaybe(dir)
}

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
