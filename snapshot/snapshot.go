package snapshot

import (
	"expvar"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/db"
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
	stream, err := s.OpenStream()
	if err != nil {
		return err
	}
	defer stream.Close()

	n, err := io.Copy(sink, stream)
	stats.Get(persistSize).(*expvar.Int).Set(n)
	return err
}

// Release is a no-op.
func (s *Snapshot) Release() {}

// OpenStream returns a stream for reading the snapshot.
func (s *Snapshot) OpenStream() (*Stream, error) {
	if len(s.files) > 0 {
		return NewFullStream(s.files...)
	}
	return NewIncrementalStream(s.walData)
}

// ReplayDB reconstructs the database from the given reader, and writes it to
// the given path.
func ReplayDB(fullSnap *FullSnapshot, r io.Reader, path string) error {
	dbInfo := fullSnap.GetDb()
	if dbInfo == nil {
		return fmt.Errorf("got nil DB info")
	}

	sqliteBaseFD, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("error creating SQLite file: %v", err)
	}
	defer sqliteBaseFD.Close()
	if _, err := io.CopyN(sqliteBaseFD, r, dbInfo.Size); err != nil {
		return fmt.Errorf("error writing SQLite file data: %v", err)
	}

	// Write out any WALs.
	var walFiles []string
	for i, wal := range fullSnap.GetWals() {
		if err := func() error {
			if wal == nil {
				return fmt.Errorf("got nil WAL")
			}

			walName := filepath.Join(filepath.Dir(path), baseSqliteWALFile+fmt.Sprintf("%d", i))
			walFD, err := os.Create(walName)
			if err != nil {
				return fmt.Errorf("error creating WAL file: %v", err)
			}
			defer walFD.Close()
			if _, err := io.CopyN(walFD, r, wal.Size); err != nil {
				return fmt.Errorf("error writing WAL file data: %v", err)
			}
			walFiles = append(walFiles, walName)
			return nil
		}(); err != nil {
			return err
		}
	}

	// Checkpoint the WAL files into the base SQLite file
	if err := db.ReplayWAL(path, walFiles, false); err != nil {
		return fmt.Errorf("error checkpointing WAL: %v", err)
	}
	return nil
}
