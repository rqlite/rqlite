package snapshot2

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/db"
)

var (
	// ErrInvalidSnapshot is returned when a snapshot is invalid.
	ErrInvalidSnapshot = errors.New("invalid snapshot")

	// ErrInvalidStore is returned when a store is in an invalid state.
	ErrInvalidStore = errors.New("invalid store")
)

// Sink is a sink for writing snapshot data to a Snapshot store.
type Sink struct {
	str  *Store
	meta *raft.SnapshotMeta

	snapTmpDirPath string
	dataFD         *os.File
	closed         bool
}

// NewSink creates a new Sink object.
func NewSink(str *Store, meta *raft.SnapshotMeta) *Sink {
	return &Sink{
		str:  str,
		meta: meta,
	}
}

// Open opens the sink for writing.
func (s *Sink) Open() error {
	// Make temp snapshot directory
	s.snapTmpDirPath = filepath.Join(s.str.Dir(), tmpName(s.meta.ID))
	if err := os.MkdirAll(s.snapTmpDirPath, 0755); err != nil {
		return err
	}

	dataPath := filepath.Join(s.snapTmpDirPath, s.meta.ID+".data")
	dataFD, err := os.Create(dataPath)
	if err != nil {
		return err
	}
	s.dataFD = dataFD
	return nil
}

// Write writes snapshot data to the sink. The snapshot is not in place
// until Close is called.
func (s *Sink) Write(p []byte) (n int, err error) {
	return s.dataFD.Write(p)
}

// ID returns the ID of the snapshot being written.
func (s *Sink) ID() string {
	return s.meta.ID
}

// Cancel cancels the snapshot. Cancel must be called if the snapshot is not
// going to be closed.
func (s *Sink) Cancel() error {
	s.closed = true
	if err := s.dataFD.Close(); err != nil {
		return err
	}
	return RemoveAllTmpSnapshotData(s.str.Dir())
}

// Close closes the sink, and finalizes creation of the snapshot. It is critical
// that Close is called, or the snapshot will not be in place.
func (s *Sink) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.dataFD.Close()

	// Write meta data
	if err := s.writeMeta(s.snapTmpDirPath); err != nil {
		return err
	}

	if err := s.processSnapshotData(); err != nil {
		return err
	}

	_, err := s.str.Reap()
	return err
}

func (s *Sink) processSnapshotData() (retErr error) {
	defer func() {
		if retErr != nil {
			RemoveAllTmpSnapshotData(s.str.Dir())
		}
	}()

	if db.IsValidSQLiteFile(s.dataFD.Name()) {
		if err := os.Rename(s.dataFD.Name(), filepath.Join(s.str.Dir(), s.meta.ID+".db")); err != nil {
			return err
		}
	} else if db.IsValidSQLiteWALFile(s.dataFD.Name()) {
		if err := os.Rename(s.dataFD.Name(), filepath.Join(s.str.Dir(), s.meta.ID+".db-wal")); err != nil {
			return err
		}
	} else {
		return ErrInvalidSnapshot
	}

	// Indicate snapshot data been successfully persisted to disk.
	if err := os.Rename(s.snapTmpDirPath, nonTmpName(s.snapTmpDirPath)); err != nil {
		return err
	}
	if err := syncDirMaybe(s.str.Dir()); err != nil {
		return err
	}

	// Now finalize the snapshot, so it's ready for use.
	snapshots, err := s.str.getSnapshots()
	if err != nil {
		return err
	}
	if len(snapshots) == 1 {
		// We just created our first snapshot. Nothing else to do, except
		// double-check that it's valid.
		snapDB := filepath.Join(s.str.Dir(), snapshots[0]+".db")
		if !db.IsValidSQLiteFile(snapDB) {
			return ErrInvalidStore
		}
		return nil
	} else if len(snapshots) >= 2 {
		snapPrev := snapshots[len(snapshots)-2]
		snapNew := snapshots[len(snapshots)-1]
		snapPrevDB := filepath.Join(s.str.Dir(), snapPrev+".db")
		snapNewDB := filepath.Join(s.str.Dir(), snapNew+".db")
		snapNewWAL := filepath.Join(s.str.Dir(), snapNew+".db-wal")

		if db.IsValidSQLiteWALFile(snapNewWAL) {
			// Double-check that the previous snapshot is a valid SQLite file.
			if !db.IsValidSQLiteFile(snapPrevDB) {
				return ErrInvalidStore
			}
			// It is, so rename it and replay the WAL into it.
			if err := os.Rename(snapPrevDB, snapNewDB); err != nil {
				return err
			}
			if err := db.ReplayWAL(snapNewDB, []string{snapNewWAL}, false); err != nil {
				return err
			}
		} else if !db.IsValidSQLiteFile(snapNewDB) {
			// There is no valid WAL file for the latest snapshot, and no valid
			// SQLite file for the latest snapshot. This is an invalid state.
			return ErrInvalidStore
		}
	} else {
		return ErrInvalidStore
	}

	s.str.Reap()
	return nil
}

func (s *Sink) writeMeta(dir string) error {
	return writeMeta(dir, s.meta)
}
