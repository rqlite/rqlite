package snapshot2

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/db"
)

// Sink is a sink for writing snapshot data to a Snapshot store.
type Sink struct {
	str  *Store
	meta *raft.SnapshotMeta

	snapDirPath    string
	snapTmpDirPath string
	dataFD         *os.File
	opened         bool
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
	if s.opened {
		return nil
	}
	s.opened = true

	// Make temp snapshot directory
	s.snapDirPath = filepath.Join(s.str.Dir(), s.meta.ID)
	s.snapTmpDirPath = tmpName(s.snapDirPath)
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
	if !s.opened {
		return nil
	}
	s.opened = false
	if err := s.dataFD.Close(); err != nil {
		return err
	}
	s.dataFD = nil
	return RemoveAllTmpSnapshotData(s.str.Dir())
}

// Close closes the sink, and finalizes creation of the snapshot. It is critical
// that Close is called, or the snapshot will not be in place.
func (s *Sink) Close() error {
	if !s.opened {
		return nil
	}
	s.opened = false

	if err := s.dataFD.Close(); err != nil {
		return err
	}

	// Write meta data
	if err := s.writeMeta(s.snapTmpDirPath); err != nil {
		return err
	}

	if err := s.processSnapshotData(); err != nil {
		return err
	}

	// Get size of SQLite file and set in meta.
	dbPath, err := s.str.getDBPath()
	if err != nil {
		return err
	}
	fi, err := os.Stat(dbPath)
	if err != nil {
		return err
	}
	if err := updateMetaSize(s.snapDirPath, fi.Size()); err != nil {
		return err
	}

	_, err = s.str.Reap()
	return err
}

func (s *Sink) processSnapshotData() (retErr error) {
	defer func() {
		if retErr != nil {
			RemoveAllTmpSnapshotData(s.str.Dir())
		}
	}()

	// Check the state of the store before processing this new snapshot. This
	// allows us to perform some sanity checks on the incoming snapshot data.
	snapshots, err := s.str.getSnapshots()
	if err != nil {
		return err
	}

	if db.IsValidSQLiteFile(s.dataFD.Name()) {
		if err := os.Rename(s.dataFD.Name(), filepath.Join(s.str.Dir(), s.meta.ID+".db")); err != nil {
			return err
		}
	} else if db.IsValidSQLiteWALFile(s.dataFD.Name()) {
		if len(snapshots) == 0 {
			// We are trying to create our first snapshot from a WAL file, which is invalid.
			return fmt.Errorf("data for first snapshot is a WAL file")
		} else {
			// We have at least one previous snapshot. That means we should have a valid SQLite file
			// for the previous snapshot.
			snapPrev := snapshots[len(snapshots)-1]
			snapPrevDB := filepath.Join(s.str.Dir(), snapPrev+".db")
			if !db.IsValidSQLiteFile(snapPrevDB) {
				return fmt.Errorf("previous snapshot data is not a SQLite file: %s", snapPrevDB)
			}
		}
		if err := os.Rename(s.dataFD.Name(), filepath.Join(s.str.Dir(), s.meta.ID+".db-wal")); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("invalid snapshot data file: %s", s.dataFD.Name())
	}

	// Indicate snapshot data been successfully persisted to disk by renaming
	// the temp directory to a non-temporary name.
	if err := os.Rename(s.snapTmpDirPath, s.snapDirPath); err != nil {
		return err
	}
	if err := syncDirMaybe(s.str.Dir()); err != nil {
		return err
	}

	// Now check if we need to replay any WAL file into the previous SQLite file. This is
	// the final step of any snapshot process.
	snapshots, err = s.str.getSnapshots()
	if err != nil {
		return err
	}
	if len(snapshots) >= 2 {
		snapPrev := snapshots[len(snapshots)-2]
		snapNew := snapshots[len(snapshots)-1]
		snapPrevDB := filepath.Join(s.str.Dir(), snapPrev+".db")
		snapNewDB := filepath.Join(s.str.Dir(), snapNew+".db")
		snapNewWAL := filepath.Join(s.str.Dir(), snapNew+".db-wal")

		if db.IsValidSQLiteWALFile(snapNewWAL) {
			// The most recent snapshot was created from a WAL file, so we need to replay
			// that WAL file into the previous SQLite file.
			if err := os.Rename(snapPrevDB, snapNewDB); err != nil {
				return err
			}
			if err := openCloseDB(snapNewDB); err != nil {
				return err
			}
		}
	}
	if err := syncDirMaybe(s.str.Dir()); err != nil {
		return err
	}

	s.str.Reap()
	return nil
}

func (s *Sink) writeMeta(dir string) error {
	return writeMeta(dir, s.meta)
}
