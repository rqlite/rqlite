package store

import (
	"io"
	"os"

	sql "github.com/rqlite/rqlite/v8/db"
	"github.com/rqlite/rqlite/v8/snapshot"
)

// SnapshotSource is a snapshot.Source that reads from a SwappableDB
// and implementes the interface required by the snapshot package.
type SnapshotSource struct {
	db *sql.SwappableDB
}

// NewSnapshotSource returns a new SnapshotSource.
func NewSnapshotSource(db *sql.SwappableDB) snapshot.Source {
	if db == nil {
		panic("nil database passed to NewSnapshotSource")
	}
	return &SnapshotSource{db}
}

// Open returns a snapshot.Proof and a ReadCloser for SnapshotSource data.
func (s *SnapshotSource) Open() (*snapshot.Proof, io.ReadCloser, error) {
	proof, err := snapshot.NewProofFromFile(s.db.Path())
	if err != nil {
		return nil, nil, err
	}
	fd, err := os.Open(s.db.Path())
	if err != nil {
		return nil, nil, err
	}
	return proof, fd, nil
}
