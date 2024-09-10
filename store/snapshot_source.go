package store

import (
	"io"
	"os"

	"github.com/rqlite/rqlite/v8/snapshot9"
)

// SnapshotSource is a snapshot.Source that reads from a file
// and implementes the interface required by the snapshot package.
type SnapshotSource struct {
	path string
}

// NewSnapshotSource returns a new SnapshotSource.
func NewSnapshotSource(path string) snapshot9.Source {
	return &SnapshotSource{path}
}

// Open returns a snapshot.Proof and a ReadCloser for SnapshotSource data.
func (s *SnapshotSource) Open() (*snapshot9.Proof, io.ReadCloser, error) {
	proof, err := snapshot9.NewProofFromFile(s.path)
	if err != nil {
		return nil, nil, err
	}
	fd, err := os.Open(s.path)
	if err != nil {
		return nil, nil, err
	}
	return proof, fd, nil
}
