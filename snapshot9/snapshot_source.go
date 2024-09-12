package snapshot9

import (
	"io"
	"os"
)

// SnapshotSource is a Source that reads from a file
// and implementes the interface required by the snapshot package.
type SnapshotSource struct {
	path string
}

// NewSnapshotSource returns a new SnapshotSource.
func NewSnapshotSource(path string) Source {
	return &SnapshotSource{path}
}

// Open returns a snapshot.Proof and a ReadCloser for SnapshotSource data.
func (s *SnapshotSource) Open() (*Proof, io.ReadCloser, error) {
	proof, err := NewProofFromFile(s.path)
	if err != nil {
		return nil, nil, err
	}
	fd, err := os.Open(s.path)
	if err != nil {
		return nil, nil, err
	}
	return proof, fd, nil
}
