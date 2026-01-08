package snapshot2

import (
	"io"
	"log"

	"github.com/hashicorp/raft"
)

// Snapshot represents a snapshot of the database state.
type Snapshot struct {
	logger *log.Logger
}

// NewSnapshot creates a new snapshot.
func NewSnapshot() *Snapshot {
	return &Snapshot{
		logger: log.New(log.Writer(), "[snapshot] ", log.LstdFlags),
	}
}

func (s *Snapshot) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	return nil, nil
}

func (s *Snapshot) List() ([]*raft.SnapshotMeta, error) {
	return nil, nil
}

func (s *Snapshot) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, nil
}
