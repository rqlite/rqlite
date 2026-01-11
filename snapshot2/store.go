package snapshot2

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
)

const (
	metaFileName = "meta.json"
	tmpSuffix    = ".tmp"
)

// Store stores snapshots in the Raft system.
type Store struct {
	dir    string
	logger *log.Logger
}

// NewStore creates a new store.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	str := &Store{
		dir:    dir,
		logger: log.New(os.Stderr, "[snapshot-store] ", log.LstdFlags),
	}
	str.logger.Printf("store initialized using %s", dir)

	emp, err := dirIsEmpty(dir)
	if err != nil {
		return nil, err
	}
	if !emp {
		if err := str.check(); err != nil {
			return nil, fmt.Errorf("check failed: %s", err)
		}
	}
	return str, nil
}

func (s *Store) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	sink := NewSink(s.dir, &raft.SnapshotMeta{
		Version:            version,
		ID:                 snapshotName(term, index),
		Index:              index,
		Term:               term,
		Configuration:      configuration,
		ConfigurationIndex: configurationIndex,
	})
	if err := sink.Open(); err != nil {
		return nil, err
	}
	s.logger.Printf("created new snapshot sink: index=%d, term=%d", index, term)
	return sink, nil
}

func (s *Store) List() ([]*raft.SnapshotMeta, error) {
	return nil, nil
}

func (s *Store) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, nil
}

// check checks the Store for any inconsistencies, and repairs
// any inconsistencies it finds. Inconsistencies can happen
// if the system crashes during snapshotting.
func (s *Store) check() error {
	return nil
}

// snapshotName generates a name for the snapshot.
func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

// metaPath returns the path to the meta file in the given directory.
func metaPath(dir string) string {
	return filepath.Join(dir, metaFileName)
}
