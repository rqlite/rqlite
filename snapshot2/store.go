package snapshot2

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v9/db"
)

const (
	metaFileName = "meta.json"
	tmpSuffix    = ".tmp"
)

type SnapshotMetaType int

// SnapshotMetaType is an enum
const (
	SnapshotMetaTypeUnknown SnapshotMetaType = iota
	SnapshotMetaTypeFull
	SnapshotMetaTypeIncremental
)

// SnapshotMeta represents metadata about a snapshot.
type SnapshotMeta struct {
	*raft.SnapshotMeta
	Filename string
	Type     SnapshotMetaType
}

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

// Create creates a new snapshot sink for the given parameters.
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

// List returns the list of available snapshots.
func (s *Store) List() ([]*raft.SnapshotMeta, error) {
	ms, err := getSnapshots(s.dir)
	if err != nil {
		return nil, err
	}
	return snapMetaSlice(ms).RaftMetaSlice(), nil
}

// Open opens the snapshot with the given ID for reading.
func (s *Store) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, nil
}

// check checks the Store for any inconsistencies, and repairs
// any inconsistencies it finds. Inconsistencies can happen
// if the system crashes during snapshotting.
func (s *Store) check() error {
	return nil
}

// getSnapshots returns the list of snapshots in the given directory,
// sorted from newest to oldest.
func getSnapshots(dir string) ([]*SnapshotMeta, error) {
	// Get the eligible snapshots
	snapshots, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// Populate the metadata
	var snapMeta []*SnapshotMeta
	for _, snap := range snapshots {
		// Snapshots are stored in directories, ignore any files.
		if !snap.IsDir() {
			continue
		}

		// Ignore any temporary snapshots
		if isTmpName(snap.Name()) {
			continue
		}

		// Read the meta data
		meta, err := readMeta(filepath.Join(dir, snap.Name()))
		if err != nil {
			return nil, fmt.Errorf("failed to read meta for snapshot %s: %s", snap.Name(), err)
		}

		path := filepath.Join(dir, snap.Name(), meta.Filename)
		if db.IsValidSQLiteFile(path) {
			meta.Type = SnapshotMetaTypeFull
		} else if db.IsValidSQLiteWALFile(path) {
			meta.Type = SnapshotMetaTypeIncremental
		} else {
			return nil, fmt.Errorf("snapshot %s does not contain a valid SQLite or WAL file", snap.Name())
		}
		snapMeta = append(snapMeta, meta)
	}

	sort.Sort(snapMetaSlice(snapMeta))
	return snapMeta, nil
}

type cmpSnapshotMeta SnapshotMeta

func (c *cmpSnapshotMeta) Less(other *cmpSnapshotMeta) bool {
	if c.Term != other.Term {
		return c.Term < other.Term
	}
	if c.Index != other.Index {
		return c.Index < other.Index
	}
	return c.ID < other.ID
}

type snapMetaSlice []*SnapshotMeta

// Len implements the sort interface for snapMetaSlice.
func (s snapMetaSlice) Len() int {
	return len(s)
}

// Less implements the sort interface for snapMetaSlice.
func (s snapMetaSlice) Less(i, j int) bool {
	si := (*cmpSnapshotMeta)(s[i])
	sj := (*cmpSnapshotMeta)(s[j])
	return si.Less(sj)
}

// Swap implements the sort interface for snapMetaSlice.
func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// RaftMetaSlice converts the snapMetaSlice to a slice of raft.SnapshotMeta.
func (s snapMetaSlice) RaftMetaSlice() []*raft.SnapshotMeta {
	if s == nil {
		return nil
	}
	r := make([]*raft.SnapshotMeta, len(s))
	for i, sm := range s {
		r[i] = sm.SnapshotMeta
	}
	return r
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

// readMeta is used to read the meta data in a given snapshot directory.
func readMeta(dir string) (*SnapshotMeta, error) {
	fh, err := os.Open(metaPath(dir))
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	meta := &SnapshotMeta{}
	dec := json.NewDecoder(fh)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}
