package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/hashicorp/raft"
)

// Snapshot represents a single snapshot stored on disk.
//
// A Snapshot corresponds to exactly one directory under the Store root. The
// directory name is the snapshot ID (typically derived from term, index, and a
// timestamp).
//
// Snapshot is a lightweight, self-contained view of the on-disk snapshot
// directory. It exposes commonly needed derived properties.
type Snapshot struct {
	id       string
	path     string
	raftMeta *raft.SnapshotMeta
}

// Less reports whether this snapshot is older than the other snapshot.
// Ordering is defined by (Term, Index, ID):
func (s *Snapshot) Less(other *Snapshot) bool {
	if s.raftMeta.Term != other.raftMeta.Term {
		return s.raftMeta.Term < other.raftMeta.Term
	}
	if s.raftMeta.Index != other.raftMeta.Index {
		return s.raftMeta.Index < other.raftMeta.Index
	}
	return s.id < other.id
}

// LessThanMeta reports whether this snapshot is older than the given metadata.
// Ordering is defined by (Term, Index, ID).
func (s *Snapshot) LessThanMeta(meta *raft.SnapshotMeta) bool {
	if s.raftMeta.Term != meta.Term {
		return s.raftMeta.Term < meta.Term
	}
	if s.raftMeta.Index != meta.Index {
		return s.raftMeta.Index < meta.Index
	}
	return s.id < meta.ID
}

// Equal reports whether this snapshot is identical to the other snapshot.
func (s *Snapshot) Equal(other *Snapshot) bool {
	return s.raftMeta.Term == other.raftMeta.Term &&
		s.raftMeta.Index == other.raftMeta.Index &&
		s.id == other.id
}

// String returns a string representation of the snapshot.
func (s *Snapshot) String() string {
	return fmt.Sprintf("Snapshot{id=%s, term=%d, index=%d}", s.id, s.raftMeta.Term, s.raftMeta.Index)
}

// ID returns the snapshot ID.
func (s *Snapshot) ID() string {
	return s.id
}

// Path returns the snapshot directory path.
func (s *Snapshot) Path() string {
	return s.path
}

// Meta returns the Raft snapshot metadata.
func (s *Snapshot) Meta() *raft.SnapshotMeta {
	return s.raftMeta
}

// SnapshotSet represents an ordered collection of snapshots from a single Store
// directory.
//
// SnapshotSet is the primary abstraction used by Store for listing, selection,
// and policy decisions. It encapsulates the rules for ordering snapshots from
// oldest to newest and provides query methods that return snapshots, IDs, and
// Raft metadata without forcing callers to re-scan the filesystem or re-encode
// selection logic.
//
// SnapshotSet is intended to be cheap to pass and safe to treat as immutable.
// Query and filter operations should return new SnapshotSet values that share
// underlying Snapshot references.
type SnapshotSet struct {
	dir   string
	items []*Snapshot
}

// Len returns the number of snapshots in the set.
//
// SnapshotSet is ordered from oldest to newest.
func (ss SnapshotSet) Len() int {
	return len(ss.items)
}

// All returns all snapshots in the set, ordered from oldest to newest.
//
// The returned slice should be treated as read-only by callers.
func (ss SnapshotSet) All() []*Snapshot {
	return ss.items
}

// IDs returns the snapshot IDs in the set, ordered from oldest to newest.
//
// This is a convenience projection for callers that only need IDs.
func (ss SnapshotSet) IDs() []string {
	ids := make([]string, len(ss.items))
	for i, snapshot := range ss.items {
		ids[i] = snapshot.id
	}
	return ids
}

// RaftMetas returns the Raft snapshot metadata for all snapshots in the set,
// ordered from oldest to newest.
func (ss SnapshotSet) RaftMetas() []*raft.SnapshotMeta {
	metas := make([]*raft.SnapshotMeta, len(ss.items))
	for i, snapshot := range ss.items {
		metas[i] = snapshot.raftMeta
	}
	return metas
}

// Oldest returns the oldest snapshot in the set.
//
// The boolean result reports whether a snapshot was present.
func (ss SnapshotSet) Oldest() (*Snapshot, bool) {
	if len(ss.items) == 0 {
		return nil, false
	}
	return ss.items[0], true
}

// Newest returns the newest snapshot in the set.
//
// The boolean result reports whether a snapshot was present.
func (ss SnapshotSet) Newest() (*Snapshot, bool) {
	if len(ss.items) == 0 {
		return nil, false
	}
	return ss.items[len(ss.items)-1], true
}

// WithID returns the snapshot with the given ID if present.
//
// This is intended for selection and lookup without re-scanning the filesystem.
// The boolean result reports whether a matching snapshot was present.
func (ss SnapshotSet) WithID(id string) (*Snapshot, bool) {
	for _, snapshot := range ss.items {
		if snapshot.id == id {
			return snapshot, true
		}
	}
	return nil, false
}

// BeforeID returns a SnapshotSet containing snapshots strictly older than the
// snapshot with the given ID, preserving order.
//
// If the given ID is not present, BeforeID returns an empty set.
func (ss SnapshotSet) BeforeID(id string) SnapshotSet {
	idx := ss.indexOf(id)
	if idx < 0 {
		return SnapshotSet{dir: ss.dir, items: []*Snapshot{}}
	}
	// If idx == 0, this correctly returns an empty set.
	return SnapshotSet{dir: ss.dir, items: ss.items[:idx]}
}

// AfterID returns a SnapshotSet containing snapshots strictly newer than the
// snapshot with the given ID, preserving order.
//
// If the given ID is not present, AfterID returns an empty set.
func (ss SnapshotSet) AfterID(id string) SnapshotSet {
	idx := ss.indexOf(id)
	if idx < 0 {
		return SnapshotSet{dir: ss.dir, items: []*Snapshot{}}
	}
	// If idx is the last element, this correctly returns an empty set.
	return SnapshotSet{dir: ss.dir, items: ss.items[idx+1:]}
}

// Range returns a SnapshotSet containing snapshots in the half-open interval
// [fromID, toID), preserving order.
//
// If toID is the empty string, Range returns [fromID, end).
//
// If fromID is not present, Range returns an empty set.
// If toID is non-empty but not present, Range returns an empty set.
// If toID is present but appears at or before fromID, Range returns an empty set.
func (ss SnapshotSet) Range(fromID, toID string) SnapshotSet {
	fromIdx := ss.indexOf(fromID)
	if fromIdx < 0 {
		return SnapshotSet{dir: ss.dir, items: []*Snapshot{}}
	}

	if toID == "" {
		return SnapshotSet{dir: ss.dir, items: ss.items[fromIdx:]}
	}

	toIdx := ss.indexOf(toID)
	if toIdx < 0 {
		return SnapshotSet{dir: ss.dir, items: []*Snapshot{}}
	}
	if toIdx <= fromIdx {
		return SnapshotSet{dir: ss.dir, items: []*Snapshot{}}
	}

	return SnapshotSet{dir: ss.dir, items: ss.items[fromIdx:toIdx]}
}

// indexOf returns the index of the snapshot with the given ID.
// If no snapshot is found, it returns -1.
func (ss SnapshotSet) indexOf(id string) int {
	for i, snapshot := range ss.items {
		if snapshot.id == id {
			return i
		}
	}
	return -1
}

// SnapshotCatalog is responsible for discovering and materializing snapshots
// from a Store directory.
//
// SnapshotCatalog performs the filesystem scan, interprets the on-disk layout
// for snapshot directories, loads meta.json for each discovered snapshot, and
// produces a SnapshotSet ordered from oldest to newest.
//
// SnapshotCatalog does not mutate on-disk state. Any errors encountered while
// reading the snapshot store or loading metadata are returned to the caller.
type SnapshotCatalog struct{}

// Scan scans the snapshot store directory and returns a SnapshotSet.
//
// Scan identifies all snapshot directories under the given root, loads their
// metadata from meta.json, and orders the resulting snapshots from oldest to
// newest according to the store’s ordering rules.
//
// If Scan encounters an error while reading the snapshot store directory or
// loading metadata for a snapshot, it returns that error. Scan does not attempt
// to repair or modify on-disk state.
func (c *SnapshotCatalog) Scan(dir string) (SnapshotSet, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return SnapshotSet{}, fmt.Errorf("reading snapshot store directory: %w", err)
	}

	var snapshots []*Snapshot
	for _, entry := range entries {
		if !entry.IsDir() || isTmpName(entry.Name()) {
			continue
		}

		snapshotPath := filepath.Join(dir, entry.Name())
		snapshot, err := c.loadSnapshot(snapshotPath, entry.Name())
		if err != nil {
			return SnapshotSet{}, fmt.Errorf("loading snapshot %q: %w", entry.Name(), err)
		}
		snapshots = append(snapshots, snapshot)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Less(snapshots[j])
	})

	return SnapshotSet{
		dir:   dir,
		items: snapshots,
	}, nil
}

func (c *SnapshotCatalog) loadSnapshot(path string, id string) (*Snapshot, error) {
	meta, err := readMeta(metaPath(path))
	if err != nil {
		return nil, fmt.Errorf("reading meta.json: %w", err)
	}

	// Ensure the snapshot ID in metadata matches the directory-based ID.
	meta.ID = id
	return &Snapshot{
		id:       id,
		path:     path,
		raftMeta: meta,
	}, nil
}

func readMeta(path string) (*raft.SnapshotMeta, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	meta := &raft.SnapshotMeta{}
	dec := json.NewDecoder(fh)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func metaPath(dir string) string {
	return filepath.Join(dir, metaFileName)
}
