package snapshot2

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/hashicorp/raft"
)

// Snapshot represents a single snapshot stored on disk.
// A Snapshot corresponds to exactly one directory under the Store root. The
// directory name is the snapshot ID (typically derived from term, index, and a
// timestamp).
//
// Snapshot is a lightweight, self-contained view of the on-disk snapshot
// directory. It exposes commonly needed derived properties (paths, declared
// kind, and Raft metadata), and it centralizes validation of the directory’s
// layout.
//
// The snapshot kind is declared by the presence of exactly one data file in the
// directory:
//
//   - data.db  declares a full snapshot (SQLite database file)
//   - data.wal declares an incremental snapshot (SQLite WAL file)
//
// Implementations may optionally validate that the file content matches the
// declared kind (e.g., via SQLite header inspection). Any disagreement between
// declared kind and observed content should be treated as store corruption and
// surfaced as a structured error suitable for repair or quarantine logic.
type Snapshot struct {
	id       string
	path     string
	typ      SnapshotType
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

// Equal reports whether this snapshot is identical to the other snapshot.
func (s *Snapshot) Equal(other *Snapshot) bool {
	return s.raftMeta.Term == other.raftMeta.Term &&
		s.raftMeta.Index == other.raftMeta.Index &&
		s.id == other.id
}

// String returns a string representation of the snapshot.
func (s *Snapshot) String() string {
	return fmt.Sprintf("Snapshot{id=%s, type=%v, term=%d, index=%d}", s.id, s.typ, s.raftMeta.Term, s.raftMeta.Index)
}

// SnapshotType describes the declared kind of a Snapshot.
//
// SnapshotType is derived from on-disk layout rather than persisted in meta.json.
// It is primarily a classification for selection, ordering, and maintenance
// operations (e.g., determining the newest full snapshot and its incremental
// successors).
type SnapshotType int

const (
	// SnapshotTypeFull indicates a snapshot directory containing data.db.
	SnapshotTypeFull SnapshotType = iota

	// SnapshotTypeIncremental indicates a snapshot directory containing data.wal.
	SnapshotTypeIncremental
)

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

// NewestFull returns the newest full snapshot in the set.
func (ss SnapshotSet) NewestFull() (*Snapshot, bool) {
	for i := len(ss.items) - 1; i >= 0; i-- {
		if ss.items[i].typ == SnapshotTypeFull {
			return ss.items[i], true
		}
	}
	return nil, false
}

// NewestIncremental returns the newest incremental snapshot in the set.
func (ss SnapshotSet) NewestIncremental() (*Snapshot, bool) {
	for i := len(ss.items) - 1; i >= 0; i-- {
		if ss.items[i].typ == SnapshotTypeIncremental {
			return ss.items[i], true
		}
	}
	return nil, false
}

// Fulls returns a SnapshotSet containing only full snapshots, preserving order.
func (ss SnapshotSet) Fulls() SnapshotSet {
	var fulls []*Snapshot
	for _, snapshot := range ss.items {
		if snapshot.typ == SnapshotTypeFull {
			fulls = append(fulls, snapshot)
		}
	}
	return SnapshotSet{
		dir:   ss.dir,
		items: fulls,
	}
}

// Incrementals returns a SnapshotSet containing only incremental snapshots,
// preserving order.
func (ss SnapshotSet) Incrementals() SnapshotSet {
	var incrementals []*Snapshot
	for _, snapshot := range ss.items {
		if snapshot.typ == SnapshotTypeIncremental {
			incrementals = append(incrementals, snapshot)
		}
	}
	return SnapshotSet{
		dir:   ss.dir,
		items: incrementals,
	}
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

// PartitionAtFull returns two SnapshotSets: the newest full snapshot and all
// snapshots newer than it.
//
// If a full snapshot exists, the first return value contains exactly that newest
// full snapshot, and the second contains all snapshots strictly newer than it.
//
// If no full snapshot exists, both return values are empty.
func (ss SnapshotSet) PartitionAtFull() (full SnapshotSet, newer SnapshotSet) {
	fullIdx := -1
	for i := len(ss.items) - 1; i >= 0; i-- {
		if ss.items[i].typ == SnapshotTypeFull {
			fullIdx = i
			break
		}
	}
	if fullIdx < 0 {
		empty := SnapshotSet{dir: ss.dir, items: []*Snapshot{}}
		return empty, empty
	}

	full = SnapshotSet{dir: ss.dir, items: ss.items[fullIdx : fullIdx+1]}
	newer = SnapshotSet{dir: ss.dir, items: ss.items[fullIdx+1:]}
	return full, newer
}

// ValidateIncrementalChain checks that all snapshots newer than the newest full
// snapshot are incremental snapshots.
//
// If the set contains no full snapshot, ValidateIncrementalChain returns an
// error indicating the chain cannot be validated.
func (ss SnapshotSet) ValidateIncrementalChain() error {
	fullIdx := -1
	var fullID string
	for i := len(ss.items) - 1; i >= 0; i-- {
		if ss.items[i].typ == SnapshotTypeFull {
			fullIdx = i
			fullID = ss.items[i].id
			break
		}
	}
	if fullIdx < 0 {
		return fmt.Errorf("no full snapshot present; cannot validate incremental chain")
	}

	for i := fullIdx + 1; i < len(ss.items); i++ {
		snap := ss.items[i]
		if snap.typ != SnapshotTypeIncremental {
			return fmt.Errorf("snapshot %s is not incremental after newest full snapshot %s", snap.id, fullID)
		}
	}
	return nil
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
// SnapshotCatalog performs the filesystem scan, interprets the on-disk layout,
// loads meta.json, determines each snapshot’s declared kind, and produces a
// SnapshotSet ordered from oldest to newest. SnapshotCatalog is the sole place
// where “what constitutes a snapshot directory” and “how to classify it” are
// defined.
//
// SnapshotCatalog does not mutate on-disk state. Inconsistent or invalid snapshot
// directories should be reported via structured errors so that Store.check() can
// decide whether to repair, quarantine, or remove them.
type SnapshotCatalog struct{}

// Scan scans the snapshot store directory and returns a SnapshotSet.
//
// Scan identifies all snapshot directories under the given root, interprets
// their layout, loads metadata, determines declared snapshot kind, and orders
// the resulting snapshots from oldest to newest according to the store’s
// ordering rules.
//
// If Scan encounters an invalid snapshot directory (e.g., missing data file,
// multiple data files, unreadable metadata, or a mismatch between declared kind
// and observed file format), it returns an error describing the inconsistency.
// Scan does not attempt to repair or modify on-disk state.
func (c *SnapshotCatalog) Scan(dir string) (SnapshotSet, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return SnapshotSet{}, fmt.Errorf("reading snapshot store directory: %w", err)
	}

	var snapshots []*Snapshot
	for _, entry := range entries {
		if !entry.IsDir() {
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
	meta, err := readRaftMeta(metaPath(path))
	if err != nil {
		return nil, fmt.Errorf("reading meta.json: %w", err)
	}

	snapshot := &Snapshot{
		id:       id,
		path:     path,
		raftMeta: meta,
	}

	dataDBPath := filepath.Join(path, "data.db")
	dataWALPath := filepath.Join(path, "data.wal")
	if fileExists(dataDBPath) {
		snapshot.typ = SnapshotTypeFull
	} else if fileExists(dataWALPath) {
		snapshot.typ = SnapshotTypeIncremental
	} else {
		return nil, fmt.Errorf("missing data file in snapshot directory %q", path)
	}
	return snapshot, nil
}

func readRaftMeta(path string) (*raft.SnapshotMeta, error) {
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
