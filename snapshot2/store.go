package snapshot2

import (
	"encoding/json"
	"errors"
	"expvar"
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
	metaFileName   = "meta.json"
	tmpSuffix      = ".tmp"
	fullNeededFile = "FULL_NEEDED"
)

const (
	persistSize            = "latest_persist_size"
	persistDuration        = "latest_persist_duration"
	upgradeOk              = "upgrade_ok"
	upgradeFail            = "upgrade_fail"
	snapshotsReaped        = "snapshots_reaped"
	snapshotsReapedFail    = "snapshots_reaped_failed"
	snapshotCreateMRSWFail = "snapshot_create_mrsw_fail"
	snapshotOpenMRSWFail   = "snapshot_open_mrsw_fail"
)

var (
	// ErrSnapshotNotFound is returned when a snapshot cannot be found.
	ErrSnapshotNotFound = errors.New("snapshot not found")

	// ErrDataFileNotFound is returned when a snapshot data file cannot be found.
	ErrDataFileNotFound = errors.New("snapshot data file not found")

	// ErrTooManyDataFiles is returned when more than one snapshot data file is found.
	ErrTooManyDataFiles = errors.New("too many snapshot data files found")
)

// stats captures stats for the Store.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("snapshot")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(persistSize, 0)
	stats.Add(persistDuration, 0)
	stats.Add(upgradeOk, 0)
	stats.Add(upgradeFail, 0)
	stats.Add(snapshotsReaped, 0)
	stats.Add(snapshotsReapedFail, 0)
	stats.Add(snapshotCreateMRSWFail, 0)
	stats.Add(snapshotOpenMRSWFail, 0)
}

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
	Type SnapshotMetaType
}

// Store stores snapshots in the Raft system.
type Store struct {
	dir            string
	fullNeededPath string
	logger         *log.Logger

	reapDisabled bool
	LogReaping   bool
}

// NewStore creates a new store.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	str := &Store{
		dir:            dir,
		fullNeededPath: filepath.Join(dir, fullNeededFile),
		logger:         log.New(os.Stderr, "[snapshot-store] ", log.LstdFlags),
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

// Len returns the number of snapshots in the Store.
func (s *Store) Len() int {
	snapshots, err := s.getSnapshots()
	if err != nil {
		return 0
	}
	return len(snapshots)
}

// LatestIndexTerm returns the index and term of the most recent
// snapshot in the Store.
func (s *Store) LatestIndexTerm() (uint64, uint64, error) {
	snapshots, err := s.getSnapshots()
	if err != nil {
		return 0, 0, err
	}
	if len(snapshots) == 0 {
		return 0, 0, nil
	}
	latest := snapshots[len(snapshots)-1]
	return latest.Index, latest.Term, nil
}

// Dir returns the directory where the snapshots are stored.
func (s *Store) Dir() string {
	return s.dir
}

// Open opens the snapshot with the given ID for reading.
func (s *Store) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	if !dirExists(filepath.Join(s.dir, id)) {
		return nil, nil, ErrSnapshotNotFound
	}

	meta, err := readMeta(filepath.Join(s.dir, id))
	if err != nil {
		return nil, nil, err
	}
	fd, err := os.Open(filepath.Join(s.dir, id+".db"))
	if err != nil {
		return nil, nil, err
	}
	return meta.SnapshotMeta, fd, nil
}

// Reap reaps all snapshots, except the most recent one. Returns the number of
// snapshots reaped.
func (s *Store) Reap() (retN int, retErr error) {
	defer func() {
		if retErr != nil {
			stats.Add(snapshotsReapedFail, 1)
		} else {
			stats.Add(snapshotsReaped, int64(retN))
		}
	}()
	if s.reapDisabled {
		return 0, nil
	}

	snapshots, err := s.getSnapshots()
	if err != nil {
		return 0, err
	}
	if len(snapshots) <= 1 {
		return 0, nil
	}
	// Remove all snapshots, and all associated data, except the newest one.
	n := 0
	for _, snap := range snapshots[:len(snapshots)-1] {
		if err := removeAllPrefix(s.dir, snap.ID); err != nil {
			return n, err
		}
		if s.LogReaping {
			s.logger.Printf("reaped snapshot %s", snap.ID)
		}
		n++
	}
	return n, nil
}

// FullNeeded returns true if a full snapshot is needed.
func (s *Store) FullNeeded() (bool, error) {
	if fileExists(s.fullNeededPath) {
		return true, nil
	}
	snaps, err := s.getSnapshots()
	if err != nil {
		return false, err
	}
	return len(snaps) == 0, nil
}

// SetFullNeeded sets the flag that indicates a full snapshot is needed.
// This flag will be cleared when a snapshot is successfully persisted.
func (s *Store) SetFullNeeded() error {
	f, err := os.Create(s.fullNeededPath)
	if err != nil {
		return err
	}
	return f.Close()
}

// Stats returns stats about the Snapshot Store. This function may return
// an error if the Store is in an inconsistent state. In that case the stats
// returned may be incomplete or invalid.
func (s *Store) Stats() (map[string]any, error) {
	snapshots, err := s.getSnapshots()
	if err != nil {
		return nil, err
	}
	snapsAsIDs := make([]string, len(snapshots))
	for i, snap := range snapshots {
		snapsAsIDs[i] = snap.ID
	}
	return map[string]any{
		"dir":       s.dir,
		"snapshots": snapsAsIDs,
	}, nil
}

// check checks the Store for any inconsistencies, and repairs
// any inconsistencies it finds. Inconsistencies can happen
// if the system crashes during snapshotting.
func (s *Store) check() error {
	return nil
}

func (s *Store) getSnapshots() ([]*SnapshotMeta, error) {
	return getSnapshots(s.dir)
}

// getSnapshots returns the list of snapshots in the given directory,
// sorted from oldest to new.
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

		path, err := getSnapshotDataFile(filepath.Join(dir, snap.Name()))
		if err != nil {
			return nil, err
		}
		if db.IsValidSQLiteFile(path) {
			meta.Type = SnapshotMetaTypeFull
		} else if db.IsValidSQLiteWALFile(path) {
			meta.Type = SnapshotMetaTypeIncremental
		} else {
			return nil, fmt.Errorf("snapshot %s does not contain a valid SQLite or WAL file at %s",
				snap.Name(), path)
		}
		snapMeta = append(snapMeta, meta)
	}

	sort.Sort(snapMetaSlice(snapMeta))
	return snapMeta, nil
}

// getSnapshotDataFile lists all files in the given snapshot directory
// that are of the form data.*. There should only be one such file, which
// is returned. It will be either data.db or data.wal.
func getSnapshotDataFile(dir string) (string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "data.*"))
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", ErrDataFileNotFound
	}
	if len(files) > 1 {
		return "", ErrTooManyDataFiles
	}
	return files[0], nil
}

type snapMetaSlice []*SnapshotMeta

// Len implements the sort interface for snapMetaSlice.
func (s snapMetaSlice) Len() int {
	return len(s)
}

// Less implements the sort interface for snapMetaSlice.
func (s snapMetaSlice) Less(i, j int) bool {
	si := s[i]
	sj := s[j]
	if si.Term != sj.Term {
		return si.Term < sj.Term
	}
	if si.Index != sj.Index {
		return si.Index < sj.Index
	}
	return si.ID < sj.ID
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
