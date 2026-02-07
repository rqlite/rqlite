package snapshot

import (
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v9/internal/rsync"
	"github.com/rqlite/rqlite/v9/snapshot/proto"
)

const (
	dbfileName        = "data.db"
	walfileName       = "data.wal"
	metaFileName      = "meta.json"
	tmpSuffix         = ".tmp"
	fullNeededFile    = "FULL_NEEDED"
	reapingMarkerFile = "REAPING"
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
	SnapshotMetaTypeFull = iota
	SnapshotMetaTypeIncremental
)

// SnapshotMeta represents metadata about a snapshot.
type SnapshotMeta struct {
	*raft.SnapshotMeta
	Type SnapshotMetaType
}

// LockingSink is a wrapper around a Sink holds the MSRW lock
// while the Sink is in use.
type LockingSink struct {
	raft.SnapshotSink
	str *Store

	mu     sync.Mutex
	closed bool
	logger *log.Logger
}

// NewLockingSink returns a new LockingSink.
func NewLockingSink(sink raft.SnapshotSink, str *Store) *LockingSink {
	return &LockingSink{
		SnapshotSink: sink,
		str:          str,
		logger:       log.New(os.Stderr, "[snapshot-locking-sink] ", log.LstdFlags),
	}
}

// Close closes the sink, unlocking the Store for creation of a new sink.
func (s *LockingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.str.mrsw.EndRead()
	return s.SnapshotSink.Close()
}

// Cancel cancels the sink, unlocking the Store for creation of a new sink.
func (s *LockingSink) Cancel() error {
	defer func() {
		s.logger.Printf("sink %s canceled", s.ID())
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.str.mrsw.EndRead()
	return s.SnapshotSink.Cancel()
}

// LockingStreamer is a snapshot which holds the Snapshot Store MRSW read-lok
// while it is open.
type LockingStreamer struct {
	io.ReadCloser
	str *Store

	mu     sync.Mutex
	closed bool
}

// NewLockingStreamer returns a new LockingStreamer.
func NewLockingStreamer(rc io.ReadCloser, str *Store) *LockingStreamer {
	return &LockingStreamer{
		ReadCloser: rc,
		str:        str,
	}
}

// Close closes the Snapshot and releases the Snapshot Store lock.
func (l *LockingStreamer) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	defer l.str.mrsw.EndRead()
	return l.ReadCloser.Close()
}

// Store stores snapshots in the Raft system.
type Store struct {
	dir               string
	fullNeededPath    string
	reapingMarkerPath string
	logger            *log.Logger

	catalog *SnapshotCatalog

	// Multi-reader single-writer lock for the Store, which must be held
	// if snaphots are deleted i.e. repead. Simply creating or reading
	// a snapshot requires only a read lock.
	mrsw         rsync.MultiRSW
	reapDisabled bool

	LogReaping bool
}

// NewStore creates a new store.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	str := &Store{
		dir:               dir,
		fullNeededPath:    filepath.Join(dir, fullNeededFile),
		reapingMarkerPath: filepath.Join(dir, reapingMarkerFile),
		catalog:           &SnapshotCatalog{},
		mrsw:              *rsync.NewMultiRSW(),
		logger:            log.New(os.Stderr, "[snapshot-store] ", log.LstdFlags),
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
	configurationIndex uint64, trans raft.Transport) (retSink raft.SnapshotSink, retErr error) {
	if err := s.mrsw.BeginRead(); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			s.mrsw.EndRead()
		}
	}()

	sink := NewSink(s.dir, &raft.SnapshotMeta{
		Version:            version,
		ID:                 snapshotName(term, index),
		Index:              index,
		Term:               term,
		Configuration:      configuration,
		ConfigurationIndex: configurationIndex,
	}, s)
	if err := sink.Open(); err != nil {
		return nil, err
	}
	s.logger.Printf("created new snapshot sink: index=%d, term=%d", index, term)
	return NewLockingSink(sink, s), nil
}

// List returns the list of available snapshots in the Store,
// ordered from newest to oldest.
func (s *Store) List() ([]*raft.SnapshotMeta, error) {
	sset, err := s.catalog.Scan(s.dir)
	if err != nil {
		return nil, err
	}
	metas := sset.RaftMetas()
	// reverse the order to be from newest to oldest
	for i, j := 0, len(metas)-1; i < j; i, j = i+1, j-1 {
		metas[i], metas[j] = metas[j], metas[i]
	}
	return metas, nil
}

// Len returns the number of snapshots in the Store.
func (s *Store) Len() int {
	sset, err := s.catalog.Scan(s.dir)
	if err != nil {
		return 0
	}
	return sset.Len()
}

// LatestIndexTerm returns the index and term of the most recent
// snapshot in the Store.
func (s *Store) LatestIndexTerm() (uint64, uint64, error) {
	sset, err := s.catalog.Scan(s.dir)
	if err != nil {
		return 0, 0, err
	}
	newest, ok := sset.Newest()
	if !ok {
		return 0, 0, ErrSnapshotNotFound
	}
	return newest.raftMeta.Index, newest.raftMeta.Term, nil
}

// Dir returns the directory where the snapshots are stored.
func (s *Store) Dir() string {
	return s.dir
}

// Open opens the snapshot with the given ID for reading. Open returns an io.ReadCloser
// which wraps a SnapshotInstall object. This is because the snapshot will be used
// to either rebuild a node's state after restart, or to send the snapshot to another node,
// both of which require the DB file and any associated WAL files.
func (s *Store) Open(id string) (raftMeta *raft.SnapshotMeta, rc io.ReadCloser, retErr error) {
	if err := s.mrsw.BeginRead(); err != nil {
		return nil, nil, err
	}
	defer func() {
		if retErr != nil {
			s.mrsw.EndRead()
		}
	}()

	snapSet, err := s.getSnapshots()
	if err != nil {
		return nil, nil, err
	}

	dbfile, walFiles, err := snapSet.ResolveFiles(id)
	if err != nil {
		return nil, nil, err
	}

	streamer, err := proto.NewSnapshotStreamer(dbfile, walFiles...)
	if err != nil {
		return nil, nil, err
	}

	if err := streamer.Open(); err != nil {
		return nil, nil, err
	}

	sz, err := streamer.Len()
	if err != nil {
		return nil, nil, err
	}

	meta, err := readMeta(filepath.Join(s.dir, id))
	if err != nil {
		return nil, nil, err
	}
	meta.Size = sz

	return meta.SnapshotMeta, NewLockingStreamer(streamer, s), nil
}

// Reap reaps snapshots. Reaping is the process of deleting old snapshots that are no
// longer needed. Reaping is a destructive operation, and is non-reversible. It it
// is interrupted, it more be completed later before the snapshot store is usabl
// again.
//
// What does Reaping do? It starts by identifying the most recent full snapshot. It
// then deletes all snapshots older than that snapshot, since they are not needed.
//
// Next, if there are no snapshots newer than that snapshot, then the reaping process
// is complete as there is nothign else to do. However, if there are snapshots newer
// than that snapshot they must be increemental snapshots, and they must be based on
// that full snapshot. In that case, the reaping process replays those incremental
// snapshots on top of the full snapshot, to create a new full snapshot that is up to date.
//
// It does this as follows. It renames the full snapshot directory to a temporary name,
// using that is the same index and term as the most recent incremental snapshat. It also
// however uses a current timestamp, to ensure that the new full snapshot is newer than
// the most recent incremental snapshot. It then replays each incremental snapshot on top of
// replays (moves and checkpoints each WAL into the temp directory). Finally, it strip the
// temp extension from the new full snapshot directory, thereby installing it. Finally, it
// deletes all snapshots older than the new full snapshot, which should be all of th
// snapshots that were just replayed.
//
// Because this is a critcal operation which must run to completion even in interrupted
// it uses a plan-then-execute approach. It first plans and then serialized the plan
// to disk at the path REAP_PLAN. Then it executes the plan. If the process is interrupted\
// during execution, it can be restarted, and it will pick up the plan from disk and continue
// executing it.
//
// It returns the number of snapshots reaped, and the number of WAL files/ checkpointed as
// part of the consolidation.
func (s *Store) Reap() (int, int, error) {
	return 0, 0, nil
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
	return snaps.Len() == 0, nil
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
	return map[string]any{
		"dir":       s.dir,
		"snapshots": snapshots.IDs(),
	}, nil
}

// check checks the Store for any inconsistencies, and repairs
// any inconsistencies it finds. Inconsistencies can happen
// if the system crashes during snapshotting.
func (s *Store) check() error {
	return nil
}

// getSnapshots returns the set of snapshots in the Store.
func (s *Store) getSnapshots() (SnapshotSet, error) {
	return s.catalog.Scan(s.dir)
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
