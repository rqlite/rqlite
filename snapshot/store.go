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
	"github.com/rqlite/rqlite/v9/snapshot/plan"
)

const (
	dbfileName     = "data.db"
	walfileName    = "data.wal"
	metaFileName   = "meta.json"
	tmpSuffix      = ".tmp"
	fullNeededFile = "FULL_NEEDED"
	reapPlanFile   = "REAP_PLAN"

	defaultReapThreshold = 8
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
// After a successful close, the Store is checked to see if reaping is needed.
func (s *LockingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	err := s.SnapshotSink.Close()
	s.str.mrsw.EndRead()
	if err == nil {
		s.str.reapIfNeeded()
	}
	return err
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
	dir            string
	fullNeededPath string
	logger         *log.Logger

	catalog *SnapshotCatalog

	// Multi-reader single-writer lock for the Store, which must be held
	// if snaphots are deleted i.e. repead. Simply creating or reading
	// a snapshot requires only a read lock.
	mrsw          rsync.MultiRSW
	reapDisabled  bool
	reapThreshold int

	LogReaping bool
}

// NewStore creates a new store.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	str := &Store{
		dir:            dir,
		fullNeededPath: filepath.Join(dir, fullNeededFile),
		catalog:        &SnapshotCatalog{},
		mrsw:           *rsync.NewMultiRSW(),
		reapThreshold:  defaultReapThreshold,
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

// SetReapThreshold sets the minimum number of snapshots that must
// exist before auto-reap is triggered after a successful persist.
func (s *Store) SetReapThreshold(n int) {
	s.reapThreshold = n
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

	streamer, err := NewSnapshotStreamer(dbfile, walFiles...)
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
// longer needed. Reaping is a destructive operation, and is non-reversible. If it
// is interrupted, it must be completed later before the snapshot store is usable
// again.
//
// What does Reaping do? It starts by identifying the most recent full snapshot. It
// then deletes all snapshots older than that snapshot, since they are not needed.
//
// Next, if there are no snapshots newer than that snapshot, then the reaping process
// is complete as there is nothing else to do. However, if there are snapshots newer
// than that snapshot they must be incremental snapshots, and they must be based on
// that full snapshot. In that case, the reaping process consolidates those incremental
// snapshots into the full snapshot, creating a single up-to-date full snapshot.
//
// It does this by checkpointing each incremental WAL file into the full snapshot's
// database file, removing the incremental snapshot directories and any older snapshot
// directories, writing new metadata reflecting the newest incremental's index and term,
// and finally renaming the full snapshot directory to a new name with a current timestamp.
//
// Because this is a critical operation which must run to completion even if interrupted,
// it uses a plan-then-execute approach. The entire sequence of operations is captured in
// a Plan, which is serialized to disk at the path REAP_PLAN. The plan is then executed.
// If the process is interrupted during execution, the plan can be re-read and re-executed
// on restart, since all operations are idempotent.
//
// It returns the number of snapshots reaped, and the number of WAL files checkpointed as
// part of the consolidation.
func (s *Store) Reap() (int, int, error) {
	if err := s.mrsw.BeginWrite("reap"); err != nil {
		return 0, 0, err
	}
	defer s.mrsw.EndWrite()

	planPath := filepath.Join(s.dir, reapPlanFile)

	// Check for existing reap plan (crash recovery).
	if fileExists(planPath) {
		s.logger.Printf("found existing reap plan at %s, resuming", planPath)
		p, err := plan.ReadFromFile(planPath)
		if err != nil {
			return 0, 0, fmt.Errorf("reading reap plan: %w", err)
		}
		return s.executeReapPlan(p, planPath)
	}

	// Scan store.
	snapSet, err := s.getSnapshots()
	if err != nil {
		return 0, 0, err
	}

	// Nothing to do for empty stores.
	if snapSet.Len() == 0 {
		return 0, 0, nil
	}

	// Find the newest full snapshot and everything newer than it.
	fullSet, newerSet := snapSet.PartitionAtFull()
	if fullSet.Len() == 0 {
		return 0, 0, fmt.Errorf("no full snapshot found")
	}

	full, _ := fullSet.Newest()

	// Single full snapshot with nothing newer — nothing to do.
	if snapSet.Len() == 1 {
		return 0, 0, nil
	}

	olderSet := snapSet.BeforeID(full.id)

	p := plan.New()

	if newerSet.Len() == 0 {
		// No incrementals after the newest full.
		// Just remove all snapshots older than the full.
		for _, snap := range olderSet.All() {
			p.AddRemoveAll(snap.path)
			p.NReaped++
		}
	} else {
		// There are incrementals after the newest full.
		// Consolidate by checkpointing the associated WALs into the full snapshot.
		newestInc, _ := newerSet.Newest()
		newID := snapshotName(newestInc.raftMeta.Term, newestInc.raftMeta.Index)
		finalDir := filepath.Join(s.dir, newID)

		newMeta := copyRaftMeta(newestInc.raftMeta)
		newMeta.ID = newID
		metaJSON, err := json.Marshal(newMeta)
		if err != nil {
			return 0, 0, fmt.Errorf("marshaling consolidated meta: %w", err)
		}

		// 1. Checkpoint all incremental WAL files into the full's DB.
		//    WAL files reside in different directories; the executor
		//    handles cross-directory moves during checkpointing.
		var walFiles []string
		for _, snap := range newerSet.All() {
			walFiles = append(walFiles, filepath.Join(snap.path, walfileName))
		}
		p.AddCheckpoint(filepath.Join(full.path, dbfileName), walFiles)
		p.NCheckpointed = len(walFiles)

		// 2. Remove all incremental snapshot dirs.
		for _, snap := range newerSet.All() {
			p.AddRemoveAll(snap.path)
		}
		p.NReaped = newerSet.Len()

		// 3. Remove all older-than-full dirs.
		for _, snap := range olderSet.All() {
			p.AddRemoveAll(snap.path)
			p.NReaped++
		}

		// 4. Write new metadata into the full snapshot dir.
		p.AddWriteMeta(full.path, metaJSON)

		// 5. Rename to final name with current timestamp.
		p.AddRename(full.path, finalDir)
	}

	// Persist the plan to disk for crash recovery.
	if err := plan.WriteToFile(p, planPath); err != nil {
		return 0, 0, fmt.Errorf("writing reap plan: %w", err)
	}

	return s.executeReapPlan(p, planPath)
}

// executeReapPlan executes a reap plan and cleans up.
func (s *Store) executeReapPlan(p *plan.Plan, planPath string) (int, int, error) {
	executor := plan.NewExecutor()
	if err := p.Execute(executor); err != nil {
		return 0, 0, fmt.Errorf("executing reap plan: %w", err)
	}

	if err := syncDirMaybe(s.dir); err != nil {
		return 0, 0, fmt.Errorf("syncing store dir: %w", err)
	}

	// Clean up the plan file.
	os.Remove(planPath)

	s.logger.Printf("reap complete: %d snapshots reaped, %d WALs checkpointed",
		p.NReaped, p.NCheckpointed)
	return p.NReaped, p.NCheckpointed, nil
}

// reapIfNeeded checks the snapshot count and triggers a reap if the
// threshold has been reached. It is called after a snapshot is
// successfully persisted.
func (s *Store) reapIfNeeded() {
	if s.reapDisabled {
		return
	}
	if s.Len() < s.reapThreshold {
		return
	}

	n, c, err := s.Reap()
	if err != nil {
		var mrsw *rsync.ErrMRSWConflict
		if errors.As(err, &mrsw) {
			s.logger.Printf("reap skipped, store is busy: %s", err)
			return
		}
		s.logger.Fatalf("reap failed: %s", err)
	}
	if s.LogReaping {
		s.logger.Printf("auto-reap complete: %d snapshots reaped, %d WALs checkpointed", n, c)
	}
}

func copyRaftMeta(m *raft.SnapshotMeta) *raft.SnapshotMeta {
	c := *m
	return &c
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
// if the system crashes during snapshotting or reaping.
func (s *Store) check() error {
	// Remove any leftover temporary directories from interrupted
	// snapshot creation.
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("reading store directory: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() && isTmpName(e.Name()) {
			tmpPath := filepath.Join(s.dir, e.Name())
			s.logger.Printf("removing leftover temporary directory %s", tmpPath)
			if err := os.RemoveAll(tmpPath); err != nil {
				return fmt.Errorf("removing temporary directory %s: %w", tmpPath, err)
			}
		}
	}

	// Resume an interrupted reap if a plan file exists.
	planPath := filepath.Join(s.dir, reapPlanFile)
	if fileExists(planPath) {
		s.logger.Printf("found interrupted reap plan at %s, resuming", planPath)
		p, err := plan.ReadFromFile(planPath)
		if err != nil {
			return fmt.Errorf("reading reap plan: %w", err)
		}
		if _, _, err := s.executeReapPlan(p, planPath); err != nil {
			return fmt.Errorf("executing interrupted reap plan: %w", err)
		}
	}

	return nil
}

// LatestIndexTerm returns the index and term of the most recent snapshot
// in the given directory. If no snapshots are found, it returns 0, 0, nil.
func LatestIndexTerm(dir string) (uint64, uint64, error) {
	cat := &SnapshotCatalog{}
	sset, err := cat.Scan(dir)
	if err != nil {
		return 0, 0, err
	}
	newest, ok := sset.Newest()
	if !ok {
		return 0, 0, nil
	}
	return newest.raftMeta.Index, newest.raftMeta.Term, nil
}

// getSnapshots returns the set of snapshots in the Store.
func (s *Store) getSnapshots() (SnapshotSet, error) {
	return s.catalog.Scan(s.dir)
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
