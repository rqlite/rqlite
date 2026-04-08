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
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/internal/rsync"
	"github.com/rqlite/rqlite/v10/snapshot/plan"
)

const (
	dbfileName     = "data.db"
	metaFileName   = "meta.json"
	tmpSuffix      = ".tmp"
	walfileSuffix  = ".wal"
	fullNeededFile = "FULL_NEEDED"
	reapPlanFile   = "REAP_PLAN"

	defaultReapThreshold = 4
)

const (
	persistSize     = "persist_size"
	persistDuration = "persist_duration_ms"

	sinkFullTotal        = "sink_full_total"
	sinkIncrementalTotal = "sink_incremental_total"
	sinkErrors           = "sink_errors"
	sinkFullCRC32Dur     = "sink_full_crc32_duration_ms"

	autoReapDuration    = "auto_reap_duration_ms"
	reapExecuteDuration = "reap_execute_duration_ms"
	reapTotal           = "reap_total"
	reapErrors          = "reap_errors"
	reapSnapshots       = "reap_snapshots"
	reapWALs            = "reap_wals"
	reapPlanRecovered   = "reap_plan_recovered"

	upgradeOk   = "upgrade_ok"
	upgradeFail = "upgrade_fail"
)

var (
	// ErrSnapshotNotFound is returned when a snapshot cannot be found.
	ErrSnapshotNotFound = errors.New("snapshot not found")
)

// stats captures stats for the Store.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("snapshot")
	ResetStats()
}

func recordDuration(stat string, startT time.Time) {
	stats.Get(stat).(*expvar.Int).Set(time.Since(startT).Milliseconds())
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(persistSize, 0)
	stats.Add(persistDuration, 0)
	stats.Add(sinkFullTotal, 0)
	stats.Add(sinkIncrementalTotal, 0)
	stats.Add(sinkErrors, 0)
	stats.Add(sinkFullCRC32Dur, 0)
	stats.Add(autoReapDuration, 0)
	stats.Add(reapExecuteDuration, 0)
	stats.Add(reapTotal, 0)
	stats.Add(reapErrors, 0)
	stats.Add(reapSnapshots, 0)
	stats.Add(reapWALs, 0)
	stats.Add(reapPlanRecovered, 0)
	stats.Add(upgradeOk, 0)
	stats.Add(upgradeFail, 0)
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
	reapPlanPath   string
	logger         *log.Logger

	catalog *SnapshotCatalog

	// Multi-reader single-writer lock for the Store, which must be held
	// if snapshots are deleted i.e. reaped. Simply creating or reading
	// a snapshot requires only a read lock.
	mrsw          *rsync.MultiRSW
	reapDisabled  *rsync.AtomicBool
	reapThreshold int

	reapCh     chan struct{}
	reapDoneCh chan struct{}
	wg         sync.WaitGroup

	observers *observerSet

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
		reapPlanPath:   filepath.Join(dir, reapPlanFile),
		catalog:        &SnapshotCatalog{},
		mrsw:           rsync.NewMultiRSW(),
		reapDisabled:   &rsync.AtomicBool{},
		reapThreshold:  defaultReapThreshold,
		reapCh:         make(chan struct{}, 1),
		reapDoneCh:     make(chan struct{}),
		observers:      newObserverSet(),
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

	// Kick off the reaper goroutine.
	str.wg.Go(str.reapLoop)

	return str, nil
}

// Create creates a new snapshot sink for the given parameters.
func (s *Store) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (retSink raft.SnapshotSink, retErr error) {
	sink := NewSink(s.dir, &raft.SnapshotMeta{
		Version:            version,
		ID:                 snapshotName(term, index),
		Index:              index,
		Term:               term,
		Configuration:      configuration,
		ConfigurationIndex: configurationIndex,
	}, s, s.reapCh)
	if err := sink.Open(); err != nil {
		return nil, err
	}
	return sink, nil
}

// ListAll returns the list of all available snapshots in the Store,
// ordered from newest to oldest.
func (s *Store) ListAll() ([]*raft.SnapshotMeta, error) {
	if err := s.mrsw.BeginRead(); err != nil {
		return nil, err
	}
	defer s.mrsw.EndRead()

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

// List returns the most recent snapshot in the Store, if any exist.
// It satisfies the raft.SnapshotStore interface.
func (s *Store) List() ([]*raft.SnapshotMeta, error) {
	metas, err := s.ListAll()
	if err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return metas, nil
	}
	return metas[:1], nil
}

// Len returns the number of snapshots in the Store.
func (s *Store) Len() int {
	if err := s.mrsw.BeginRead(); err != nil {
		return 0
	}
	defer s.mrsw.EndRead()

	sset, err := s.catalog.Scan(s.dir)
	if err != nil {
		return 0
	}
	return sset.Len()
}

// LatestIndexTerm returns the index and term of the most recent
// snapshot in the Store.
func (s *Store) LatestIndexTerm() (uint64, uint64, error) {
	if err := s.mrsw.BeginRead(); err != nil {
		return 0, 0, err
	}
	defer s.mrsw.EndRead()

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
//
// A sink does not need to lock the Store because either the Snapshot directory it
// creates will be visible or not. Reaping will not see it until it is fully created,
// and Listing it will not return it until it is fully created too.
func (s *Store) Open(id string) (raftMeta *raft.SnapshotMeta, rc io.ReadCloser, retErr error) {
	if err := s.mrsw.BeginRead(); err != nil {
		return nil, nil, fmt.Errorf("acquiring read lock: %w", err)
	}
	defer func() {
		if retErr != nil {
			s.mrsw.EndRead()
		}
	}()

	snapSet, err := s.getSnapshots()
	if err != nil {
		return nil, nil, fmt.Errorf("scanning snapshots: %w", err)
	}
	if snapSet.Len() == 0 {
		return nil, nil, ErrSnapshotNotFound
	}

	dbFile, walFiles, err := snapSet.ResolveFiles(id)
	if err != nil {
		return nil, nil, fmt.Errorf("resolving files for snapshot %s: %w", id, err)
	}

	streamer, err := NewChecksummedSnapshotStreamer(dbFile, walFiles...)
	if err != nil {
		return nil, nil, fmt.Errorf("creating streamer for snapshot %s: %w", id, err)
	}

	if err := streamer.Open(); err != nil {
		return nil, nil, fmt.Errorf("opening streamer for snapshot %s: %w", id, err)
	}

	sz, err := streamer.Len()
	if err != nil {
		return nil, nil, fmt.Errorf("computing stream length for snapshot %s: %w", id, err)
	}

	meta, err := readRaftMeta(metaPath(filepath.Join(s.dir, id)))
	if err != nil {
		return nil, nil, fmt.Errorf("reading metadata for snapshot %s: %w", id, err)
	}
	meta.Size = sz

	return meta, NewLockingStreamer(streamer, s), nil
}

// RegisterObserver registers an observer to receive observations.
func (s *Store) RegisterObserver(o *Observer) {
	s.observers.register(o)
}

// DeregisterObserver removes a previously registered observer.
func (s *Store) DeregisterObserver(o *Observer) {
	s.observers.deregister(o)
}

// Close shuts down the reaper goroutine and waits for it to exit.
func (s *Store) Close() error {
	close(s.reapDoneCh)
	s.wg.Wait()
	return nil
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
	return s.reap()
}

// reap performs the actual reap. The caller must hold the write lock.
// On success, registered observers are notified.
func (s *Store) reap() (int, int, error) {
	startTime := time.Now()
	n, c, err := s.reapInternal()
	if err != nil {
		return n, c, err
	}
	stats.Add(reapTotal, 1)
	stats.Get(reapSnapshots).(*expvar.Int).Set(int64(n))
	stats.Get(reapWALs).(*expvar.Int).Set(int64(c))
	s.observers.notify(ReapObservation{
		SnapshotsReaped: n,
		WALsReaped:      c,
		Duration:        time.Since(startTime),
	})
	return n, c, nil
}

// reapInternal performs the actual reap logic. The caller must hold the write lock.
func (s *Store) reapInternal() (int, int, error) {
	// If a reap plan file exists, that means a previous reap must have encountered an error.
	// Let's make sure it is completed before we start a new reap.
	if fileExists(s.reapPlanPath) {
		s.logger.Printf("found interrupted reap plan at %s, resuming", s.reapPlanPath)
		stats.Add(reapPlanRecovered, 1)
		p, err := plan.ReadFromFile(s.reapPlanPath)
		if err != nil {
			return 0, 0, fmt.Errorf("reading reap plan: %w", err)
		}
		return s.executeReapPlan(p, s.reapPlanPath)
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

	// Collect all WAL files: from the full snapshot itself and from any
	// incremental snapshots that follow it.
	var walFiles []string
	for _, wf := range full.walFiles {
		walFiles = append(walFiles, wf.Path)
	}
	for _, snap := range newerSet.All() {
		for _, wf := range snap.walFiles {
			walFiles = append(walFiles, wf.Path)
		}
	}

	if newerSet.Len() == 0 && len(walFiles) == 0 {
		// No WALs to checkpoint and no incrementals — just remove older snapshots.
		for _, snap := range olderSet.All() {
			p.AddRemoveAll(snap.path)
			p.NReaped++
		}
	} else if len(walFiles) > 0 {
		// 1. Checkpoint all WAL files into the full snapshot's DB. We do it this way
		// because presumably the full snapshot DB is the largest file and it generally
		// makes sense to move the WAL files to it.
		dbPath := filepath.Join(full.path, dbfileName)
		p.AddCheckpoint(dbPath, walFiles)
		p.NCheckpointed = len(walFiles)

		// 2. Recompute CRC32 sidecar for the checkpointed DB file.
		p.AddCalcCRC32(dbPath, dbPath+crcSuffix)

		// 3. Remove all incremental snapshot dirs since we have just checkpointed
		// associated WAL files into the full database.
		for _, snap := range newerSet.All() {
			p.AddRemoveAll(snap.path)
			p.NReaped++
		}

		// 4. Remove all older-than-full dirs. They are not needed any longer.
		for _, snap := range olderSet.All() {
			p.AddRemoveAll(snap.path)
			p.NReaped++
		}

		// 5. Write new metadata into the full snapshot dir, overwriting the existing
		// metadata.
		var newest *Snapshot
		if newerSet.Len() > 0 {
			newest, _ = newerSet.Newest()
		} else {
			newest = full
		}
		newID := snapshotName(newest.raftMeta.Term, newest.raftMeta.Index)
		newMeta := copyRaftMeta(newest.raftMeta)
		newMeta.ID = newID
		metaJSON, err := json.Marshal(newMeta)
		if err != nil {
			return 0, 0, fmt.Errorf("marshaling consolidated meta: %w", err)
		}
		p.AddWriteMeta(full.path, metaJSON)

		// 6. Rename to new snapshot name. The end result of the Reaping process
		// will be a new full snapshot with a new ID. That ID is generated from
		// the newest snapshot's index and term, and the current timestamp.
		finalDir := filepath.Join(s.dir, newID)
		p.AddRename(full.path, finalDir)
	}

	// Persist the plan to disk for crash recovery.
	if err := plan.WriteToFile(p, s.reapPlanPath); err != nil {
		return 0, 0, fmt.Errorf("writing reap plan: %w", err)
	}

	return s.executeReapPlan(p, s.reapPlanPath)
}

// executeReapPlan executes a reap plan and cleans up.
func (s *Store) executeReapPlan(p *plan.Plan, planPath string) (int, int, error) {
	startT := time.Now()
	defer recordDuration(reapExecuteDuration, startT)

	executor := plan.NewExecutor()
	if err := p.Execute(executor); err != nil {
		return 0, 0, fmt.Errorf("executing reap plan: %w", err)
	}

	if err := syncDirMaybe(s.dir); err != nil {
		return 0, 0, fmt.Errorf("syncing store dir: %w", err)
	}

	// Clean up the plan file.
	os.Remove(planPath)
	return p.NReaped, p.NCheckpointed, nil
}

// signalReap sends a non-blocking signal to the reaper goroutine.
func (s *Store) signalReap() {
	select {
	case s.reapCh <- struct{}{}:
	default:
	}
}

// DueNext returns the type of snapshot due next.
func (s *Store) DueNext() (Type, error) {
	if fileExists(s.fullNeededPath) {
		return Full, nil
	}

	nSnaps := s.snapshotCount()
	if nSnaps == 0 {
		return Full, nil
	}
	return Incremental, nil
}

// SetDueNext sets the type of snapshot due next. Setting Full
// creates a flag file; setting Incremental removes it.
func (s *Store) SetDueNext(t Type) error {
	switch t {
	case Full:
		f, err := os.Create(s.fullNeededPath)
		if err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		return syncDirMaybe(s.dir)
	case Incremental:
		if !fileExists(s.fullNeededPath) {
			return nil
		}
		if err := os.Remove(s.fullNeededPath); err != nil {
			return err
		}
		return syncDirMaybe(s.dir)
	default:
		return fmt.Errorf("unknown snapshot type: %s", t)
	}
}

// Stats returns stats about the Snapshot Store. This function may return
// an error if the Store is in an inconsistent state. In that case the stats
// returned may be incomplete or invalid.
func (s *Store) Stats() (map[string]any, error) {
	if err := s.mrsw.BeginRead(); err != nil {
		return nil, err
	}
	defer s.mrsw.EndRead()

	dirSz, err := fsutil.DirSize(s.dir)
	if err != nil {
		// If we can't compute the directory size, we can still return other stats,
		// so we ignore the error and just report a size of 0.
		dirSz = 0
	}

	snapshots, err := s.getSnapshots()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"dir":       s.dir,
		"dir_size":  dirSz,
		"snapshots": snapshots.IDs(),
	}, nil
}

// snapshotCount returns the number of non-tmp snapshot subdirectories.
// This is a lightweight heuristic that does not require any lock.
func (s *Store) snapshotCount() int {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() && !isTmpName(e.Name()) {
			n++
		}
	}
	return n
}

// reapLoop is the reaper goroutine. It waits for signals from Sink.Close()
// and reaps snapshots when the count exceeds the threshold. It uses a blocking
// write lock acquisition so it will wait for active readers to finish rather
// than failing.
func (s *Store) reapLoop() {
	for {
		select {
		case <-s.reapCh:
		case <-s.reapDoneCh:
			return
		}

		if s.reapDisabled.Is() {
			continue
		}

		count := s.snapshotCount()
		if count < s.reapThreshold {
			continue
		}

		startT := time.Now()
		n, c, err := func() (int, int, error) {
			defer recordDuration(autoReapDuration, startT)
			s.mrsw.BeginWriteBlocking("reap")
			defer s.mrsw.EndWrite()
			return s.reap()
		}()
		if err != nil {
			s.logger.Printf("reap failed: %s", err)
			stats.Add(reapErrors, 1)
			continue
		}
		if s.LogReaping {
			s.logger.Printf("autoreap complete in %s: %d snapshots reaped, %d WALs checkpointed",
				time.Since(startT), n, c)
		}
	}
}

// check checks the Store for any inconsistencies, and repairs
// any inconsistencies it finds. Inconsistencies can happen
// if the system crashes during snapshotting or reaping.
func (s *Store) check() error {
	// Remove any incomplete plan file from an interrupted plan write.
	os.Remove(tmpName(s.reapPlanPath))

	reapPlanFound := fileExists(s.reapPlanPath)

	// If no reap was interrupted, verify the CRC32 of every data file
	// in every snapshot directory. If a reap was interrupted, the files
	// may be in an inconsistent state and the CRC32 validation may fail,
	// so we skip it in that case and just resume the reap to fix any
	// inconsistencies.
	if !reapPlanFound {
		if err := s.checkCRCs(); err != nil {
			return err
		}
	}

	// Resume an interrupted reap if such a plan file exists. Only then should
	// we remove any leftover temporary directories, since they may be needed
	// for the reap to complete. If a reap plan was found, skip CRC validation
	// since files may have been left in an inconsistent state by the crash.
	if reapPlanFound {
		s.logger.Printf("found interrupted reap plan at %s, resuming", s.reapPlanPath)
		p, err := plan.ReadFromFile(s.reapPlanPath)
		if err != nil {
			return fmt.Errorf("reading reap plan: %w", err)
		}
		if _, _, err := s.executeReapPlan(p, s.reapPlanPath); err != nil {
			return fmt.Errorf("executing interrupted reap plan: %w", err)
		}
	}

	// Anything remaining is truly temporary and can now be cleaned up.
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
	return nil
}

// checkCRCs verifies the CRC32 of every data file in every snapshot directory.
func (s *Store) checkCRCs() error {
	startT := time.Now()
	snapshots, err := s.catalog.Scan(s.dir)
	if err != nil {
		return fmt.Errorf("scanning snapshots for CRC check: %w", err)
	}
	if len(snapshots.items) == 0 {
		return nil
	}

	checker := NewCRCChecker()
	for _, snap := range snapshots.items {
		if snap.dbFile != nil {
			checker.Add(snap.dbFile)
		}
		for _, wf := range snap.walFiles {
			checker.Add(wf)
		}
	}

	if err := <-checker.Check(); err != nil {
		return err
	}

	s.logger.Printf("completed CRC32 check of %d snapshot directories in %s",
		len(snapshots.items), time.Since(startT))
	return nil
}

// getSnapshots returns the set of snapshots in the Store.
func (s *Store) getSnapshots() (SnapshotSet, error) {
	return s.catalog.Scan(s.dir)
}

func copyRaftMeta(m *raft.SnapshotMeta) *raft.SnapshotMeta {
	c := *m
	return &c
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
