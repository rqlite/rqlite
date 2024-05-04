package snapshot

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/db"
	"github.com/rqlite/rqlite/v8/rsync"
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

const (
	metaFileName   = "meta.json"
	tmpSuffix      = ".tmp"
	fullNeededFile = "FULL_NEEDED"
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

// LockingSink is a wrapper around a SnapshotSink holds the CAS lock
// while the Sink is in use.
type LockingSink struct {
	raft.SnapshotSink
	str *Store

	mu     sync.Mutex
	closed bool
}

// NewLockingSink returns a new LockingSink.
func NewLockingSink(sink raft.SnapshotSink, str *Store) *LockingSink {
	return &LockingSink{
		SnapshotSink: sink,
		str:          str,
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
	defer s.str.mrsw.EndWrite()
	return s.SnapshotSink.Close()
}

// Cancel cancels the sink, unlocking the Store for creation of a new sink.
func (s *LockingSink) Cancel() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.str.mrsw.EndWrite()
	return s.SnapshotSink.Cancel()
}

// LockingSnapshot is a snapshot which holds the Snapshot Store CAS while open.
type LockingSnapshot struct {
	*os.File
	str *Store

	mu     sync.Mutex
	closed bool
}

// NewLockingSink returns a new LockingSink.
func NewLockingSnapshot(fd *os.File, str *Store) *LockingSnapshot {
	return &LockingSnapshot{
		File: fd,
		str:  str,
	}
}

// Close closes the Snapshot and releases the Snapshot Store lock.
func (l *LockingSnapshot) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	defer l.str.mrsw.EndRead()
	return l.File.Close()
}

// Store stores Snapshots.
type Store struct {
	ctx            context.Context
	dir            string
	fullNeededPath string
	logger         *log.Logger

	mrsw *rsync.MultiRSW

	LogReaping   bool
	reapDisabled bool // For testing purposes
}

// NewStore returns a new Snapshot Store.
func NewStore(ctx context.Context, dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	str := &Store{
		ctx:            ctx,
		dir:            dir,
		fullNeededPath: filepath.Join(dir, fullNeededFile),
		logger:         log.New(os.Stderr, "[snapshot-store] ", log.LstdFlags),
		mrsw:           rsync.NewMultiRSW(ctx),
	}
	str.logger.Printf("store initialized using %s", dir)

	if err := str.check(); err != nil {
		return nil, fmt.Errorf("check failed: %s", err)
	}
	return str, nil
}

// Create creates a new Sink object, ready for writing a snapshot. Sinks make certain assumptions about
// the state of the store, and if those assumptions were changed by another Sink writing to the store
// it could cause failures. Therefore we only allow 1 Sink to be in existence at a time. This shouldn't
// be a problem, since snapshots are taken infrequently in one at a time.
func (s *Store) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (retSink raft.SnapshotSink, retErr error) {
	if err := s.mrsw.BeginWrite(); err != nil {
		stats.Add(snapshotCreateMRSWFail, 1)
		return nil, err
	}
	defer func() {
		if retErr != nil {
			s.mrsw.EndWrite()
		}
	}()

	meta := &raft.SnapshotMeta{
		ID:                 snapshotName(term, index),
		Index:              index,
		Term:               term,
		Configuration:      configuration,
		ConfigurationIndex: configurationIndex,
		Version:            version,
	}
	sink := NewSink(s, meta)
	if err := sink.Open(); err != nil {
		return nil, err
	}
	return NewLockingSink(sink, s), nil
}

// List returns a list of all the snapshots in the Store. In practice, this will at most be
// a list of 1, and that will be the newest snapshot available.
func (s *Store) List() ([]*raft.SnapshotMeta, error) {
	snapshots, err := s.getSnapshots()
	if err != nil {
		return nil, err
	}

	var snapMeta []*raft.SnapshotMeta
	if len(snapshots) > 0 {
		snapshotDir := filepath.Join(s.dir, snapshots[len(snapshots)-1].ID)
		meta, err := readMeta(snapshotDir)
		if err != nil {
			return nil, err
		}
		snapMeta = append(snapMeta, meta) // Insert it.
	}
	return snapMeta, nil
}

// Open opens the snapshot with the given ID. Close() must be called on the snapshot
// when finished with it.
func (s *Store) Open(id string) (_ *raft.SnapshotMeta, _ io.ReadCloser, retErr error) {
	if err := s.mrsw.BeginRead(); err != nil {
		stats.Add(snapshotOpenMRSWFail, 1)
		return nil, nil, err
	}
	defer func() {
		if retErr != nil {
			s.mrsw.EndRead()
		}
	}()
	meta, err := readMeta(filepath.Join(s.dir, id))
	if err != nil {
		return nil, nil, err
	}
	fd, err := os.Open(filepath.Join(s.dir, id+".db"))
	if err != nil {
		return nil, nil, err
	}
	return meta, NewLockingSnapshot(fd, s), nil
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
func (s *Store) Stats() (map[string]interface{}, error) {
	snapshots, err := s.getSnapshots()
	if err != nil {
		return nil, err
	}
	snapsAsIDs := make([]string, len(snapshots))
	for i, snap := range snapshots {
		snapsAsIDs[i] = snap.ID
	}
	dbPath, err := s.getDBPath()
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"dir":       s.dir,
		"snapshots": snapsAsIDs,
		"db_path":   dbPath,
	}, nil
}

// Reap reaps all snapshots, except the most recent one. Returns the number of
// snapshots reaped. This function does not take the Store CAS lock, and so
// it is up to the caller to ensure no other operations are happening on the
// Store.
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

// Dir returns the directory where the snapshots are stored.
func (s *Store) Dir() string {
	return s.dir
}

// check checks the Store for any inconsistencies, and repairs
// any inconsistencies it finds. Inconsistencies can happen
// if the system crashes during snapshotting.
func (s *Store) check() (retError error) {
	defer func() {
		syncDirMaybe(s.dir)
		s.logger.Printf("check complete")
	}()
	s.logger.Printf("checking consistency of snapshot store at %s", s.dir)

	if err := RemoveAllTmpSnapshotData(s.dir); err != nil {
		return err
	}

	snapshots, err := s.getSnapshots()
	if err != nil {
		return err
	}
	if len(snapshots) == 0 {
		// Nothing to do!
		return nil
	} else if len(snapshots) == 1 {
		// We only have one snapshot. Confirm we have a valid SQLite file
		// for that snapshot.
		snap := snapshots[0]
		snapDB := filepath.Join(s.dir, snap.ID+".db")
		if !db.IsValidSQLiteFile(snapDB) {
			return fmt.Errorf("sole snapshot data is not a valid SQLite file: %s", snap.ID)
		}
	} else {
		// Do we have a valid SQLite file for the most recent snapshot?
		snap := snapshots[len(snapshots)-1]
		snapDB := filepath.Join(s.dir, snap.ID+".db")
		snapDir := filepath.Join(s.dir, snap.ID)
		if db.IsValidSQLiteFile(snapDB) {
			// Open and close it, which will replay any WAL file into it.
			return openCloseDB(snapDB)
		}
		// We better have a SQLite file for the previous snapshot.
		snapPrev := snapshots[len(snapshots)-2]
		snapPrevDB := filepath.Join(s.dir, snapPrev.ID+".db")
		if !db.IsValidSQLiteFile(snapPrevDB) {
			return fmt.Errorf("previous snapshot data is not a SQLite file: %s", snapPrev.ID)
		}
		// Rename the previous SQLite file to the current snapshot, and then replay any WAL file into it.
		if err := os.Rename(snapPrevDB, snapDB); err != nil {
			return err
		}
		if err := openCloseDB(snapDB); err != nil {
			return err
		}

		// Ensure the size is set in the Snapshot's meta.
		fi, err := os.Stat(snapDB)
		if err != nil {
			return err
		}
		if err := updateMetaSize(snapDir, fi.Size()); err != nil {
			return err
		}
	}
	return nil
}

// getSnapshots returns a list of all snapshots in the store, sorted
// from oldest to newest.
func (s *Store) getSnapshots() ([]*raft.SnapshotMeta, error) {
	return getSnapshots(s.dir)
}

// getDBPath returns the path to the database file for the most recent snapshot.
func (s *Store) getDBPath() (string, error) {
	snapshots, err := s.getSnapshots()
	if err != nil {
		return "", err
	}
	if len(snapshots) == 0 {
		return "", nil
	}
	return filepath.Join(s.dir, snapshots[len(snapshots)-1].ID+".db"), nil
}

// unsetFullNeeded removes the flag that indicates a full snapshot is needed.
func (s *Store) unsetFullNeeded() error {
	err := os.Remove(s.fullNeededPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// snapshotName generates a name for the snapshot.
func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

func parentDir(dir string) string {
	return filepath.Dir(dir)
}

func tmpName(path string) string {
	return path + tmpSuffix
}

func nonTmpName(path string) string {
	return strings.TrimSuffix(path, tmpSuffix)
}

func isTmpName(name string) bool {
	return filepath.Ext(name) == tmpSuffix
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func dirExists(path string) bool {
	stat, err := os.Stat(path)
	return err == nil && stat.IsDir()
}

func dirIsEmpty(dir string) (bool, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	return len(files) == 0, nil
}

func syncDir(dir string) error {
	fh, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fh.Close()
	return fh.Sync()
}

func removeDirSync(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return syncDirParentMaybe(dir)
}

// syncDirParentMaybe syncs the parent directory of the given
// directory, but only on non-Windows platforms.
func syncDirParentMaybe(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	return syncDir(parentDir(dir))
}

// syncDirMaybe syncs the given directory, but only on non-Windows platforms.
func syncDirMaybe(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	return syncDir(dir)
}

// removeAllPrefix removes all files in the given directory that have the given prefix.
func removeAllPrefix(path, prefix string) error {
	files, err := filepath.Glob(filepath.Join(path, prefix) + "*")
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := os.RemoveAll(f); err != nil {
			return err
		}
	}
	return nil
}

// metaPath returns the path to the meta file in the given directory.
func metaPath(dir string) string {
	return filepath.Join(dir, metaFileName)
}

// metaExists returns true if the meta file exists in the given directory.
func metaExists(dir string) bool {
	_, err := os.Stat(metaPath(dir))
	return !os.IsNotExist(err)
}

// readMeta is used to read the meta data in a given snapshot directory.
func readMeta(dir string) (*raft.SnapshotMeta, error) {
	fh, err := os.Open(metaPath(dir))
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

// writeMeta is used to write the meta data in a given snapshot directory.
func writeMeta(dir string, meta *raft.SnapshotMeta) error {
	fh, err := os.Create(metaPath(dir))
	if err != nil {
		return fmt.Errorf("error creating meta file: %v", err)
	}
	defer fh.Close()

	// Write out as JSON
	enc := json.NewEncoder(fh)
	if err = enc.Encode(meta); err != nil {
		return fmt.Errorf("failed to encode meta: %v", err)
	}

	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}

func updateMetaSize(dir string, sz int64) error {
	meta, err := readMeta(dir)
	if err != nil {
		return err
	}

	meta.Size = sz
	return writeMeta(dir, meta)
}

func openCloseDB(path string) error {
	d, err := db.Open(path, false, true)
	if err != nil {
		return err
	}
	return d.Close()
}

type cmpSnapshotMeta raft.SnapshotMeta

func (c *cmpSnapshotMeta) Less(other *cmpSnapshotMeta) bool {
	if c.Term != other.Term {
		return c.Term < other.Term
	}
	if c.Index != other.Index {
		return c.Index < other.Index
	}
	return c.ID < other.ID
}

type snapMetaSlice []*raft.SnapshotMeta

// Implement the sort interface for []*fileSnapshotMeta.
func (s snapMetaSlice) Len() int {
	return len(s)
}

func (s snapMetaSlice) Less(i, j int) bool {
	si := (*cmpSnapshotMeta)(s[i])
	sj := (*cmpSnapshotMeta)(s[j])
	return si.Less(sj)
}

func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
