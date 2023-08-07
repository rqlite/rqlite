package snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/db"
	"github.com/rqlite/rqlite/snapshot/streamer"
)

const (
	minSnapshotRetain        = 2
	defaultReapCheckDuration = time.Minute

	baseSqliteFile    = "base-sqlite.db"
	baseSqliteWALFile = "base-sqlite.db-wal"
	snapWALFile       = "wal"
	tmpSuffix         = ".tmp"
	metaFileName      = "meta.json"
)

var (
	// ErrRetainCountTooLow is returned when the retain count is too low.
	ErrRetainCountTooLow = errors.New("retain count must be >= 2")

	// ErrSnapshotNotFound is returned when a snapshot is not found.
	ErrSnapshotNotFound = errors.New("snapshot not found")

	// ErrSnapshotBaseMissing is returned when a snapshot base SQLite file is missing.
	ErrSnapshotBaseMissing = errors.New("snapshot base SQLite file missing")
)

// walSnapshotMeta is stored on disk. We also put a CRC
// on disk so that we can verify the snapshot.
type walSnapshotMeta struct {
	raft.SnapshotMeta
	Full bool
}

func (w *walSnapshotMeta) String() string {
	return fmt.Sprintf("walSnapshotMeta{ID:%s, Full:%v}", w.ID, w.Full)
}

// walSnapshotSink is a sink for a snapshot.
type walSnapshotSink struct {
	store *WALSnapshotStore

	dir       string   // The directory to store the snapshot in.
	parentDir string   // The parent directory of the snapshot.
	dataFd    *os.File // The file to write the snapshot data to.

	meta *walSnapshotMeta

	logger *log.Logger
	closed bool
}

// Write writes the given bytes to the snapshot.
func (w *walSnapshotSink) Write(p []byte) (n int, err error) {
	return w.dataFd.Write(p)
}

// Cancel closes the snapshot and removes it.
func (w *walSnapshotSink) Cancel() error {
	w.closed = true
	return w.cleanup()
}

// ID returns the ID of the snapshot.
func (w *walSnapshotSink) ID() string {
	return w.meta.ID
}

func (w *walSnapshotSink) cleanup() error {
	w.dataFd.Close()
	os.Remove(w.dataFd.Name())
	os.Remove(nonTmpName(w.dataFd.Name()))
	os.RemoveAll(w.dir)
	os.RemoveAll(nonTmpName(w.dir))
	return nil
}

func (w *walSnapshotSink) writeMeta(full bool) error {
	fh, err := os.Create(filepath.Join(w.dir, metaFileName))
	if err != nil {
		return err
	}
	defer fh.Close()

	w.meta.Full = full

	// Write out as JSON
	enc := json.NewEncoder(fh)
	if err = enc.Encode(w.meta); err != nil {
		return err
	}

	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}

// WALFullSnapshotSink is a sink for a full snapshot.
type WALFullSnapshotSink struct {
	walSnapshotSink
}

// Close closes the snapshot.
func (w *WALFullSnapshotSink) Close() (retErr error) {
	if w.closed {
		return nil
	}
	w.closed = true

	defer func() {
		if retErr != nil {
			w.cleanup()
		}
	}()

	if err := w.dataFd.Sync(); err != nil {
		w.logger.Printf("failed syncing snapshot SQLite file: %s", err)
		return err
	}

	if err := w.dataFd.Close(); err != nil {
		w.logger.Printf("failed closing snapshot SQLite file: %s", err)
		return err
	}

	if err := w.writeMeta(true); err != nil {
		return err
	}

	if _, err := moveFromTmp(w.dataFd.Name()); err != nil {
		w.logger.Printf("failed to move SQLite file into place: %s", err)
		return err
	}

	dstDir, err := moveFromTmp(w.dir)
	if err != nil {
		w.logger.Printf("failed to move snapshot directory into place: %s", err)
		return err
	}

	// Sync parent directory to ensure snapshot is visible, but it's only
	// needed on *nix style file systems.
	if runtime.GOOS != "windows" {
		if err := syncDir(w.parentDir); err != nil {
			w.logger.Printf("failed syncing parent directory: %s", err)
			return err
		}
	}

	w.logger.Printf("full snapshot (ID %s) written to %s", w.meta.ID, dstDir)
	return nil
}

// WALIncrementalSnapshotSink is a sink for an incremental snapshot.
type WALIncrementalSnapshotSink struct {
	walSnapshotSink
}

// Close closes the snapshot.
func (w *WALIncrementalSnapshotSink) Close() (retErr error) {
	if w.closed {
		return nil
	}
	w.closed = true

	defer func() {
		if retErr != nil {
			w.cleanup()
		}
	}()

	if err := w.dataFd.Sync(); err != nil {
		w.logger.Printf("failed syncing snapshot SQLite file: %s", err)
		return err
	}

	if err := w.dataFd.Close(); err != nil {
		w.logger.Printf("failed closing snapshot SQLite file: %s", err)
		return err
	}

	if err := w.writeMeta(false); err != nil {
		return err
	}

	dstDir, err := moveFromTmp(w.dir)
	if err != nil {
		w.logger.Printf("failed to move snapshot directory into place: %s", err)
		return err
	}

	// Sync parent directory to ensure snapshot is visible, but it's only
	// needed on *nix style file systems.
	if runtime.GOOS != "windows" {
		if err := syncDir(w.parentDir); err != nil {
			w.logger.Printf("failed syncing parent directory: %s", err)
			return err
		}
	}
	w.logger.Printf("incremental snapshot (ID %s) written to %s", w.meta.ID, dstDir)
	return nil
}

// WALSnapshotStore is a Store for persisting Raft snapshots to disk. It allows
// WAL-based systems to store only the most recent WAL file for the snapshot, which
// minimizes disk IO.
type WALSnapshotStore struct {
	dir string // The directory to store snapshots in.

	mu           sync.RWMutex
	reapInterval time.Duration
	done         chan struct{}

	logger *log.Logger
}

// NewWALSnapshotStore returns a new WALSnapshotStore.
func NewWALSnapshotStore(dir string) (*WALSnapshotStore, error) {
	s := &WALSnapshotStore{
		dir:          dir,
		reapInterval: defaultReapCheckDuration,
		done:         make(chan struct{}),
		logger:       log.New(os.Stderr, "[wal-snapshot-store] ", log.LstdFlags),
	}
	if s.check() != nil {
		return nil, fmt.Errorf("failed WALSnapshotStore check")
	}
	return s, nil
}

// RunReaper runs the snapshot reaping process in the background.
func (s *WALSnapshotStore) RunReaper() {
	go s.runReaper()
}

// Close closes the WAL snapshot store.
func (s *WALSnapshotStore) Close() error {
	s.logger.Println("closing WAL snapshot store")
	close(s.done)
	return nil
}

// Path returns the path to the directory this store uses
func (s *WALSnapshotStore) Path() string {
	return s.dir
}

// Create creates a new Sink object, ready for writing a snapshot.
func (s *WALSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {

	snapshotName := snapshotName(term, index)
	snapshotPath := filepath.Join(s.dir, snapshotName+tmpSuffix)
	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
		return nil, err
	}

	meta := &walSnapshotMeta{
		SnapshotMeta: raft.SnapshotMeta{
			ID:                 snapshotName,
			Index:              index,
			Term:               term,
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
		},
	}

	var sink raft.SnapshotSink
	if s.hasBase() {
		walFd, err := os.Create(filepath.Join(snapshotPath, snapWALFile))
		if err != nil {
			return nil, err
		}
		sink = &WALIncrementalSnapshotSink{
			walSnapshotSink: walSnapshotSink{
				store:     s,
				dir:       snapshotPath,
				parentDir: s.dir,
				dataFd:    walFd,
				meta:      meta,
				logger:    log.New(os.Stderr, "[wal-inc-snapshot-sink] ", log.LstdFlags),
			},
		}
	} else {
		sqliteFd, err := os.Create(s.basePath() + tmpSuffix)
		if err != nil {
			return nil, err
		}

		sink = &WALFullSnapshotSink{
			walSnapshotSink: walSnapshotSink{
				store:     s,
				dir:       snapshotPath,
				parentDir: s.dir,
				dataFd:    sqliteFd,
				meta:      meta,
				logger:    log.New(os.Stderr, "[wal-full-snapshot-sink] ", log.LstdFlags),
			},
		}
	}

	return sink, nil
}

// List returns a list of all the snapshots in the store.
func (s *WALSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	snapshots, err := s.getSnapshots()
	if err != nil {
		return nil, err
	}

	// Convert to the public type and make only 1 available.
	var snaps = []*raft.SnapshotMeta{}
	if len(snapshots) > 0 {
		snaps = append(snaps, &snapshots[0].SnapshotMeta)
	}
	return snaps, nil
}

// Open opens the snapshot with the given ID.
func (s *WALSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, err := s.readMeta(id)
	if err != nil {
		return nil, nil, err
	}

	snapshots, err := s.getSnapshots()
	if err != nil {
		return nil, nil, err
	}
	sort.Sort(snapMetaSlice(snapshots))
	if !snapMetaSlice(snapshots).Contains(id) {
		return nil, nil, ErrSnapshotNotFound
	}

	// Always include the base SQLite file. There may not be a snapshot directory
	// for it if it's been checkpointed due to snapshot-reaping.
	files := []string{s.basePath()}
	for _, snap := range snapshots {
		if !snap.Full {
			files = append(files, filepath.Join(s.dir, snap.ID, snapWALFile))
		}
		if snap.ID == id {
			// Stop after we've reached the requested snapshot
			break
		}
	}
	return &meta.SnapshotMeta, NewWALSnapshotState(streamer.NewEncoder(files), s), nil
}

// ReapSnapshots removes snapshots that are no longer needed. It does this by
// checkpointing WAL-based snapshots into the base SQLite file. The function
// returns the number of snapshots removed, or an error. The retain parameter
// specifies the number of snapshots to retain.
func (s *WALSnapshotStore) ReapSnapshots(retain int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if retain < minSnapshotRetain {
		return 0, ErrRetainCountTooLow
	}

	snapshots, err := s.getSnapshots()
	if err != nil {
		s.logger.Printf("failed to get snapshots: %s", err)
		return 0, err
	}

	// Keeping multiple snapshots makes it much easier to reason about the fixing
	// up the Snapshot store if we crash in the middle of snapshotting or reaping.
	if len(snapshots) <= retain {
		return 0, nil
	}

	// We need to checkpoint the WAL files starting with the oldest snapshot. We'll
	// do this by opening the base SQLite file and then replaying the WAL files into it.
	// We'll then delete each snapshot once we've checkpointed it.
	sort.Sort(snapMetaSlice(snapshots))

	n := 0
	for _, snap := range snapshots[0 : len(snapshots)-retain] {
		snapDirPath := filepath.Join(s.dir, snap.ID)
		snapWALFilePath := filepath.Join(snapDirPath, snapWALFile)
		walToCheckpointFilePath := filepath.Join(s.dir, baseSqliteWALFile)

		// If the snapshot directory doesn't contain a WAL file, then the base SQLite
		// file is the snapshot state, and there is no checkpointing to do.
		if fileExists(snapWALFilePath) {
			// Move the WAL file to beside the base SQLite file
			if err := os.Rename(snapWALFilePath, walToCheckpointFilePath); err != nil {
				s.logger.Printf("failed to move WAL file %s: %s", snapWALFilePath, err)
				return n, err
			}

			// Checkpoint the WAL file into the base SQLite file
			if err := db.ReplayWAL(s.basePath(), []string{walToCheckpointFilePath}, false); err != nil {
				s.logger.Printf("failed to checkpoint WAL file %s: %s", walToCheckpointFilePath, err)
				return n, err
			}
		}

		// Delete the snapshot directory, since the state is now in the base SQLite file.
		if err := os.RemoveAll(snapDirPath); err != nil {
			s.logger.Printf("failed to delete snapshot %s: %s", snap.ID, err)
			return n, err
		}
		n++
		s.logger.Printf("reaped snapshot %s successfully", snap.ID)
	}

	return n, nil
}

// ReplayWALs returns a path to a SQLite path, created from copying the
// base SQLite file and replaying all WAL files into it.
func (s *WALSnapshotStore) ReplayWALs() (string, error) {
	// make a temporary directory
	tmpDir, err := os.MkdirTemp("", "wal-snapshot-store-replay")
	if err != nil {
		return "", err
	}

	// copy the base SQLite file into the temporary directory
	tmpSqliteFilePath := filepath.Join(tmpDir, baseSqliteFile)
	if err := copyFile(s.basePath(), tmpSqliteFilePath); err != nil {
		return "", err
	}

	snaps, err := s.getSnapshots()
	if err != nil {
		return "", err
	}
	sort.Sort(snapMetaSlice(snaps))

	walFiles := []string{}
	for i, snap := range snaps {
		if snap.Full {
			continue
		}

		// Copy the WAL file to the temporary directory
		snapWALFilePath := filepath.Join(s.dir, snap.ID, snapWALFile)
		tmpSnapWALFilePath := filepath.Join(tmpDir, snapWALFile+fmt.Sprintf("_%d", i))
		if err := copyFile(snapWALFilePath, tmpSnapWALFilePath); err != nil {
			return "", err
		}
		walFiles = append(walFiles, tmpSnapWALFilePath)
	}

	if err := db.ReplayWAL(tmpSqliteFilePath, walFiles, false); err != nil {
		return "", err
	}
	return tmpSqliteFilePath, nil
}

// Stats returns stats about the snapshot store.
func (s *WALSnapshotStore) Stats() (map[string]interface{}, error) {
	snaps, err := s.getSnapshots()
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"snapshots": snaps,
	}, nil
}

// check checks the Store for inconsistencies, and repairs it as needed.
func (s *WALSnapshotStore) check() error {
	// Remove any temporary files or directories. They represent operations
	// that were interrupted.
	paths, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if isTmpName(path.Name()) {
			if err := os.RemoveAll(filepath.Join(s.dir, path.Name())); err != nil {
				return err
			}
		}
	}

	// If we have no base file, we shouldn't have any snapshot directories. If we
	// do it's an inconsistent state which we cannot repair, and needs to be flagged.
	if !s.hasBase() {
		snapshots, err := s.getSnapshots()
		if err != nil {
			return err
		}
		if len(snapshots) > 0 {
			return ErrSnapshotBaseMissing
		}
	}

	// If we have a base SQLite file, but no snapshot directories, this implies
	// that we crashed after creating the base SQLite file, but before officially
	// creating the first full snapshot. We need to delete the base SQLite file,
	// which will cause the next snapshot to be a full snapshot.
	if s.hasBase() {
		snapshots, err := s.getSnapshots()
		if err != nil {
			return err
		}
		if len(snapshots) == 0 {
			if err := os.Remove(s.basePath()); err != nil {
				return err
			}
		}
	}

	// If we have a base SQLite file, and a WAL file sitting beside it, this implies
	// that we were interrupted before completing a checkpoint operation, as part of
	// reaping snapshots. Complete the checkpoint operation now.
	if s.hasBase() {
		if fileExists(filepath.Join(s.dir, baseSqliteWALFile)) {
			if err := db.ReplayWAL(s.basePath(), []string{filepath.Join(s.dir, baseSqliteWALFile)},
				false); err != nil {
				return err
			}
			if err := os.Remove(filepath.Join(s.dir, baseSqliteWALFile)); err != nil {
				return err
			}
		}
	}

	// If we have any incremental snapshot directories which are missing a WAL file,
	// this implies that we crashed after checkpointing the WAL file but before deleting
	// the snapshot directory the WAL file came from. Delete that snapshot directory now,
	// since that snapshot is no longer available.
	snapshots, err := s.getSnapshots()
	if err != nil {
		return err
	}
	for _, snap := range snapshots {
		snapshotDirPath := filepath.Join(s.dir, snap.ID)
		if !snap.Full && !fileExists(filepath.Join(snapshotDirPath, snapWALFile)) {
			if err := os.RemoveAll(snapshotDirPath); err != nil {
				return err
			}
		}
	}
	return nil
}

// getSnapshots returns a list of all the snapshots in the store, sorted from
// most recently created to oldest created.
func (s *WALSnapshotStore) getSnapshots() ([]*walSnapshotMeta, error) {
	snapshots, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}

	// Populate the metadata
	var snapMeta []*walSnapshotMeta
	for _, snap := range snapshots {
		// Ignore any files
		if !snap.IsDir() {
			continue
		}

		// Ignore any temporary snapshots
		snapName := snap.Name()
		if isTmpName(snapName) {
			continue
		}

		// Try to read the meta data
		meta, err := s.readMeta(snapName)
		if err != nil {
			return nil, err
		}
		snapMeta = append(snapMeta, meta)
	}

	// Sort the snapshot, reverse so we get new -> old
	sort.Sort(sort.Reverse(snapMetaSlice(snapMeta)))

	return snapMeta, nil
}

// runReaper runs the snapshot reaping process.
func (s *WALSnapshotStore) runReaper() {
	ticker := time.NewTicker(s.reapInterval)
	defer ticker.Stop()

	s.logger.Println("starting snapshot reaper")
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			if _, err := s.ReapSnapshots(minSnapshotRetain); err != nil {
				s.logger.Printf("failed to reap snapshots: %s", err)
			}
		}
	}
}

// readMeta is used to read the meta data for a given named backup
func (s *WALSnapshotStore) readMeta(name string) (*walSnapshotMeta, error) {
	// Open the meta file
	metaPath := filepath.Join(s.dir, name, metaFileName)
	fh, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	// Read in the JSON
	meta := &walSnapshotMeta{}
	dec := json.NewDecoder(fh)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// return true if sqliteFilePath exists in the snapshot directory
func (s *WALSnapshotStore) hasBase() bool {
	return fileExists(s.basePath())
}

// basePath returns the path to the base SQLite file.
func (s *WALSnapshotStore) basePath() string {
	return filepath.Join(s.dir, baseSqliteFile)
}

// snapshotName generates a name for the snapshot.
func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

func syncDir(dir string) error {
	fh, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fh.Close()

	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}

func nonTmpName(path string) string {
	return strings.TrimSuffix(path, tmpSuffix)
}

func isTmpName(path string) bool {
	return strings.HasSuffix(path, tmpSuffix)
}

func moveFromTmp(src string) (string, error) {
	dst := nonTmpName(src)
	if err := os.Rename(src, dst); err != nil {
		return "", err
	}
	return dst, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// copyFile copies a file from src to dst. If dst already exists, it will be
// overwritten. The file will be copied with the same permissions as the
// original.
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return err
	}

	return destinationFile.Sync()
}

// snapMetaSlice is a sortable slice of walSnapshotMeta, which are sorted
// by term, index, and then ID. Snapshots are sorted from oldest to newest.
type snapMetaSlice []*walSnapshotMeta

func (s snapMetaSlice) Len() int {
	return len(s)
}

func (s snapMetaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s snapMetaSlice) Contains(id string) bool {
	for _, snap := range s {
		if snap.ID == id {
			return true
		}
	}
	return false
}
