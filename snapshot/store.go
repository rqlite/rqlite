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
	"runtime"
	"sort"
	"strconv"
	"strings"
	sync "sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/db"
)

func init() {
	stats = expvar.NewMap("snapshot")
	ResetStats()
}

const (
	minSnapshotRetain = 2

	generationsDir  = "generations"
	firstGeneration = "0000000001"

	baseSqliteFile    = "base.sqlite"
	baseSqliteWALFile = "base.sqlite-wal"
	snapWALFile       = "wal"
	metaFileName      = "meta.json"
	tmpSuffix         = ".tmp"
)

const (
	persistSize          = "latest_persist_size"
	persistDuration      = "latest_persist_duration"
	numSnapshotsReaped   = "num_snapshots_reaped"
	numGenerationsReaped = "num_generations_reaped"
)

var (
	// ErrRetainCountTooLow is returned when the retain count is too low.
	ErrRetainCountTooLow = errors.New("retain count must be >= 2")

	// ErrSnapshotNotFound is returned when a snapshot is not found.
	ErrSnapshotNotFound = errors.New("snapshot not found")

	// ErrSnapshotBaseMissing is returned when a snapshot base SQLite file is missing.
	ErrSnapshotBaseMissing = errors.New("snapshot base SQLite file missing")
)

// stats captures stats for the Store.
var stats *expvar.Map

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(persistSize, 0)
	stats.Add(persistDuration, 0)
	stats.Add(numSnapshotsReaped, 0)
	stats.Add(numGenerationsReaped, 0)
}

// Meta represents the metadata for a snapshot.
type Meta struct {
	raft.SnapshotMeta
	Full bool
}

// LockingSink is a wrapper around a SnapshotSink that ensures that the
// Store has handed out only 1 sink at a time.
type LockingSink struct {
	raft.SnapshotSink
	str *Store
}

// Close closes the sink, unlocking the Store for creation of a new sink.
func (s *LockingSink) Close() error {
	s.str.sinkMu.Unlock()
	return s.SnapshotSink.Close()
}

// Cancel cancels the sink, unlocking the Store for creation of a new sink.
func (s *LockingSink) Cancel() error {
	s.str.sinkMu.Unlock()
	return s.SnapshotSink.Cancel()
}

// Store is a store for snapshots.
type Store struct {
	rootDir        string
	workDir        string
	generationsDir string

	sinkMu sync.Mutex

	noAutoreap bool
	logger     *log.Logger
}

// NewStore creates a new Store object.
func NewStore(dir string) (*Store, error) {
	genDir := filepath.Join(dir, generationsDir)
	if err := os.MkdirAll(genDir, 0755); err != nil {
		return nil, err
	}
	s := &Store{
		rootDir:        dir,
		workDir:        filepath.Join(dir, "scratchpad"),
		generationsDir: genDir,
		logger:         log.New(os.Stderr, "[snapshot-store] ", log.LstdFlags),
	}

	if err := s.check(); err != nil {
		return nil, fmt.Errorf("check failed: %s", err)
	}
	return s, nil
}

// Create creates a new Sink object, ready for writing a snapshot. Sinks make certain assumptions about
// the state of the store, and if those assumptions were changed by another Sink writing to the store
// it could cause failures. Therefore we only allow 1 Sink to be in existence at a time. This shouldn't
// be a problem, since snapshots are taken infrequently in one at a time.
func (s *Store) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (retSink raft.SnapshotSink, retErr error) {
	s.sinkMu.Lock()
	defer func() {
		if retErr != nil {
			s.sinkMu.Unlock()
		}
	}()

	currGenDir, ok, err := s.GetCurrentGenerationDir()
	if err != nil {
		return nil, err
	}
	nextGenDir, err := s.GetNextGenerationDir()
	if err != nil {
		return nil, err
	}
	if !ok {
		// With an empty store, the snapshot will be written to the same directory
		// regardless of whether it's a full or incremental snapshot.
		currGenDir = nextGenDir
	}

	meta := &Meta{
		SnapshotMeta: raft.SnapshotMeta{
			ID:                 snapshotName(term, index),
			Index:              index,
			Term:               term,
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
			Version:            version,
		},
	}

	sink := NewSink(s, s.workDir, currGenDir, nextGenDir, meta)
	if err := sink.Open(); err != nil {
		sink.Cancel()
		return nil, fmt.Errorf("failed to open Sink: %v", err)
	}
	return &LockingSink{sink, s}, nil
}

// List returns a list of all the snapshots in the Store.
func (s *Store) List() ([]*raft.SnapshotMeta, error) {
	gen, ok, err := s.GetCurrentGenerationDir()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	snapshots, err := s.getSnapshots(gen)
	if err != nil {
		return nil, err
	}

	// Convert to the type Raft expects and make only 1 available.
	var snaps = []*raft.SnapshotMeta{}
	if len(snapshots) > 0 {
		snaps = append(snaps, &snapshots[0].SnapshotMeta)
	}
	return snaps, nil
}

// Open opens the snapshot with the given ID.
func (s *Store) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	generations, err := s.GetGenerations()
	if err != nil {
		return nil, nil, err
	}
	var meta *raft.SnapshotMeta
	for i := len(generations) - 1; i >= 0; i-- {
		genDir := filepath.Join(s.generationsDir, generations[i])
		snapshots, err := s.getSnapshots(genDir)
		if err != nil {
			return nil, nil, err
		}
		if len(snapshots) == 0 {
			continue
		}
		sort.Sort(metaSlice(snapshots))
		if !metaSlice(snapshots).Contains(id) {
			// Try the previous generation.
			continue
		}

		// Always include the base SQLite file. There may not be a snapshot directory
		// if it's been checkpointed due to snapshot-reaping.
		baseSqliteFilePath := filepath.Join(genDir, baseSqliteFile)
		if !fileExists(baseSqliteFilePath) {
			return nil, nil, ErrSnapshotBaseMissing
		}
		files := []string{baseSqliteFilePath}
		for _, snap := range snapshots {
			if !snap.Full {
				// Only include WAL files for incremental snapshots, since base SQLite database
				// is always included
				snapWALFilePath := filepath.Join(genDir, snap.ID, snapWALFile)
				if !fileExists(snapWALFilePath) {
					return nil, nil, fmt.Errorf("WAL file %s does not exist", snapWALFilePath)
				}
				files = append(files, snapWALFilePath)
			}
			if snap.ID == id {
				// Stop after we've reached the requested snapshot
				meta = &raft.SnapshotMeta{
					ID:                 snap.ID,
					Index:              snap.Index,
					Term:               snap.Term,
					Configuration:      snap.Configuration,
					ConfigurationIndex: snap.ConfigurationIndex,
					Version:            snap.Version,
				}
				break
			}
		}

		str, err := NewFullStream(files...)
		if err != nil {
			return nil, nil, err
		}
		meta.Size = str.Size()
		s.logger.Printf("opened snapshot %s successfully (size=%d)", id, meta.Size)
		return meta, str, nil
	}
	return nil, nil, ErrSnapshotNotFound
}

// Dir returns the directory where the snapshots are stored.
func (s *Store) Dir() string {
	return s.rootDir
}

// Stats returns stats about the Snapshot Store.
func (s *Store) Stats() (map[string]interface{}, error) {
	ng, err := s.GetNextGeneration()
	if err != nil {
		return nil, err
	}
	dirSize, err := dirSize(s.rootDir)
	if err != nil {
		return nil, err

	}
	stats := map[string]interface{}{
		"root_dir":        s.rootDir,
		"size":            dirSize,
		"full_needed":     s.FullNeeded(),
		"next_generation": ng,
		"auto_reap":       !s.noAutoreap,
	}

	snaps, err := s.List()
	if err != nil {
		return nil, err
	}
	if len(snaps) > 0 {
		var snapsAvailable []string
		for i := range snaps {
			snapsAvailable = append(snapsAvailable, snaps[i].ID)
		}
		stats["available"] = snapsAvailable
	}

	generations, err := s.GetGenerations()
	if err != nil {
		return nil, err
	}
	if len(generations) > 0 {
		stats["generations"] = generations
	}

	return stats, nil
}

// FullNeeded returns true if the next type of snapshot needed
// by the Store is a full snapshot.
func (s *Store) FullNeeded() bool {
	currGenDir, ok, err := s.GetCurrentGenerationDir()
	if err != nil {
		return false
	}
	return !ok || !fileExists(filepath.Join(currGenDir, baseSqliteFile))
}

// GetNextGeneration returns the name of the next generation.
func (s *Store) GetNextGeneration() (string, error) {
	generations, err := s.GetGenerations()
	if err != nil {
		return "", err
	}
	nextGen := 1
	if len(generations) > 0 {
		i, err := strconv.Atoi(generations[len(generations)-1])
		if err != nil {
			return "", err
		}
		nextGen = i + 1
	}
	return fmt.Sprintf("%010d", nextGen), nil
}

// GetNextGenerationDir returns the directory path of the next generation.
// It is not guaranteed to exist.
func (s *Store) GetNextGenerationDir() (string, error) {
	nextGen, err := s.GetNextGeneration()
	if err != nil {
		return "", err
	}
	return filepath.Join(s.generationsDir, nextGen), nil
}

// GetGenerations returns a list of all existing generations, sorted
// from oldest to newest.
func (s *Store) GetGenerations() ([]string, error) {
	entries, err := os.ReadDir(s.generationsDir)
	if err != nil {
		return nil, err
	}
	var generations []string
	for _, entry := range entries {
		if !entry.IsDir() || isTmpName(entry.Name()) {
			continue
		}

		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		generations = append(generations, entry.Name())
	}
	return generations, nil
}

// GetCurrentGenerationDir returns the directory path of the current generation.
// If there are no generations, the function returns false, but no error.
func (s *Store) GetCurrentGenerationDir() (string, bool, error) {
	generations, err := s.GetGenerations()
	if err != nil {
		return "", false, err
	}
	if len(generations) == 0 {
		return "", false, nil
	}
	return filepath.Join(s.generationsDir, generations[len(generations)-1]), true, nil
}

// Reap reaps old generations, and reaps snapshots within the remaining generation.
func (s *Store) Reap() error {
	if _, err := s.ReapGenerations(); err != nil {
		return fmt.Errorf("failed to reap generations during reap: %s", err)
	}

	currDir, ok, err := s.GetCurrentGenerationDir()
	if err != nil {
		return fmt.Errorf("failed to get current generation directory during reap: %s", err)
	}
	if ok {
		_, err = s.ReapSnapshots(currDir, 2)
		if err != nil {
			return fmt.Errorf("failed to reap snapshots during reap: %s", err)
		}
	}
	return nil
}

// ReapGenerations removes old generations. It returns the number of generations
// removed, or an error.
func (s *Store) ReapGenerations() (int, error) {
	generations, err := s.GetGenerations()
	if err != nil {
		return 0, err
	}
	if len(generations) == 0 {
		return 0, nil
	}
	n := 0
	for i := 0; i < len(generations)-1; i++ {
		genDir := filepath.Join(s.generationsDir, generations[i])
		if err := os.RemoveAll(genDir); err != nil {
			return n, err
		}
		s.logger.Printf("reaped generation %s successfully", generations[i])
		n++
	}
	stats.Add(numGenerationsReaped, int64(n))
	return n, nil
}

// ReapSnapshots removes snapshots that are no longer needed. It does this by
// checkpointing WAL-based snapshots into the base SQLite file. The function
// returns the number of snapshots removed, or an error. The retain parameter
// specifies the number of snapshots to retain.
func (s *Store) ReapSnapshots(dir string, retain int) (n int, err error) {
	defer func() {
		stats.Add(numSnapshotsReaped, int64(n))
	}()

	if retain < minSnapshotRetain {
		return 0, ErrRetainCountTooLow
	}

	snapshots, err := s.getSnapshots(dir)
	if err != nil {
		s.logger.Printf("failed to get snapshots in directory %s: %s", dir, err)
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
	sort.Sort(metaSlice(snapshots))

	baseSqliteFilePath := filepath.Join(dir, baseSqliteFile)

	n = 0
	for _, snap := range snapshots[0 : len(snapshots)-retain] {
		snapDirPath := filepath.Join(dir, snap.ID)                       // Path to the snapshot directory
		walFileInSnapshot := filepath.Join(snapDirPath, snapWALFile)     // Path to the WAL file in the snapshot
		walToCheckpointFilePath := filepath.Join(dir, baseSqliteWALFile) // Path to the WAL file to checkpoint

		// If the snapshot directory doesn't contain a WAL file, then the base SQLite
		// file is the snapshot state, and there is no checkpointing to do.
		if fileExists(walFileInSnapshot) {
			// Copy the WAL file from the snapshot to a temporary location beside the base SQLite file.
			// We do this so that we only delete the snapshot directory once we can be sure that
			// we've copied it out fully. Renaming is not atomic on every OS, so let's be sure. We
			// also use a temporary file name, so we know where the WAL came from if we exit here
			// and need to clean up on a restart.
			if err := copyWALFromSnapshot(walFileInSnapshot, walToCheckpointFilePath); err != nil {
				s.logger.Printf("failed to copy WAL file from snapshot %s: %s", walFileInSnapshot, err)
				return n, err
			}

			// Checkpoint the WAL file into the base SQLite file
			if err := db.ReplayWAL(baseSqliteFilePath, []string{walToCheckpointFilePath}, false); err != nil {
				s.logger.Printf("failed to checkpoint WAL file %s: %s", walToCheckpointFilePath, err)
				return n, err
			}
		} else {
			if err := removeDirSync(snapDirPath); err != nil {
				s.logger.Printf("failed to remove full snapshot directory %s: %s", snapDirPath, err)
				return n, err
			}
		}

		n++
		s.logger.Printf("reaped snapshot %s successfully", snap.ID)
	}

	return n, nil
}

// getSnapshots returns a list of all the snapshots in the given directory, sorted from
// most recently created to oldest created.
func (s *Store) getSnapshots(dir string) ([]*Meta, error) {
	var snapMeta []*Meta

	entries, err := os.ReadDir(dir)
	if err != nil {
		// If the directory doesn't exist, that's fine, just return an empty list
		if os.IsNotExist(err) {
			return snapMeta, nil
		}
		return nil, err
	}

	// Populate the metadata
	for _, entry := range entries {
		// Ignore any files or temporary snapshots
		if !entry.IsDir() || isTmpName(entry.Name()) {
			continue
		}

		// Try to read the meta data
		meta, err := s.readMeta(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("failed to read meta for snapshot %s: %s", entry.Name(), err)
		}
		snapMeta = append(snapMeta, meta)
	}

	// Sort the snapshot, reverse so we get new -> old
	sort.Sort(sort.Reverse(metaSlice(snapMeta)))
	return snapMeta, nil
}

// readMeta is used to read the meta data in a given snapshot directory.
func (s *Store) readMeta(dir string) (*Meta, error) {
	// Open the meta file
	metaPath := filepath.Join(dir, metaFileName)
	fh, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	// Read in the JSON
	meta := &Meta{}
	dec := json.NewDecoder(fh)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *Store) check() (retError error) {
	defer s.logger.Printf("check complete")
	s.logger.Printf("checking snapshot store at %s", s.rootDir)
	var n int

	if err := s.resetWorkDir(); err != nil {
		return fmt.Errorf("failed to reset work directory: %s", err)
	}

	// Simplify logic by reaping generations first.
	n, err := s.ReapGenerations()
	if err != nil {
		return fmt.Errorf("failed to reap generations: %s", err)
	}
	s.logger.Printf("reaped %d generations", n)

	// Remove any temporary generational directories. They represent operations
	// that were interrupted.
	entries, err := os.ReadDir(s.generationsDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !isTmpName(entry.Name()) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(s.generationsDir, entry.Name())); err != nil {
			return fmt.Errorf("failed to remove temporary generation directory %s: %s", entry.Name(), err)
		}
		n++
	}
	s.logger.Printf("removed %d temporary generation directories", n)

	// Remove any temporary files in the current generation.
	currGenDir, ok, err := s.GetCurrentGenerationDir()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	entries, err = os.ReadDir(currGenDir)
	if err != nil {
		return err
	}
	n = 0
	for _, entry := range entries {
		if isTmpName(entry.Name()) {
			if err := os.RemoveAll(filepath.Join(currGenDir, entry.Name())); err != nil {
				return fmt.Errorf("failed to remove temporary file %s: %s", entry.Name(), err)
			}
			n++
		}
	}
	s.logger.Printf("removed %d temporary files from current generation", n)

	baseSqliteFilePath := filepath.Join(currGenDir, baseSqliteFile)
	baseSqliteWALFilePath := filepath.Join(currGenDir, baseSqliteWALFile)

	// Any snapshots in the current generation?
	snapshots, err := s.getSnapshots(currGenDir)
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %s", err)
	}
	if len(snapshots) == 0 {
		// An empty current generation is useless. This could happen if the very first
		// snapshot was interrupted after writing the base SQLite file, but before
		// moving its snapshot directory into place.
		if err := os.RemoveAll(currGenDir); err != nil {
			return fmt.Errorf("failed to remove empty current generation directory %s: %s", currGenDir, err)
		}
		s.logger.Printf("removed an empty current generation directory")
		return nil
	}

	// If we have no base file, we shouldn't have any snapshot directories. If we
	// do it's an inconsistent state which we cannot repair, and needs to be flagged.
	if !fileExists(baseSqliteFilePath) {
		return ErrSnapshotBaseMissing
	}
	s.logger.Printf("found base SQLite file at %s", baseSqliteFilePath)

	// If we have a WAL file in the current generation which ends with the same ID as
	// the oldest snapshot, then the copy of the WAL from the snapshot and subsequent
	// checkpointing was interrupted. We need to redo the move-from-snapshot operation.
	sort.Sort(metaSlice(snapshots))
	walSnapshotCopyPath := walSnapCopyName(currGenDir, snapshots[0].ID)
	snapDirPath := filepath.Join(currGenDir, snapshots[0].ID)
	if fileExists(walSnapshotCopyPath) {
		s.logger.Printf("found uncheckpointed copy of WAL file from snapshot %s", snapshots[0].ID)
		if err := os.Remove(walSnapshotCopyPath); err != nil {
			return fmt.Errorf("failed to remove copy of WAL file %s: %s", walSnapshotCopyPath, err)
		}
		if err := copyWALFromSnapshot(snapDirPath, baseSqliteWALFilePath); err != nil {
			s.logger.Printf("failed to copy WAL file from snapshot %s: %s", snapshots[0].ID, err)
			return err
		}
		// Now we can remove the snapshot directory.
		if err := removeDirSync(snapDirPath); err != nil {
			return fmt.Errorf("failed to remove snapshot directory %s: %s", snapDirPath, err)
		}
		s.logger.Printf("completed copy of WAL file from snapshot %s", snapshots[0].ID)
	}

	// If we have a base SQLite file, and a WAL file sitting beside it, this implies
	// that we were interrupted before completing a checkpoint operation, as part of
	// reaping snapshots. Complete the checkpoint operation now.
	if fileExists(baseSqliteFilePath) && fileExists(baseSqliteWALFilePath) {
		if err := db.ReplayWAL(baseSqliteFilePath, []string{baseSqliteWALFilePath},
			false); err != nil {
			return fmt.Errorf("failed to replay WALs: %s", err)
		}
		if err := os.Remove(baseSqliteWALFilePath); err != nil {
			return fmt.Errorf("failed to remove WAL file %s: %s", baseSqliteWALFilePath, err)
		}
		s.logger.Printf("completed checkpoint of WAL file %s", baseSqliteWALFilePath)
	}
	return nil
}

func (s *Store) resetWorkDir() error {
	if err := os.RemoveAll(s.workDir); err != nil {
		return fmt.Errorf("failed to remove work directory %s: %s", s.workDir, err)
	}
	if err := os.MkdirAll(s.workDir, 0755); err != nil {
		return fmt.Errorf("failed to create work directory %s: %s", s.workDir, err)
	}
	return nil
}

// copyWALFromSnapshot copies the WAL file from the snapshot at the given path
// to the file at the given path. It does this in stages, so that we can be sure
// that the copy is complete before deleting the snapshot directory.
func copyWALFromSnapshot(srcWALPath string, dstWALPath string) error {
	snapName := filepath.Base(srcWALPath)
	snapDirPath := filepath.Dir(srcWALPath)
	dstWALDir := filepath.Dir(dstWALPath)
	walFileInSnapshotCopy := walSnapCopyName(dstWALDir, snapName)
	if err := copyFileSync(srcWALPath, walFileInSnapshotCopy); err != nil {
		return fmt.Errorf("failed to copy WAL file %s from snapshot: %s", srcWALPath, err)
	}

	// Delete the snapshot directory, since we have what we need now.
	if err := removeDirSync(snapDirPath); err != nil {
		return fmt.Errorf("failed to remove incremental snapshot directory %s: %s", snapDirPath, err)
	}

	// NOT HANDLING CRASHING HERE. XXXX FIX IN CHECK

	// Move the WAL file to the correct name for checkpointing.
	if err := os.Rename(walFileInSnapshotCopy, dstWALPath); err != nil {
		return fmt.Errorf("failed to move WAL file %s: %s", walFileInSnapshotCopy, err)
	}
	return nil
}

// walSnapCopyName returns the path of the file used for the intermediate copy of
// the WAL file, for a given source snapshot. dstDir is the directory where the
// copy will be placed, and snapName is the name of the source snapshot.
func walSnapCopyName(dstDir, snapName string) string {
	return filepath.Join(dstDir, baseSqliteWALFile+"."+snapName)
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

func copyFileSync(src, dst string) error {
	srcFd, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFd.Close()
	dstFd, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFd.Close()
	if _, err = io.Copy(dstFd, srcFd); err != nil {
		return err
	}
	return dstFd.Sync()
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

func moveFromTmpSync(src string) (string, error) {
	dst := nonTmpName(src)
	if err := os.Rename(src, dst); err != nil {
		return "", err
	}
	return dst, syncDirParentMaybe(dst)
}

func removeDirSync(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return syncDirParentMaybe(dir)
}

func syncDir(dir string) error {
	fh, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fh.Close()
	return fh.Sync()
}

// syncDirParentMaybe syncs the parent directory of the given
// directory, but only on non-Windows platforms.
func syncDirParentMaybe(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	return syncDir(parentDir(dir))
}

// snapshotName generates a name for the snapshot.
func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

// metaSlice is a sortable slice of Meta, which are sorted
// by term, index, and then ID. Snapshots are sorted from oldest to newest.
type metaSlice []*Meta

func (s metaSlice) Len() int {
	return len(s)
}

func (s metaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s metaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s metaSlice) Contains(id string) bool {
	for _, snap := range s {
		if snap.ID == id {
			return true
		}
	}
	return false
}

// dirSize returns the total size of all files in the given directory
func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			// If the file doesn't exist, we can ignore it. Snapshot files might
			// disappear during walking.
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
