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
	"strconv"
	sync "sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/db"
)

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

var (
	// ErrRetainCountTooLow is returned when the retain count is too low.
	ErrRetainCountTooLow = errors.New("retain count must be >= 2")

	// ErrSnapshotNotFound is returned when a snapshot is not found.
	ErrSnapshotNotFound = errors.New("snapshot not found")

	// ErrSnapshotBaseMissing is returned when a snapshot base SQLite file is missing.
	ErrSnapshotBaseMissing = errors.New("snapshot base SQLite file missing")
)

// Meta represents the metadata for a snapshot.
type Meta struct {
	raft.SnapshotMeta
	Full bool
}

// WALName returns the name of the WAL file for the snapshot.
func (m *Meta) WALName() string {
	return m.ID + ".wal"
}

// String returns a string representation of the snapshot metadata.
func (m *Meta) String() string {
	return fmt.Sprintf(`{Meta ID=%s,Full=%t}`, m.ID, m.Full)
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

	sink := NewSink(s, s.rootDir, currGenDir, nextGenDir, meta)
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
				snapWALFilePath := filepath.Join(genDir, snap.WALName())
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
		return meta, str, nil
	}
	return nil, nil, ErrSnapshotNotFound
}

// Dir returns the directory where the snapshots are stored.
func (s *Store) Dir() string {
	return s.rootDir
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
		n++
	}
	return n, nil
}

// ReapSnapshots removes snapshots that are no longer needed. It does this by
// checkpointing WAL-based snapshots into the base SQLite file. The function
// returns the number of snapshots removed, or an error. The retain parameter
// specifies the number of snapshots to retain.
func (s *Store) ReapSnapshots(dir string, retain int) (int, error) {
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

	sort.Sort(metaSlice(snapshots))
	n := 0
	for _, snap := range snapshots[0 : len(snapshots)-retain] {
		if !snap.Full {
			// We need to checkpoint the WAL files starting with the oldest snapshot. We'll
			// do this by opening the base SQLite file and then replaying the WAL files into it.
			// We'll then delete each snapshot once we've checkpointed it.
			baseSqliteFilePath := filepath.Join(dir, baseSqliteFile)
			snapWALFilePath := filepath.Join(dir, snap.WALName())            // Path to the WAL file from the snapshot
			walToCheckpointFilePath := filepath.Join(dir, baseSqliteWALFile) // Path to the WAL file to checkpoint
			if err := os.Rename(snapWALFilePath, walToCheckpointFilePath); err != nil {
				s.logger.Printf("failed to rename WAL from snapshot %s: %s", snapWALFilePath, err)
				return n, err
			}

			// We don't need the snapshot directory anymore
			snapDirPath := filepath.Join(dir, snap.ID)
			if err := removeDirSync(snapDirPath); err != nil {
				s.logger.Printf("failed to remove incremental snapshot directory %s: %s", snapDirPath, err)
				return n, err
			}

			// Checkpoint the WAL file into the base SQLite file
			if err := db.ReplayWAL(baseSqliteFilePath, []string{walToCheckpointFilePath}, false); err != nil {
				s.logger.Printf("failed to checkpoint WAL file %s: %s", walToCheckpointFilePath, err)
				return n, err
			}
		}
		snapDirPath := filepath.Join(dir, snap.ID) // Path to the snapshot directory
		if err := removeDirSync(snapDirPath); err != nil {
			s.logger.Printf("failed to remove snapshot directory %s: %s", snapDirPath, err)
			return n, err
		}

		n++
		s.logger.Printf("reaped snapshot %s successfully", snap)
	}

	return n, nil
}

// getSnapshots returns a list of all the snapshots in the given directory, sorted from
// most recently created to oldest created.
func (s *Store) getSnapshots(dir string) ([]*Meta, error) {
	var snapMeta []*Meta

	snapshots, err := os.ReadDir(dir)
	if err != nil {
		// If the directory doesn't exist, that's fine, just return an empty list
		if os.IsNotExist(err) {
			return snapMeta, nil
		}
		return nil, err
	}

	// Populate the metadata
	for _, snap := range snapshots {
		// Ignore any files
		if !snap.IsDir() {
			continue
		}

		// Ignore any temporary snapshots
		if isTmpName(snap.Name()) {
			continue
		}

		// Try to read the meta data
		meta, err := s.readMeta(filepath.Join(dir, snap.Name()))
		if err != nil {
			return nil, fmt.Errorf("failed to read meta for snapshot %s: %s", snap.Name(), err)
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
		// An empty current generation is useless.
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

	// If the oldest snapshot directory is an incremental snapshot, but we don't
	// have its WAL file in the current generation directory, it means we never got
	// to delete the snapshot directory after renaming its WAL file to the right path
	// for checkpointing. We should just delete the snapshot directory
	sort.Sort(sort.Reverse(metaSlice(snapshots)))
	if !snapshots[0].Full {
		snapWALPath := filepath.Join(currGenDir, snapshots[0].ID+".wal")
		if !fileExists(snapWALPath) {
			if err := removeDirSync(filepath.Join(currGenDir, snapshots[0].ID)); err != nil {
				return fmt.Errorf("failed to remove obsolete incremental snapshot %s: %s",
					snapshots[0].ID, err)
			}
			s.logger.Printf("removed obsolete incremental snapshot %s", snapshots[0].ID)
		}
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

func removeDirSync(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		if err := syncDir(filepath.Dir(dir)); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(dir string) error {
	fh, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fh.Close()
	return fh.Sync()
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
