package snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

// Store is a store for snapshots.
type Store struct {
	rootDir        string
	generationsDir string
	logger         *log.Logger
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

// Create creates a new Sink object, ready for writing a snapshot.
func (s *Store) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	currGenDir, err := s.GetCurrentGenerationDir()
	if err != nil {
		return nil, err
	}
	nextGenDir, err := s.GetNextGenerationDir()
	if err != nil {
		return nil, err
	}

	meta := &Meta{
		SnapshotMeta: raft.SnapshotMeta{
			Index:              index,
			Term:               term,
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
			Version:            version,
		},
	}

	sink := NewSink(s, s.rootDir, currGenDir, nextGenDir, meta)
	if err := sink.Open(); err != nil {
		return nil, fmt.Errorf("failed to open Sink: %v", err)
	}
	return sink, nil
}

// List returns a list of all the snapshots in the Store.
func (s *Store) List() ([]*raft.SnapshotMeta, error) {
	gen, err := s.GetCurrentGenerationDir()
	if err != nil {
		return nil, err
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
		files := []string{filepath.Join(genDir, baseSqliteFile)}
		for _, snap := range snapshots {
			if !snap.Full {
				files = append(files, filepath.Join(genDir, snap.ID, snapWALFile))
			}
			if snap.ID == id {
				// Stop after we've reached the requested snapshot
				break
			}
		}

		str, err := NewFullStream(files...)
		if err != nil {
			return nil, nil, err
		}
		return nil, str, nil
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
	currGenDir, err := s.GetCurrentGenerationDir()
	if !dirExists(currGenDir) {
		return true
	}
	if err != nil {
		return false
	}
	return !fileExists(filepath.Join(currGenDir, baseSqliteFile))
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

// GetGenerations returns a list of all the generations, sorted
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
// It is not guaranteed to exist.
func (s *Store) GetCurrentGenerationDir() (string, error) {
	generations, err := s.GetGenerations()
	if err != nil {
		return "", err
	}
	if len(generations) == 0 {
		return filepath.Join(s.generationsDir, firstGeneration), nil
	}
	return filepath.Join(s.generationsDir, generations[len(generations)-1]), nil
}

// Reap reaps old generations, and reaps snapshots within the remaining generation.
func (s *Store) Reap() error {
	generations, err := s.GetGenerations()
	if err != nil {
		return err
	}

	if len(generations) < 2 {
		return nil
	}

	// Reap all but the last generation.
	for i := 0; i < len(generations)-1; i++ {
		genDir := filepath.Join(s.generationsDir, generations[i])
		if err := os.RemoveAll(genDir); err != nil {
			return err
		}
	}

	currDir, err := s.GetCurrentGenerationDir()
	if err != nil {
		return err
	}
	_, err = s.reapSnapshots(currDir, 2)
	return err
}

// ReapSnapshots removes snapshots that are no longer needed. It does this by
// checkpointing WAL-based snapshots into the base SQLite file. The function
// returns the number of snapshots removed, or an error. The retain parameter
// specifies the number of snapshots to retain.
func (s *Store) reapSnapshots(dir string, retain int) (int, error) {
	// s.mu.Lock()
	// defer s.mu.Unlock()

	if retain < minSnapshotRetain {
		return 0, ErrRetainCountTooLow
	}

	snapshots, err := s.getSnapshots(dir)
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
	sort.Sort(metaSlice(snapshots))

	n := 0
	for _, snap := range snapshots[0 : len(snapshots)-retain] {
		baseSqliteFilePath := filepath.Join(dir, baseSqliteFile)
		snapDirPath := filepath.Join(dir, snap.ID)
		snapWALFilePath := filepath.Join(snapDirPath, snapWALFile)
		walToCheckpointFilePath := filepath.Join(dir, baseSqliteWALFile)

		// If the snapshot directory doesn't contain a WAL file, then the base SQLite
		// file is the snapshot state, and there is no checkpointing to do.
		if fileExists(snapWALFilePath) {
			// Move the WAL file to beside the base SQLite file
			if err := os.Rename(snapWALFilePath, walToCheckpointFilePath); err != nil {
				s.logger.Printf("failed to move WAL file %s: %s", snapWALFilePath, err)
				return n, err
			}

			// Checkpoint the WAL file into the base SQLite file
			if err := db.ReplayWAL(baseSqliteFilePath, []string{walToCheckpointFilePath}, false); err != nil {
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
	sort.Sort(sort.Reverse(metaSlice(snapMeta)))

	return snapMeta, nil
}

// readMeta is used to read the meta data for a given named backup
func (s *Store) readMeta(name string) (*Meta, error) {
	// Open the meta file
	metaPath := filepath.Join(s.rootDir, name, metaFileName)
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

func (s *Store) check() error {
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
