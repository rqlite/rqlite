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
)

const (
	generationsDir = "generations"
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
	if err := s.ensureFirstGeneration(); err != nil {
		return nil, fmt.Errorf("creatinmg first generation failed: %s", err)
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
	snapshotName := snapshotName(term, index)
	snapshotPath := filepath.Join(currGenDir, snapshotName+tmpSuffix)
	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
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

	return NewSink(s, snapshotPath, meta), nil
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

		enc := NewFullEncoder()
		if err := enc.Open(files...); err != nil {
			return nil, nil, err
		}
		return &snapshots[0].SnapshotMeta, enc, nil
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
	return false
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
		if entry.IsDir() {
			if _, err := strconv.Atoi(entry.Name()); err != nil {
				continue
			}
			generations = append(generations, entry.Name())
		}
	}
	return generations, nil
}

func (s *Store) GetCurrentGenerationDir() (string, error) {
	generations, err := s.GetGenerations()
	if err != nil {
		return "", err
	}
	if len(generations) == 0 {
		return "", fmt.Errorf("no generations found")
	}
	return filepath.Join(s.generationsDir, generations[len(generations)-1]), nil
}

func (s *Store) ensureFirstGeneration() error {
	generations, err := s.GetGenerations()
	if err != nil {
		return err
	}
	if len(generations) == 0 {
		if err := os.MkdirAll(filepath.Join(s.generationsDir, "0000000001"), 0755); err != nil {
			return err
		}
	}
	return nil
}

// getSnapshots returns a list of all the snapshots in the given directory, sorted from
// most recently created to oldest created.
func (s *Store) getSnapshots(dir string) ([]*Meta, error) {
	snapshots, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// Populate the metadata
	var snapMeta []*Meta
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
