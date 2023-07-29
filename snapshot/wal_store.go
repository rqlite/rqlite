package snapshot

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

const (
	sqliteFilePath = "base-sqlite.db"
	walFilePath    = "wal"
	tmpSuffix      = ".tmp"
	metaFileName   = "meta.json"
)

// WALFullSnapshotSink is a sink for a full snapshot.
type WALFullSnapshotSink struct {
	dir       string // The directory to store the snapshot in.
	parentDir string // The parent directory of the snapshot.

	sqliteFd *os.File
	meta     *raft.SnapshotMeta

	logger *log.Logger

	closed bool
}

// Write writes the given bytes to the snapshot.
func (w *WALFullSnapshotSink) Write(p []byte) (n int, err error) {
	return w.sqliteFd.Write(p)
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

	if err := w.sqliteFd.Sync(); err != nil {
		w.logger.Printf("failed syncing snapshot SQLite file: %s", err)
		return err
	}

	if err := w.sqliteFd.Close(); err != nil {
		w.logger.Printf("failed closing snapshot SQLite file: %s", err)
		return err
	}

	// Need to worry about crashes here. If we crash after the SQLite file is
	// synced, but before the snapshot directory is moved into place, we'll
	// have a dangling SQLite file. And perhaps other issues. XXXX

	if err := moveFromTmp(w.sqliteFd.Name()); err != nil {
		w.logger.Printf("failed to move SQLite file into place: %s", err)
		return err
	}

	if err := moveFromTmp(w.dir); err != nil {
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

	// Reap old snapshots here XXXX -- best effort! Don't cleanup
	return nil
}

// ID returns the ID of the snapshot.
func (w *WALFullSnapshotSink) ID() string {
	return w.meta.ID
}

// Cancel closes the snapshot and removes it.
func (w *WALFullSnapshotSink) Cancel() error {
	w.closed = true
	return w.cleanup()
}

func (w *WALFullSnapshotSink) cleanup() error {
	w.sqliteFd.Close()
	os.Remove(w.sqliteFd.Name())
	os.Remove(nonTmpName(w.sqliteFd.Name()))
	os.RemoveAll(w.dir)
	os.RemoveAll(nonTmpName(w.dir))
	return nil
}

// WALIncrementalSnapshotSink is a sink for an incremental snapshot.
type WALIncrementalSnapshotSink struct {
	dir       string // The directory to store the snapshot in.
	parentDir string // The parent directory of the snapshot.

	walFd *os.File
	meta  *raft.SnapshotMeta

	logger *log.Logger

	closed bool
}

// ID returns the ID of the snapshot.
func (w *WALIncrementalSnapshotSink) ID() string {
	return w.meta.ID
}

// Write writes the given bytes to the snapshot.
func (w *WALIncrementalSnapshotSink) Write(p []byte) (n int, err error) {
	return w.walFd.Write(p)
}

// Cancel closes the snapshot and removes it.
func (w *WALIncrementalSnapshotSink) Cancel() error {
	return nil
}

func (w *WALIncrementalSnapshotSink) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.walFd.Sync(); err != nil {
		w.logger.Printf("failed syncing snapshot SQLite file: %s", err)
		return err
	}

	if err := w.walFd.Close(); err != nil {
		w.logger.Printf("failed closing snapshot SQLite file: %s", err)
		return err
	}

	if err := moveFromTmp(w.dir); err != nil {
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

	return nil
}

// WALSnapshotStore is a Store for persisting Raft snapshots to disk. It allows
// WAL-based systems to store only the most recent WAL file for the snapshot, which
// minimizes disk IO.
type WALSnapshotStore struct {
	dir string // The directory to store snapshots in.

	logger *log.Logger
}

// NewWALSnapshotStore returns a new WALSnapshotStore.
func NewWALSnapshotStore(dir string) *WALSnapshotStore {
	return &WALSnapshotStore{
		dir:    dir,
		logger: log.New(os.Stderr, "[wal-snapshot-store] ", log.LstdFlags),
	}
}

// Path returns the path to directory this store uses
func (s *WALSnapshotStore) Path() string {
	return s.dir
}

// Create creates a new Sink object, ready for the writing a snapshot.
func (s *WALSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {

	snapshotName := snapshotName(term, index)
	snapshotPath := filepath.Join(s.dir, snapshotName+tmpSuffix)
	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
		return nil, err
	}

	meta := &raft.SnapshotMeta{
		ID:                 snapshotName,
		Index:              index,
		Term:               term,
		Configuration:      configuration,
		ConfigurationIndex: configurationIndex,
		// XXX not setting size. Don't think it matters.
	}

	var sink raft.SnapshotSink
	if s.hasBase() {
		walFd, err := os.Create(filepath.Join(snapshotPath, walFilePath))
		if err != nil {
			return nil, err
		}
		sink = &WALIncrementalSnapshotSink{
			dir:       snapshotPath,
			parentDir: s.dir,
			walFd:     walFd,
			meta:      meta,
			logger:    log.New(os.Stderr, "[wal-inc-snapshot-sink] ", log.LstdFlags),
		}
	} else {
		// If we're going to create a base, all previous snapshots are now invalid. XXXX
		// Create the file to where the SQLite file will be written.
		sqliteFd, err := os.Create(filepath.Join(s.dir, sqliteFilePath) + tmpSuffix)
		if err != nil {
			return nil, err
		}

		sink = &WALFullSnapshotSink{
			dir:       snapshotPath,
			parentDir: s.dir,
			sqliteFd:  sqliteFd,
			meta:      meta,
			logger:    log.New(os.Stderr, "[wal-full-snapshot-sink] ", log.LstdFlags),
		}
	}

	if err := writeMeta(meta, filepath.Join(snapshotPath, metaFileName)); err != nil {
		return nil, err
	}

	return sink, nil
}

// List returns a list of all the snapshots in the store.
func (s *WALSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	// Get the eligible snapshots
	snapshots, err := os.ReadDir(s.dir)
	if err != nil {
		s.logger.Printf("failed to scan snapshot directory: %s", err)
		return nil, err
	}

	// Populate the metadata
	var snapMeta []*raft.SnapshotMeta
	for _, snap := range snapshots {
		// Ignore any files
		if !snap.IsDir() {
			continue
		}

		// Ignore any temporary snapshots
		snapName := snap.Name()
		if strings.HasSuffix(snapName, tmpSuffix) {
			s.logger.Printf("ignoring temporary snapshot: %s", snapName)
			continue
		}

		// Try to read the meta data
		meta, err := readMeta(s.dir, snapName)
		if err != nil {
			s.logger.Printf("failed to read metadata in %s: %s", snapName, err)
			continue
		}

		// Append, but only return up to the retain count XXXX
		snapMeta = append(snapMeta, meta)
	}

	// Sort the snapshot, reverse so we get new -> old
	sort.Sort(sort.Reverse(snapMetaSlice(snapMeta)))

	return snapMeta, nil
}

// Open opens the snapshot with the given ID.
func (s *WALSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, nil
}

// return true if sqliteFilePath exists in the snapshot directory
func (s *WALSnapshotStore) hasBase() bool {
	return fileExists(filepath.Join(s.dir, sqliteFilePath))
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

func moveFromTmp(src string) error {
	dst := nonTmpName(src)
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func writeMeta(meta *raft.SnapshotMeta, metaPath string) error {
	var err error
	// Open the meta file
	var fh *os.File
	fh, err = os.Create(metaPath)
	if err != nil {
		return err
	}
	defer fh.Close()

	// Write out as JSON
	enc := json.NewEncoder(fh)
	if err = enc.Encode(meta); err != nil {
		return err
	}

	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}

// readMeta is used to read the meta data for a given named backup
func readMeta(dir, name string) (*raft.SnapshotMeta, error) {
	// Open the meta file
	metaPath := filepath.Join(dir, name, metaFileName)
	fh, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	// Read in the JSON
	meta := &raft.SnapshotMeta{}
	dec := json.NewDecoder(fh)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

type snapMetaSlice []*raft.SnapshotMeta

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
