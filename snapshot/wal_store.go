package snapshot

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

const (
	sqliteFilePath = "base-sqlite.db"
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
		parentFH, err := os.Open(w.parentDir)
		if err != nil {
			w.logger.Printf("failed to open snapshot parent directory: %s", err)
			return err
		}
		defer parentFH.Close()

		if err = parentFH.Sync(); err != nil {
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
	io.WriteCloser
}

// ID returns the ID of the snapshot.
func (w *WALIncrementalSnapshotSink) ID() string {
	return ""
}

// Cancel closes the snapshot and removes it.
func (w *WALIncrementalSnapshotSink) Cancel() error {
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

// Path returns the path to director this store uses
func (s *WALSnapshotStore) Path() string {
	return s.dir
}

// Create creates a new Sink object, ready for the writing a snapshot.
func (s *WALSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {

	if s.hasBase() {
		return &WALIncrementalSnapshotSink{}, nil
	}

	// If we're going to create a base, all previous snapshots are now invalid. XXXX

	// Create the file to where the SQLite file will be written.
	sqliteFd, err := os.Create(filepath.Join(s.dir, sqliteFilePath) + tmpSuffix)
	if err != nil {
		return nil, err
	}

	// Create a directory named for the snapshot. This directory won't actually contain
	// a WAL file, but will contain meta.
	snapshotName := snapshotName(term, index)
	snapshotPath := filepath.Join(s.dir, snapshotName+tmpSuffix)
	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
		return nil, err
	}

	fullSink := &WALFullSnapshotSink{
		dir:       snapshotPath,
		parentDir: s.dir,
		sqliteFd:  sqliteFd,
		meta: &raft.SnapshotMeta{
			ID:                 snapshotName,
			Index:              index,
			Term:               term,
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
			// XXX not setting size. Don't think it matters.
		},
		logger: log.New(os.Stderr, "[wal-snapshot-sink] ", log.LstdFlags),
	}

	if err := writeMeta(fullSink.meta, filepath.Join(fullSink.dir, metaFileName)); err != nil {
		return nil, err
	}

	return fullSink, nil
}

// List returns a list of all the snapshots in the store.
func (s *WALSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	return nil, nil
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
