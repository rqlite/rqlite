package snapshot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/db"
)

const (
	sqliteFilePath = "base-sqlite.db"
	walFilePath    = "wal"
	tmpSuffix      = ".tmp"
	metaFileName   = "meta.json"
)

// walSnapshotMeta is stored on disk. We also put a CRC
// on disk so that we can verify the snapshot.
type walSnapshotMeta struct {
	raft.SnapshotMeta
	CRC []byte // CRC of the data XXX shoudl decide if i really need this.
}

// walSnapshotSink is a sink for a snapshot.
type walSnapshotSink struct {
	dir       string // The directory to store the snapshot in.
	parentDir string // The parent directory of the snapshot.
	dataFd    *os.File
	dataHash  hash.Hash64

	multiW io.Writer

	meta *walSnapshotMeta

	logger *log.Logger
	closed bool
}

// Write writes the given bytes to the snapshot.
func (w *walSnapshotSink) Write(p []byte) (n int, err error) {
	return w.multiW.Write(p)
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

func (w *walSnapshotSink) writeMeta() error {
	fh, err := os.Create(filepath.Join(w.dir, metaFileName))
	if err != nil {
		return err
	}
	defer fh.Close()

	w.meta.CRC = w.dataHash.Sum(nil)

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

// WALStoreCheck performs a series of checks and cleanups on the WAL store. It should be
// called before opening the store.
func WALStoreCheck(dir string) error {
	// Verify checksums?
	// Check -- and repair -- dangling snapshots?
	// Remove any tmp directories
	return nil
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

	if err := w.writeMeta(); err != nil {
		return err
	}

	// Need to worry about crashes here. If we crash after the SQLite file is
	// synced, but before the snapshot directory is moved into place, we'll
	// have a dangling SQLite file. And perhaps other issues. XXXX Perform
	// cleanup at Store open.

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

	if err := w.writeMeta(); err != nil {
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

	// Cleanup the old snapshots

	w.logger.Printf("incremental snapshot (ID %s) written to %s", w.meta.ID, dstDir)
	return nil
}

// WALSnapshotStore is a Store for persisting Raft snapshots to disk. It allows
// WAL-based systems to store only the most recent WAL file for the snapshot, which
// minimizes disk IO.
type WALSnapshotStore struct {
	dir    string // The directory to store snapshots in.
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
			// XXX not setting size. Don't think it matters.
		},
	}

	var sink raft.SnapshotSink
	hash := crc64.New(crc64.MakeTable(crc64.ECMA))

	if s.hasBase() {
		walFd, err := os.Create(filepath.Join(snapshotPath, walFilePath))
		if err != nil {
			return nil, err
		}
		sink = &WALIncrementalSnapshotSink{
			walSnapshotSink: walSnapshotSink{
				dir:       snapshotPath,
				parentDir: s.dir,
				dataFd:    walFd,
				dataHash:  hash,
				multiW:    io.MultiWriter(walFd, hash),
				meta:      meta,
				logger:    log.New(os.Stderr, "[wal-inc-snapshot-sink] ", log.LstdFlags),
			},
		}
	} else {
		// If we're going to create a base, all previous snapshots are now invalid. There
		// shouldn't be any previous snapshots without a base being present, but if there
		// are, we need to clean them up. This could happen if someone manually deletes
		// the base file.
		if err := s.deleteAllSnapshots(); err != nil {
			return nil, err
		}

		sqliteFd, err := os.Create(filepath.Join(s.dir, sqliteFilePath) + tmpSuffix)
		if err != nil {
			return nil, err
		}

		sink = &WALFullSnapshotSink{
			walSnapshotSink: walSnapshotSink{
				dir:       snapshotPath,
				parentDir: s.dir,
				dataFd:    sqliteFd,
				dataHash:  crc64.New(crc64.MakeTable(crc64.ECMA)),
				multiW:    io.MultiWriter(sqliteFd, hash),
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
	// Try to read the meta data
	meta, err := s.readMeta(id)
	if err != nil {
		return nil, nil, err
	}

	// if the snapshot directory contains a WAL file, then it's an incremental snapshot
	// otherwise it's a full snapshot.
	if fileExists(filepath.Join(s.dir, id, walFilePath)) {
		fh, err := os.Open(filepath.Join(s.dir, id, walFilePath))
		if err != nil {
			return nil, nil, err
		}

		// Compute and verify the hash
		dataHash := crc64.New(crc64.MakeTable(crc64.ECMA))
		_, err = io.Copy(dataHash, fh)
		if err != nil {
			s.logger.Println("failed to read WAL file:", err)
			fh.Close()
			return nil, nil, err
		}
		computed := dataHash.Sum(nil)
		if !bytes.Equal(meta.CRC, computed) {
			s.logger.Println("CRC checksum failed", "stored", meta.CRC, "computed", computed)
			fh.Close()
			return nil, nil, fmt.Errorf("CRC mismatch")
		}

		// Rewind the file
		if _, err := fh.Seek(0, 0); err != nil {
			s.logger.Println("failed to rewind WAL file:", err)
			fh.Close()
			return nil, nil, err
		}

		// Return the file XXXX -- this actually won't be the final implementation. What
		// the snapshot needs to be is the database and all WAL files.
		return &meta.SnapshotMeta, fh, nil
	}

	return nil, nil, nil
}

// ReapSnapshots removes snapshots that are no longer needed. It does this by
// checkpointing WAL-based snapshots into the base SQLite file. The function
// returns the number of snapshots removed, or an error
func (s *WALSnapshotStore) ReapSnapshots() (int, error) {
	snapshots, err := s.getSnapshots()
	if err != nil {
		s.logger.Printf("failed to get snapshots: %s", err)
		return 0, err
	}

	// Keeping 2 snapshots -- 1 full and 1 incremental --  makes it much easier to
	// reason about the fixing up the Snapshot store if we crash in the middle of reaping.
	if len(snapshots) <= 2 {
		return 0, nil
	}

	// We have at least 3 incremental snapshots. We need to checkpoint the WAL files
	// starting with the oldest snapshot. We'll do this by opening the base SQLite file
	// and then replaying the WAL files into it. We'll then delete the snapshot.
	n := 0
	sort.Sort(sort.Reverse(snapMetaSlice(snapshots)))
	for _, snap := range snapshots[1 : len(snapshots)-1] {
		// Move the WAL file to beside the base SQLite file
		walPath := filepath.Join(s.dir, snap.ID, walFilePath)
		if err := os.Rename(walPath, filepath.Join(s.dir, walFilePath)); err != nil {
			s.logger.Printf("failed to move WAL file %s: %s", walPath, err)
			return 0, err
		}

		// Checkpoint the WAL file into the base SQLite file
		if db.ReplayWAL(filepath.Join(s.dir, sqliteFilePath), []string{walPath}, false) != nil {
			s.logger.Printf("failed to replay WAL file %s: %s", walPath, err)
			return 0, err
		}

		// Delete the snapshot directory that contained the WAL file
		if err := os.RemoveAll(filepath.Join(s.dir, snap.ID)); err != nil {
			s.logger.Printf("failed to delete snapshot %s: %s", snap.ID, err)
			return 0, err
		}
		n++
	}

	return n, nil
}

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
	return fileExists(filepath.Join(s.dir, sqliteFilePath))
}

func (s *WALSnapshotStore) deleteAllSnapshots() error {
	dirs, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	for _, d := range dirs {
		if !d.IsDir() || isTmpName(d.Name()) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(s.dir, d.Name())); err != nil {
			return err
		}
	}
	return nil
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
