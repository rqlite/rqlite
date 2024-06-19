package snapshot9

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/db"
	"github.com/rqlite/rqlite/v8/rsync"
)

const (
	snapshotsReaped        = "snapshots_reaped"
	snapshotsReapedFail    = "snapshots_reaped_failed"
	snapshotCreateMRSWFail = "snapshot_create_mrsw_fail"
	snapshotOpenMRSWFail   = "snapshot_open_mrsw_fail"
)

const (
	tmpSuffix     = ".tmp"
	metaFileName  = "meta.json"
	stateFileName = "state.bin"
)

var (
	// ErrSnapshotNotFound is returned when a snapshot is not found.
	ErrSnapshotNotFound = fmt.Errorf("snapshot not found")

	// ErrSnapshotProofMismatch is returned when a snapshot proof does not match the source.
	ErrSnapshotProofMismatch = fmt.Errorf("snapshot proof mismatch")
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
	stats.Add(snapshotsReaped, 0)
	stats.Add(snapshotsReapedFail, 0)
	stats.Add(snapshotCreateMRSWFail, 0)
	stats.Add(snapshotOpenMRSWFail, 0)
}

type StateProvider interface {
	Open() (*Proof, io.ReadCloser, error)
}

type ReferentialStore struct {
	dir string
	sp  StateProvider

	mrsw   *rsync.MultiRSW
	logger *log.Logger

	LogReaping   bool
	reapDisabled bool
}

func NewReferentialStore(dir string, sp StateProvider) *ReferentialStore {
	return &ReferentialStore{
		dir:    dir,
		sp:     sp,
		mrsw:   rsync.NewMultiRSW(),
		logger: log.New(os.Stderr, "[snapshot-store] ", log.LstdFlags),
	}
}

// Create creates a new Sink object, ready for writing a snapshot. Sinks make certain assumptions about
// the state of the store, and if those assumptions were changed by another Sink writing to the store
// it could cause failures. Therefore we only allow 1 Sink to be in existence at a time. This shouldn't
// be a problem, since snapshots are taken infrequently in one at a time.
func (s *ReferentialStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
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

// Open opens the snapshot with the given ID. Close() must be called on the snapshot
// when finished with it.
func (s *ReferentialStore) Open(id string) (_ *raft.SnapshotMeta, _ io.ReadCloser, retErr error) {
	if err := s.mrsw.BeginRead(); err != nil {
		stats.Add(snapshotOpenMRSWFail, 1)
		return nil, nil, err
	}
	defer func() {
		if retErr != nil {
			s.mrsw.EndRead()
		}
	}()

	if !dirExists(filepath.Join(s.dir, id)) {
		return nil, nil, ErrSnapshotNotFound
	}

	meta, err := readMeta(filepath.Join(s.dir, id))
	if err != nil {
		return nil, nil, err
	}

	var rc io.ReadCloser
	// Check if state file is a valid SQLite file. If so, just hand that out.
	statePath := filepath.Join(s.dir, id, stateFileName)
	if db.IsValidSQLiteFile(statePath) {
		fd, err := os.Open(statePath)
		if err != nil {
			return nil, nil, err
		}

		sz, err := fileSize(statePath)
		if err != nil {
			return nil, nil, err
		}
		meta.Size = sz
		rc = fd
	} else {
		// Otherwise we must have a Proof file. Check if the source it references
		// is available from the Provider.
		b, err := os.ReadFile(statePath)
		if err != nil {
			return nil, nil, err
		}
		proof, err := UnmarshalProof(b)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal proof: %w", err)
		}

		srcProof, src, err := s.sp.Open()
		if err != nil {
			return nil, nil, err
		}

		if !proof.Equals(srcProof) {
			return nil, nil, ErrSnapshotProofMismatch
		}
		meta.Size = proof.SizeBytes
		rc = src
	}

	return meta, NewLockingSnapshot(rc, s), nil
}

// List returns a list of all the snapshots in the Store. In practice, this will at most be
// a list of 1, and that will be the newest snapshot available.
func (s *ReferentialStore) List() ([]*raft.SnapshotMeta, error) {
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

// Dir returns the directory where the snapshots are stored.
func (s *ReferentialStore) Dir() string {
	return s.dir
}

// Reap reaps all snapshots, except the most recent one. Returns the number of
// snapshots reaped. This function does not take the Store CAS lock, and so
// it is up to the caller to ensure no other operations are happening on the
// Store.
func (s *ReferentialStore) Reap() (retN int, retErr error) {
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

// Stats returns stats about the Snapshot Store. This function may return
// an error if the Store is in an inconsistent state. In that case the stats
// returned may be incomplete or invalid.
func (s *ReferentialStore) Stats() (map[string]interface{}, error) {
	snapshots, err := s.getSnapshots()
	if err != nil {
		return nil, err
	}
	snapsAsIDs := make([]string, len(snapshots))
	for i, snap := range snapshots {
		snapsAsIDs[i] = snap.ID
	}
	return map[string]interface{}{
		"dir":       s.dir,
		"snapshots": snapsAsIDs,
	}, nil
}

// getSnapshots returns a list of all snapshots in the store, sorted
// from oldest to newest.
func (s *ReferentialStore) getSnapshots() ([]*raft.SnapshotMeta, error) {
	return getSnapshots(s.dir)
}

// metaPath returns the path to the meta file in the given directory.
func metaPath(dir string) string {
	return filepath.Join(dir, metaFileName)
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

// snapshotName generates a name for the snapshot.
func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
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

func fileSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.IsDir()
}

func syncDir(dir string) error {
	fh, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fh.Close()
	return fh.Sync()
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
