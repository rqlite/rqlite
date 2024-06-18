package snapshot9

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/rsync"
	"github.com/rqlite/rqlite/v8/snapshot9/proto"
)

const (
	snapshotCreateMRSWFail = "snapshot_create_mrsw_fail"
	snapshotOpenMRSWFail   = "snapshot_open_mrsw_fail"
)

const (
	tmpSuffix    = ".tmp"
	metaFileName = "meta.json"
	dataFileName = "data.bin"
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
	stats.Add(snapshotCreateMRSWFail, 0)
	stats.Add(snapshotOpenMRSWFail, 0)
}

type StateProvider interface {
	Open() (*proto.Proof, io.ReadCloser, error)
}

type ReferentialStore struct {
	dir string
	sp  StateProvider

	mrsw   *rsync.MultiRSW
	logger *log.Logger
}

func NewReferentialStore(dir string, sp StateProvider) *ReferentialStore {
	return &ReferentialStore{
		dir:    dir,
		sp:     sp,
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
	meta, err := readMeta(filepath.Join(s.dir, id))
	if err != nil {
		return nil, nil, err
	}
	proof, rc, err := s.sp.Open()
	if err != nil {
		return nil, nil, err
	}

	// Compare the proof with the snapshot proof
	_ = proof

	return meta, NewLockingSnapshot(rc, s), nil
}

// Dir returns the directory where the snapshots are stored.
func (s *ReferentialStore) Dir() string {
	return s.dir
}

// metaPath returns the path to the meta file in the given directory.
func metaPath(dir string) string {
	return filepath.Join(dir, metaFileName)
}

// proofPath returns the path to the proof file in the given directory.
func proofPath(dir string) string {
	return filepath.Join(dir, "proof")
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

func writeProof(dir string, proof *proto.Proof) error {
	fh, err := os.Create(proofPath(dir))
	if err != nil {
		return fmt.Errorf("error creating proof file: %v", err)
	}
	defer fh.Close()

	// Write out as JSON for human readability
	enc := json.NewEncoder(fh)
	if err = enc.Encode(proof); err != nil {
		return fmt.Errorf("failed to encode proof: %v", err)
	}

	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}

func readProof(dir string) (*proto.Proof, error) {
	fh, err := os.Open(proofPath(dir))
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	proof := &proto.Proof{}
	dec := json.NewDecoder(fh)
	if err := dec.Decode(proof); err != nil {
		return nil, err
	}
	return proof, nil
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

func removeIfEmpty(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		return os.Remove(path)
	}
	return nil
}
