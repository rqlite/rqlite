package snapshot9

import (
	"bytes"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func Test_SnapshotMetaSort(t *testing.T) {
	metas := []*raft.SnapshotMeta{
		{
			ID:    "2-1017-1704807719996",
			Index: 1017,
			Term:  2,
		},
		{
			ID:    "2-1131-1704807720976",
			Index: 1131,
			Term:  2,
		},
	}
	sort.Sort(snapMetaSlice(metas))
	if metas[0].ID != "2-1017-1704807719996" {
		t.Errorf("Expected first snapshot ID to be 2-1017-1704807719996, got %s", metas[0].ID)
	}
	if metas[1].ID != "2-1131-1704807720976" {
		t.Errorf("Expected second snapshot ID to be 2-1131-1704807720976, got %s", metas[1].ID)
	}

	sort.Sort(sort.Reverse(snapMetaSlice(metas)))
	if metas[0].ID != "2-1131-1704807720976" {
		t.Errorf("Expected first snapshot ID to be 2-1131-1704807720976, got %s", metas[0].ID)
	}
	if metas[1].ID != "2-1017-1704807719996" {
		t.Errorf("Expected second snapshot ID to be 2-1017-1704807719996, got %s", metas[1].ID)
	}
}

func Test_NewStore(t *testing.T) {
	dir := t.TempDir()
	sp := &mockStateProvider{}
	str, err := NewStore(dir, sp)
	if err != nil {
		t.Fatalf("Failed to create new ReferentialStore: %v", err)
	}
	if str == nil {
		t.Fatalf("Failed to create new ReferentialStore")
	}
}

func Test_StoreEmpty(t *testing.T) {
	dir := t.TempDir()
	sp := &mockStateProvider{}
	store, err := NewStore(dir, sp)
	if err != nil {
		t.Fatalf("Failed to create new ReferentialStore: %v", err)
	}

	snaps, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 0 {
		t.Errorf("Expected no snapshots, got %d", len(snaps))
	}

	_, _, err = store.Open("nonexistent")
	if err != ErrSnapshotNotFound {
		t.Fatalf("Expected ErrSnapshotNotFound opening nonexistent snapshot, got %v", err)
	}

	n, err := store.Reap()
	if err != nil {
		t.Fatalf("Failed to reap snapshots from empty store: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected no snapshots reaped, got %d", n)
	}

	if _, err := store.Stats(); err != nil {
		t.Fatalf("Failed to get stats from empty store: %v", err)
	}
}

func Test_Store_CreateCancel(t *testing.T) {
	dir := t.TempDir()
	sp := &mockStateProvider{}
	str, err := NewStore(dir, sp)
	if err != nil {
		t.Fatalf("Failed to create new ReferentialStore: %v", err)
	}

	// Create a snapshot
	sink, err := str.Create(1, 2, 3, makeTestConfiguration("1", "localhost:1"), 1, nil)
	if err != nil {
		t.Fatalf("Failed to create sink: %v", err)
	}
	if sink.ID() == "" {
		t.Errorf("Expected sink ID to not be empty, got empty string")
	}

	// Should be a tmp directory with the name of the sink ID
	if !pathExists(dir + "/" + sink.ID() + tmpSuffix) {
		t.Errorf("Expected directory with name %s, but it does not exist", sink.ID())
	}

	// Test writing to the sink
	if n, err := sink.Write([]byte("hello")); err != nil {
		t.Fatalf("Failed to write to sink: %v", err)
	} else if n != 5 {
		t.Errorf("Expected 5 bytes written, got %d", n)
	}

	// Test canceling the sink
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Failed to cancel sink: %v", err)
	}

	// Should not be a tmp directory with the name of the sink ID
	if pathExists(dir + "/" + sink.ID() + tmpSuffix) {
		t.Errorf("Expected directory with name %s to not exist, but it does", sink.ID())
	}
}

func Test_StoreList(t *testing.T) {
	dir := t.TempDir()
	sp := &mockStateProvider{}
	store, err := NewStore(dir, sp)
	if err != nil {
		t.Fatalf("Failed to create new ReferentialStore: %v", err)
	}
	store.reapDisabled = true

	snaps, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 0 {
		t.Errorf("Expected 0 snapshots, got %d", len(snaps))
	}

	createSnapshot := func(id string, index, term, cfgIndex uint64, data []byte) {
		sink := NewSink(store, makeRaftMeta(id, index, term, cfgIndex))
		if sink == nil {
			t.Fatalf("Failed to create new sink")
		}
		if err := sink.Open(); err != nil {
			t.Fatalf("Failed to open sink: %v", err)
		}

		_, err := sink.Write(data)
		if err != nil {
			t.Fatalf("Failed to write to sink: %v", err)
		}
		if err := sink.Close(); err != nil {
			t.Fatalf("Failed to close sink: %v", err)
		}
	}

	proof1 := NewProof(100, time.Now())
	proof2 := NewProof(150, time.Now())
	createSnapshot("2-1017-1704807719996", 1017, 2, 1, mustMarshalProof(t, proof1))
	createSnapshot("2-1131-1704807720976", 1131, 2, 1, mustMarshalProof(t, proof2))
	snaps, err = store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Errorf("Expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].ID != "2-1131-1704807720976" {
		t.Errorf("Expected snapshot ID to be 2-1131-1704807720976, got %s", snaps[0].ID)
	}

	// Should be able to create a new snapshot sink now that the Snapshot from
	// Open is closed.
	sink, err := store.Create(1, 2, 3, makeTestConfiguration("1", "localhost:1"), 1, nil)
	if err != nil {
		t.Fatalf("Failed to create sink: %v", err)
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Failed to cancel sink: %v", err)
	}
}

func Test_Store_FullCycle(t *testing.T) {
	dir := t.TempDir()
	sp := &mockStateProvider{}
	store, err := NewStore(dir, sp)
	if err != nil {
		t.Fatalf("Failed to create new ReferentialStore: %v", err)
	}

	//////////////////////////////////////////////////////////////////////////
	// Create a referential Snapshot.
	sink, err := store.Create(1, 2, 3, makeTestConfiguration("1", "localhost:1"), 1, nil)
	if err != nil {
		t.Fatalf("Failed to create sink: %v", err)
	}
	proof := NewProof(100, time.Now())
	pb := mustMarshalProof(t, proof)
	if _, err := sink.Write(pb); err != nil {
		t.Fatalf("Failed to write proof: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Check that if the Provider Proof doesn't match the snapshot, we get an error.
	sp.proof = NewProof(100, time.Now())
	_, _, err = store.Open(sink.ID())
	if err != ErrSnapshotProofMismatch {
		t.Fatalf("Expected ErrSnapshotProofMismatch opening snapshot with mismatched Provider Proof")
	}

	// Now open the snapshot with matching Provider Proof, and ensure we get the expected data.
	sp.proof = proof
	sp.rc = io.NopCloser(strings.NewReader("hello world"))
	meta, rc, err := store.Open(sink.ID())
	if err != nil {
		t.Fatalf("Failed to open snapshot: %v", err)
	}
	if meta.ID != sink.ID() {
		t.Fatalf("Unexpected snapshot ID: %s", meta.ID)
	}
	if meta.Index != 2 {
		t.Fatalf("Unexpected snapshot index: %d", meta.Index)
	}
	if meta.Term != 3 {
		t.Fatalf("Unexpected snapshot term: %d", meta.Term)
	}
	if meta.ConfigurationIndex != 1 {
		t.Fatalf("Unexpected snapshot configuration index: %d", meta.ConfigurationIndex)
	}
	if meta.Version != 1 {
		t.Fatalf("Unexpected snapshot version: %d", meta.Version)
	}

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Failed to read snapshot data: %v", err)
	}

	if string(b) != "hello world" {
		t.Fatalf("Unexpected snapshot data: %s", b)
	}

	if err := rc.Close(); err != nil {
		t.Fatalf("Failed to close snapshot: %v", err)
	}

	// There should be only one snapshot returned by List(), and it should be the latest one.
	snaps, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].ID != sink.ID() {
		t.Fatalf("Unexpected snapshot ID: %s", snaps[0].ID)
	}

	// Check a State-level function here, for convenience.
	latestMeta, err := GetLatestSnapshotMeta(dir)
	if err != nil {
		t.Fatalf("Failed to get latest snapshot meta: %v", err)
	}
	if latestMeta.ID != sink.ID() {
		t.Fatalf("Unexpected snapshot ID: %s", latestMeta.ID)
	}
	if latestMeta.Index != 2 {
		t.Fatalf("Unexpected snapshot index: %d", latestMeta.Index)
	}
	if latestMeta.Term != 3 {
		t.Fatalf("Unexpected snapshot term: %d", latestMeta.Term)
	}

	//////////////////////////////////////////////////////////////////////////
	// Create a snapshot using actual SQLite data.
	sink, err = store.Create(1, 6, 7, makeTestConfiguration("1", "localhost:1"), 8, nil)
	if err != nil {
		t.Fatalf("Failed to create sink: %v", err)
	}

	sqliteData := mustReadFile(t, "testdata/full12k.db")
	if _, err := sink.Write(sqliteData); err != nil {
		t.Fatalf("Failed to write proof: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Now open the snapshot, and ensure we get the expected data.
	meta, rc, err = store.Open(sink.ID())
	if err != nil {
		t.Fatalf("Failed to open snapshot: %v", err)
	}
	if meta.ID != sink.ID() {
		t.Fatalf("Unexpected snapshot ID: %s", meta.ID)
	}
	if meta.Index != 6 {
		t.Fatalf("Unexpected snapshot index: %d", meta.Index)
	}
	if meta.Term != 7 {
		t.Fatalf("Unexpected snapshot term: %d", meta.Term)
	}
	if meta.ConfigurationIndex != 8 {
		t.Fatalf("Unexpected snapshot configuration index: %d", meta.ConfigurationIndex)
	}
	if meta.Version != 1 {
		t.Fatalf("Unexpected snapshot version: %d", meta.Version)
	}

	if !compareReaderToFile(t, rc, "testdata/full12k.db") {
		t.Fatalf("Snapshot data does not match")
	}

	if err := rc.Close(); err != nil {
		t.Fatalf("Failed to close snapshot: %v", err)
	}

	// There should still be only one snapshot returned by List().
	snaps, err = store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].ID != sink.ID() {
		t.Fatalf("Unexpected snapshot ID: %s", snaps[0].ID)
	}
}

func makeTestConfiguration(i, a string) raft.Configuration {
	return raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(i),
				Address: raft.ServerAddress(a),
			},
		},
	}
}

type mockStateProvider struct {
	proof *Proof
	rc    io.ReadCloser
}

func (m *mockStateProvider) Open() (*Proof, io.ReadCloser, error) {
	return m.proof, m.rc, nil
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func mustMarshalProof(t *testing.T, p *Proof) []byte {
	t.Helper()
	b, err := p.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal proof: %v", err)
	}
	return b
}

func mustOpenFile(t *testing.T, path string) *os.File {
	t.Helper()
	fd, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	return fd
}

func mustTouchFile(t *testing.T, path string) {
	t.Helper()
	fd, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to touch file: %v", err)
	}
	fd.Close()
}

func compareReaderToFile(t *testing.T, r io.Reader, path string) bool {
	t.Helper()
	fd := mustOpenFile(t, path)
	defer fd.Close()
	return compareReaderToReader(t, r, fd)
}

func compareReaderToReader(t *testing.T, r1, r2 io.Reader) bool {
	t.Helper()
	buf1, err := io.ReadAll(r1)
	if err != nil {
		t.Fatalf("Failed to read from reader 1: %v", err)
	}
	buf2, err := io.ReadAll(r2)
	if err != nil {
		t.Fatalf("Failed to read from reader 2: %v", err)
	}
	return bytes.Equal(buf1, buf2)
}
