package snapshot9

import (
	"io"
	"os"
	"sort"
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
