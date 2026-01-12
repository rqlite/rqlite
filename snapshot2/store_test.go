package snapshot2

import (
	"os"
	"sort"
	"testing"

	"github.com/hashicorp/raft"
)

func Test_SnapshotMetaSort(t *testing.T) {
	metas := []*SnapshotMeta{
		{
			SnapshotMeta: &raft.SnapshotMeta{
				ID:    "2-1017-1704807719996",
				Index: 1017,
				Term:  2,
			},
			Filename: "db.sqlite",
			Type:     SnapshotMetaTypeFull,
		},
		{
			SnapshotMeta: &raft.SnapshotMeta{
				ID:    "2-1131-1704807720976",
				Index: 1131,
				Term:  2,
			},
			Filename: "wal",
			Type:     SnapshotMetaTypeIncremental,
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
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}

	if store.Dir() != dir {
		t.Errorf("Expected store directory to be %s, got %s", dir, store.Dir())
	}

	if store.Len() != 0 {
		t.Errorf("Expected store to have 0 snapshots, got %d", store.Len())
	}
}

func Test_StoreEmpty(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)

	snaps, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 0 {
		t.Errorf("Expected no snapshots, got %d", len(snaps))
	}

	if fn, err := store.FullNeeded(); err != nil {
		t.Fatalf("Failed to check if full snapshot needed: %v", err)
	} else if !fn {
		t.Errorf("Expected full snapshot needed, but it is not")
	}

	_, _, err = store.Open("nonexistent")
	if err != ErrSnapshotNotFound {
		t.Fatalf("Expected ErrSnapshotNotFound, got %v", err)
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

	li, tm, err := store.LatestIndexTerm()
	if err != nil {
		t.Fatalf("Failed to get latest index and term from empty store: %v", err)
	}
	if li != 0 {
		t.Fatalf("Expected latest index to be 0, got %d", li)
	}
	if tm != 0 {
		t.Fatalf("Expected latest term to be 0, got %d", tm)
	}

	if store.Len() != 0 {
		t.Errorf("Expected store to have 0 snapshots, got %d", store.Len())
	}
}

func Test_StoreCreateCancel(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}

	sink, err := store.Create(1, 2, 3, makeTestConfiguration("1", "localhost:1"), 1, nil)
	if err != nil {
		t.Fatalf("Failed to create sink: %v", err)
	}
	if sink.ID() == "" {
		t.Errorf("Expected sink ID to not be empty, got empty string")
	}

	tmpSnapDir := dir + "/" + sink.ID() + tmpSuffix

	// Should be a tmp directory with the name of the sink ID
	if !pathExists(tmpSnapDir) {
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
	if pathExists(tmpSnapDir) {
		t.Errorf("Expected directory with name %s to not exist, but it does", sink.ID())
	}

	if store.Len() != 0 {
		t.Errorf("Expected store to have 0 snapshots, got %d", store.Len())
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

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
