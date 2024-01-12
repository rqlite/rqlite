package snapshot

import (
	"io"
	"os"
	"sort"
	"testing"

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

func Test_RemoveAllTmpSnapshotData(t *testing.T) {
	dir := t.TempDir()
	if err := RemoveAllTmpSnapshotData(dir); err != nil {
		t.Fatalf("Failed to remove all tmp snapshot data: %v", err)
	}
	if !pathExists(dir) {
		t.Fatalf("Expected dir to exist, but it does not")
	}
	directories, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}
	if len(directories) != 0 {
		t.Fatalf("Expected dir to be empty, got %d files", len(directories))
	}

	mustTouchDir(t, dir+"/dir")
	mustTouchFile(t, dir+"/file")
	if err := RemoveAllTmpSnapshotData(dir); err != nil {
		t.Fatalf("Failed to remove all tmp snapshot data: %v", err)
	}
	if !pathExists(dir + "/dir") {
		t.Fatalf("Expected dir to exist, but it does not")
	}
	if !pathExists(dir + "/file") {
		t.Fatalf("Expected file to exist, but it does not")
	}

	mustTouchDir(t, dir+"/snapshot1234.tmp")
	mustTouchFile(t, dir+"/snapshot1234.db")
	mustTouchFile(t, dir+"/snapshot1234.db-wal")
	mustTouchFile(t, dir+"/snapshot1234-5678")
	if err := RemoveAllTmpSnapshotData(dir); err != nil {
		t.Fatalf("Failed to remove all tmp snapshot data: %v", err)
	}
	if !pathExists(dir + "/dir") {
		t.Fatalf("Expected dir to exist, but it does not")
	}
	if !pathExists(dir + "/file") {
		t.Fatalf("Expected file to exist, but it does not")
	}
	if pathExists(dir + "/snapshot1234.tmp") {
		t.Fatalf("Expected snapshot1234.tmp to not exist, but it does")
	}
	if pathExists(dir + "/snapshot1234.db") {
		t.Fatalf("Expected snapshot1234.db to not exist, but it does")
	}
	if pathExists(dir + "/snapshot1234.db-wal") {
		t.Fatalf("Expected snapshot1234.db-wal to not exist, but it does")
	}
	if pathExists(dir + "/snapshot1234-5678") {
		t.Fatalf("Expected /snapshot1234-5678 to not exist, but it does")
	}

	mustTouchFile(t, dir+"/snapshotABCD.tmp")
	if err := RemoveAllTmpSnapshotData(dir); err != nil {
		t.Fatalf("Failed to remove all tmp snapshot data: %v", err)
	}
	if !pathExists(dir + "/snapshotABCD.tmp") {
		t.Fatalf("Expected /snapshotABCD.tmp to exist, but it does not")
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

	_, _, err = store.Open("non-existent")
	if err == nil {
		t.Fatalf("Expected error opening non-existent snapshot, got nil")
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
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}
	store.reapDisabled = true

	snaps, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 0 {
		t.Errorf("Expected 0 snapshots, got %d", len(snaps))
	}

	createSnapshot := func(id string, index, term, cfgIndex uint64, file string) {
		sink := NewSink(store, makeRaftMeta(id, index, term, cfgIndex))
		if sink == nil {
			t.Fatalf("Failed to create new sink")
		}
		if err := sink.Open(); err != nil {
			t.Fatalf("Failed to open sink: %v", err)
		}
		wal := mustOpenFile(t, file)
		defer wal.Close()
		_, err := io.Copy(sink, wal)
		if err != nil {
			t.Fatalf("Failed to copy WAL file: %v", err)
		}
		if err := sink.Close(); err != nil {
			t.Fatalf("Failed to close sink: %v", err)
		}

		if fn, err := store.FullNeeded(); err != nil {
			t.Fatalf("Failed to check if full snapshot needed: %v", err)
		} else if fn {
			t.Errorf("Expected full snapshot not to be needed, but it is")
		}
	}

	createSnapshot("2-1017-1704807719996", 1017, 2, 1, "testdata/db-and-wals/backup.db")
	createSnapshot("2-1131-1704807720976", 1131, 2, 1, "testdata/db-and-wals/wal-00")
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
}

func mustTouchFile(t *testing.T, path string) {
	t.Helper()
	fd, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if err := fd.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}
}

func mustTouchDir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
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
