package snapshot2

import (
	"io"
	"os"
	"sort"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v9/db"
	"github.com/rqlite/rqlite/v9/snapshot2/proto"
)

func Test_SnapshotMetaSort(t *testing.T) {
	metas := []*SnapshotMeta{
		{
			SnapshotMeta: &raft.SnapshotMeta{
				ID:    "2-1017-1704807719996",
				Index: 1017,
				Term:  2,
			},
			Type: SnapshotMetaTypeFull,
		},
		{
			SnapshotMeta: &raft.SnapshotMeta{
				ID:    "2-1131-1704807720976",
				Index: 1131,
				Term:  2,
			},
			Type: SnapshotMetaTypeIncremental,
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

func Test_Store_CreateList(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}

	snaps, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 0 {
		t.Errorf("Expected 0 snapshots, got %d", len(snaps))
	}

	createSnapshot := func(id string, index, term, cfgIndex uint64, file string) {
		sink := NewSink(store.Dir(), makeRaftMeta(id, index, term, cfgIndex))
		if sink == nil {
			t.Fatalf("Failed to create new sink")
		}
		if err := sink.Open(); err != nil {
			t.Fatalf("Failed to open sink: %v", err)
		}

		var w io.WriterTo
		if db.IsValidSQLiteFile(file) {
			manifest, err := proto.NewSnapshotManifestFromDB(file)
			if err != nil {
				t.Fatalf("Failed to create SnapshotDBFile from file: %v", err)
			}
			w = manifest
		} else {
			manifest, err := proto.NewSnapshotManifestFromWAL(file)
			if err != nil {
				t.Fatalf("Failed to create SnapshotDBFile from file: %v", err)
			}
			w = manifest
		}

		n, err := w.WriteTo(sink)
		if err != nil {
			t.Fatalf("Failed to write SnapshotManifest to sink: %v", err)
		}
		if n == 0 {
			t.Fatalf("Expected to write some bytes to sink, wrote 0")
		}

		fd := mustOpenFile(t, file)
		defer fd.Close()
		_, err = io.Copy(sink, fd)
		if err != nil {
			t.Fatalf("Failed to copy file to sink: %v", err)
		}

		if err := sink.Close(); err != nil {
			t.Fatalf("Failed to close sink: %v", err)
		}
	}

	createSnapshot("2-1017-1704807719996", 1017, 2, 1, "testdata/db-and-wals/backup.db")
	createSnapshot("2-1131-1704807720976", 1131, 2, 1, "testdata/db-and-wals/wal-00")

	if store.Len() != 2 {
		t.Errorf("Expected store to have 2 snapshots, got %d", store.Len())
	}

	li, tm, err := store.LatestIndexTerm()
	if err != nil {
		t.Fatalf("Failed to get latest index and term from empty store: %v", err)
	}
	if li != 1131 {
		t.Fatalf("Expected latest index to be 1131, got %d", li)
	}
	if tm != 2 {
		t.Fatalf("Expected latest term to be 2, got %d", tm)
	}

	snaps, err = store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 2 {
		t.Errorf("Expected 2 snapshots, got %d", len(snaps))
	}
	if snaps[0].ID != "2-1017-1704807719996" {
		t.Errorf("Expected snapshot ID to be 2-1017-1704807719996, got %s", snaps[0].ID)
	}
	if snaps[1].ID != "2-1131-1704807720976" {
		t.Errorf("Expected snapshot ID to be 2-1131-1704807720976, got %s", snaps[1].ID)
	}
}

// Test_Store_SingleInstall tests that installing a single snapshot works as expected.
// This tests the scenario where a snapshot is instaled into the store from another
// node, or rebuilding on restart.
//
// The snapshot is created on the fly, not by using Store.Open().
func Test_Store_SingleInstall(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}

	sink, err := store.Create(1, 2, 3, makeTestConfiguration("1", "localhost:1"), 1, nil)
	if err != nil {
		t.Fatalf("Failed to create sink: %v", err)
	}
	defer sink.Cancel()

	manifest, err := proto.NewSnapshotManifestWithInstall(
		"testdata/db-and-wals/full2.db",
		"testdata/db-and-wals/wal-00",
	)
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}

	if _, err := manifest.WriteTo(sink); err != nil {
		t.Fatalf("unexpected error writing manifest to sink: %s", err.Error())
	}

	for _, filePath := range []string{"testdata/db-and-wals/full2.db", "testdata/db-and-wals/wal-00"} {
		fd, err := os.Open(filePath)
		if err != nil {
			t.Fatalf("unexpected error opening source file %s: %s", filePath, err.Error())
		}
		if _, err := io.Copy(sink, fd); err != nil {
			t.Fatalf("unexpected error copying data to sink: %s", err.Error())
		}
		fd.Close()
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
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

func makeRaftMeta(id string, index, term, cfgIndex uint64) *raft.SnapshotMeta {
	return &raft.SnapshotMeta{
		ID:                 id,
		Index:              index,
		Term:               term,
		Configuration:      makeTestConfiguration("1", "localhost:1"),
		ConfigurationIndex: cfgIndex,
		Version:            1,
	}
}

func mustOpenFile(t *testing.T, path string) *os.File {
	t.Helper()
	fd, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	return fd
}
