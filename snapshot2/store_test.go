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

// Test_Store_CreateIncrementalFirst_Fail tests that creating an incremental snapshot
// in an empty store fails as expected. All Stores mut start with a full snapshot.
func Test_Store_CreateIncrementalFirst_Fail(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}
	store.SetFullNeeded()

	sink := NewSink(store.Dir(), makeRaftMeta("1234", 45, 1, 40), store)
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}
	defer sink.Cancel()

	// Make the streamer.
	streamer, err := proto.NewSnapshotStreamer("", "testdata/db-and-wals/wal-00")
	if err != nil {
		t.Fatalf("Failed to create SnapshotStreamer: %v", err)
	}
	if err := streamer.Open(); err != nil {
		t.Fatalf("Failed to open SnapshotStreamer: %v", err)
	}
	defer func() {
		if err := streamer.Close(); err != nil {
			t.Fatalf("Failed to close SnapshotStreamer: %v", err)
		}
	}()

	// Copy from streamer into sink.
	_, err = io.Copy(sink, streamer)
	if err == nil {
		t.Fatalf("Expected error when writing incremental snapshot sink in empty store, got nil")
	}
}

func Test_Store_CreateThenList(t *testing.T) {
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

	createSnapshotInStore(t, store, "2-1017-1704807719996", 1017, 2, 1, "testdata/db-and-wals/backup.db")
	createSnapshotInStore(t, store, "2-1131-1704807720976", 1131, 2, 1, "testdata/db-and-wals/wal-00")

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

	streamer, err := proto.NewSnapshotStreamer(
		"testdata/db-and-wals/full2.db",
		"testdata/db-and-wals/wal-00",
	)
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}

	// Read from streamer into sink.
	_, err = io.Copy(sink, streamer)
	if err != nil {
		t.Fatalf("Failed to copy snapshot data to sink: %v", err)
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

func createSnapshotInStore(t *testing.T, store *Store, id string, index, term, cfgIndex uint64, file string) {
	t.Helper()

	sink := NewSink(store.Dir(), makeRaftMeta(id, index, term, cfgIndex), store)
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}

	dbFile := ""
	walFile := []string{}
	if db.IsValidSQLiteFile(file) {
		dbFile = file
	} else {
		walFile = append(walFile, file)
	}

	// Make the streamer.
	streamer, err := proto.NewSnapshotStreamer(dbFile, walFile...)
	if err != nil {
		t.Fatalf("Failed to create SnapshotStreamer: %v", err)
	}
	if err := streamer.Open(); err != nil {
		t.Fatalf("Failed to open SnapshotStreamer: %v", err)
	}
	defer func() {
		if err := streamer.Close(); err != nil {
			t.Fatalf("Failed to close SnapshotStreamer: %v", err)
		}
	}()

	// Copy from streamer into sink.
	_, err = io.Copy(sink, streamer)
	if err != nil {
		t.Fatalf("Failed to copy snapshot data to sink: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}
}
