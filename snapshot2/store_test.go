package snapshot2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	createSnapshotInStore(t, store, "2-1131-1704807720976", 1131, 2, 1, "", "testdata/db-and-wals/wal-00")

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

// Test_Store_EndToEndCycle tests an end-to-end cycle of creating a Store,
// creating sinks, and writing various types of snapshots to other Stores.
func Test_Store_EndToEndCycle(t *testing.T) {
	store0, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create source store: %v", err)
	}

	store1, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create source store: %v", err)
	}

	id1 := "2-100-1704807719996"
	id2 := "2-200-1704807800000"

	createSnapshotInStore(t, store0, id1, 100, 2, 1, "testdata/db-and-wals/backup.db")
	if exp, got := 1, store0.Len(); exp != got {
		t.Errorf("Expected store to have %d snapshots, got %d", exp, got)
	}
	createSnapshotInStore(t, store0, id2, 200, 2, 1, "", "testdata/db-and-wals/wal-00")
	if exp, got := 2, store0.Len(); exp != got {
		t.Errorf("Expected store to have %d snapshots, got %d", exp, got)
	}

	snaps, err := store0.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 2 {
		t.Errorf("Expected 2 snapshots, got %d", len(snaps))
	}
	if exp, got := id1, snaps[0].ID; exp != got {
		t.Errorf("Expected snapshot ID to be %s, got %s", exp, got)
	}
	if exp, got := id2, snaps[1].ID; exp != got {
		t.Errorf("Expected snapshot ID to be %s, got %s", exp, got)
	}

	//////////////////////////////////////////////////////////////////////////////////
	// Open the first snapshot, and write it to the second store.
	//////////////////////////////////////////////////////////////////////////////////
	meta, rc, err := store0.Open(id1)
	if err != nil {
		t.Fatalf("Failed to open snapshot: %v", err)
	}
	if meta.ID != id1 && meta.Index != 100 && meta.Term != 2 {
		t.Errorf("Snapshot metadata does not match expected values")
	}

	dstSink, err := store1.Create(1, 1000, 2000, makeTestConfiguration("1", "localhost:1"), 1, nil)
	if err != nil {
		t.Fatalf("Failed to create sink in destination store: %v", err)
	}
	if _, err := io.Copy(dstSink, rc); err != nil {
		t.Fatalf("Failed to copy snapshot data to destination store: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Failed to close snapshot reader: %v", err)
	}
	if err := dstSink.Close(); err != nil {
		t.Fatalf("Failed to close sink in destination store: %v", err)
	}
	// Double check the second store.
	if exp, got := 1, store1.Len(); exp != got {
		t.Errorf("Expected store to have %d snapshots, got %d", exp, got)
	}

	// Open the snapshot in the second store, check its contents.
	snaps, err = store1.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots in destination store: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("Expected 1 snapshot in destination store, got %d", len(snaps))
	}
	meta, rc, err = store1.Open(snaps[0].ID)
	if err != nil {
		t.Fatalf("Failed to open snapshot in destination store: %v", err)
	}

	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, rc); err != nil {
		t.Fatalf("Failed to read snapshot data from destination store: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Failed to close snapshot reader in destination store: %v", err)
	}

	dbPath, walPaths := persistStreamerData(t, buf)
	if len(walPaths) != 0 {
		t.Fatalf("Expected 0 WAL files, got %d", len(walPaths))
	}
	if !filesIdentical(dbPath, "testdata/db-and-wals/backup.db") {
		t.Fatalf("Database file in snapshot does not match source")
	}

	//////////////////////////////////////////////////////////////////////////////////
	// Open the second snapshot, and write it to the second store.
	//////////////////////////////////////////////////////////////////////////////////
	meta, rc, err = store0.Open(id2)
	if err != nil {
		t.Fatalf("Failed to open snapshot: %v", err)
	}
	if meta.ID != id2 && meta.Index != 200 && meta.Term != 2 {
		t.Fatalf("Snapshot metadata does not match expected values")
	}

	dstSink, err = store1.Create(1, 2000, 3000, makeTestConfiguration("1", "localhost:1"), 1, nil)
	if err != nil {
		t.Fatalf("Failed to create sink in destination store: %v", err)
	}
	if _, err := io.Copy(dstSink, rc); err != nil {
		t.Fatalf("Failed to copy snapshot data to destination store: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Failed to close snapshot reader: %v", err)
	}
	if err := dstSink.Close(); err != nil {
		t.Fatalf("Failed to close sink in destination store: %v", err)
	}
	// Double check the second store.
	if exp, got := 2, store1.Len(); exp != got {
		t.Fatalf("Expected store to have %d snapshots, got %d", exp, got)
	}

	// Open the second snapshot in the second store, check its contents. When writing
	// snapshot with both a database file and one (or more) WAL files from one store to
	// another, the Sink writing to the second store will checkpoint the WAL files into
	// the database file. Therefore, when we read back the snapshot from the second store,
	// we expect to see only a database file, with no associated WAL files.
	snaps, err = store1.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots in destination store: %v", err)
	}
	if len(snaps) != 2 {
		t.Errorf("Expected 1 snapshot in destination store, got %d", len(snaps))
	}
	meta, rc, err = store1.Open(snaps[1].ID)
	if err != nil {
		t.Fatalf("Failed to open snapshot in destination store: %v", err)
	}

	buf = &bytes.Buffer{}
	if _, err := io.Copy(buf, rc); err != nil {
		t.Fatalf("Failed to read snapshot data from destination store: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Failed to close snapshot reader in destination store: %v", err)
	}

	dbPath, walPaths = persistStreamerData(t, buf)
	if len(walPaths) != 0 {
		t.Fatalf("Expected 0 WAL files, got %d", len(walPaths))
	}

	// Check the file, it should have the content of the backup plus the changes
	// from the checkpointed WAL file.
	checkDB, err := db.Open(dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database at %s: %s", dbPath, err)
	}
	defer checkDB.Close()
	rows, err := checkDB.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query exp: %s got: %s", exp, got)
	}

	//////////////////////////////////////////////////////////////////////////////////
	// Write a third snapshot to the first store, another incremental based on the second.
	//////////////////////////////////////////////////////////////////////////////////
	id3 := "2-300-1704807900000"
	createSnapshotInStore(t, store0, id3, 100, 2, 1, "", "testdata/db-and-wals/wal-01")
	if exp, got := 3, store0.Len(); exp != got {
		t.Errorf("Expected store to have %d snapshots, got %d", exp, got)
	}

	// Double check the first store.
	if store0.Len() != 3 {
		t.Errorf("Expected store to have 3 snapshots, got %d", store0.Len())
	}

	// Open the third snapshot, write it to the second store.
	meta, rc, err = store0.Open(id3)
	if err != nil {
		t.Fatalf("Failed to open snapshot: %v", err)
	}
	if meta.ID != id3 && meta.Index != 100 && meta.Term != 2 {
		t.Errorf("Snapshot metadata does not match expected values")
	}

	dstSink, err = store1.Create(1, 3000, 4000, makeTestConfiguration("1", "localhost:1"), 1, nil)
	if err != nil {
		t.Fatalf("Failed to create sink in destination store: %v", err)
	}
	if _, err := io.Copy(dstSink, rc); err != nil {
		t.Fatalf("Failed to copy snapshot data to destination store: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Failed to close snapshot reader: %v", err)
	}
	if err := dstSink.Close(); err != nil {
		t.Fatalf("Failed to close sink in destination store: %v", err)
	}
	// Double check the second store.
	if exp, got := 3, store1.Len(); exp != got {
		t.Fatalf("Expected store to have %d snapshots, got %d", exp, got)
	}

	// Open the third snapshot in the second store, check its contents.
	snaps, err = store1.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots in destination store: %v", err)
	}
	if len(snaps) != 3 {
		t.Errorf("Expected 3 snapshots in destination store, got %d", len(snaps))
	}
	meta, rc, err = store1.Open(snaps[2].ID)
	if err != nil {
		t.Fatalf("Failed to open snapshot in destination store: %v", err)
	}

	buf = &bytes.Buffer{}
	if _, err := io.Copy(buf, rc); err != nil {
		t.Fatalf("Failed to read snapshot data from destination store: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Failed to close snapshot reader in destination store: %v", err)
	}

	dbPath, walPaths = persistStreamerData(t, buf)
	if len(walPaths) != 0 {
		t.Fatalf("Expected 0 WAL files, got %d", len(walPaths))
	}

	// Check the file, it should have the content of the backup plus the changes
	// from the two checkpointed WAL files.
	checkDB, err = db.Open(dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database at %s: %s", dbPath, err)
	}
	defer checkDB.Close()
	rows, err = checkDB.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query exp: %s got: %s", exp, got)
	}

	//////////////////////////////////////////////////////////////////////////////////
	// Write a fourth snapshot to the first store, this time full with both DB and WALs.
	//////////////////////////////////////////////////////////////////////////////////
	id4 := "2-400-1704808000000"
	createSnapshotInStore(t, store0, id4, 400, 2, 1,
		"testdata/db-and-wals/backup.db",
		"testdata/db-and-wals/wal-00",
		"testdata/db-and-wals/wal-01",
		"testdata/db-and-wals/wal-02")

	// Double check the first store.
	if store0.Len() != 4 {
		t.Fatalf("Expected store to have 4 snapshots, got %d", store0.Len())
	}

	// Open the fourth snapshot, write it to the second store.
	meta, rc, err = store0.Open(id4)
	if err != nil {
		t.Fatalf("Failed to open snapshot: %v", err)
	}
	if meta.ID != id4 && meta.Index != 400 && meta.Term != 2 {
		t.Fatalf("Snapshot metadata does not match expected values")
	}

	dstSink, err = store1.Create(1, 5000, 6000, makeTestConfiguration("1", "localhost:1"), 1, nil)
	if err != nil {
		t.Fatalf("Failed to create sink in destination store: %v", err)
	}
	if _, err := io.Copy(dstSink, rc); err != nil {
		t.Fatalf("Failed to copy snapshot data to destination store: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Failed to close snapshot reader: %v", err)
	}
	if err := dstSink.Close(); err != nil {
		t.Fatalf("Failed to close sink in destination store: %v", err)
	}
	if exp, got := 4, store1.Len(); exp != got {
		t.Fatalf("Expected store to have %d snapshots, got %d", exp, got)
	}

	// Open the fourth snapshot in the second store, check its contents. Because
	// the snapshot contains both a DB file and WAL files, the Sink in the second
	// store will checkpoint the WAL files into the DB file.
	snaps, err = store1.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots in destination store: %v", err)
	}
	if exp, got := 4, len(snaps); exp != got {
		t.Fatalf("Expected %d snapshots in destination store, got %d", exp, got)
	}
	meta, rc, err = store1.Open(snaps[3].ID)
	if err != nil {
		t.Fatalf("Failed to open snapshot in destination store: %v", err)
	}

	buf = &bytes.Buffer{}
	if _, err := io.Copy(buf, rc); err != nil {
		t.Fatalf("Failed to read snapshot data from destination store: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Failed to close snapshot reader in destination store: %v", err)
	}

	dbPath, walPaths = persistStreamerData(t, buf)
	if len(walPaths) != 0 {
		t.Fatalf("Expected 0 WAL files, got %d", len(walPaths))
	}

	// Check the file, it should have the content of the backup plus the changes
	// from the three checkpointed WAL files.
	checkDB, err = db.Open(dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database at %s: %s", dbPath, err)
	}
	defer checkDB.Close()
	rows, err = checkDB.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[3]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query exp: %s got: %s", exp, got)
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

func createSnapshotInStore(t *testing.T, store *Store, id string, index, term, cfgIndex uint64, dbFile string, walFiles ...string) {
	t.Helper()

	sink := NewSink(store.Dir(), makeRaftMeta(id, index, term, cfgIndex), store)
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}

	// Make the streamer.
	streamer, err := proto.NewSnapshotStreamer(dbFile, walFiles...)
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

func persistStreamerData(t *testing.T, buf *bytes.Buffer) (string, []string) {
	t.Helper()

	tmpDir := t.TempDir()
	dbPath := ""
	walPaths := []string{}

	// Read header first.
	hdr := &proto.SnapshotHeader{}
	hdrSizeBuf := make([]byte, 4)
	if _, err := buf.Read(hdrSizeBuf); err != nil {
		t.Fatalf("Failed to read header size: %v", err)
	}
	hdrSize := binary.BigEndian.Uint32(hdrSizeBuf)
	hdrBuf := make([]byte, hdrSize)
	if _, err := buf.Read(hdrBuf); err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}
	hdr, err := proto.UnmarshalSnapshotHeader(hdrBuf)
	if err != nil {
		t.Fatalf("Failed to unmarshal header: %v", err)
	}

	// Read DB file if present.
	if hdr.DbHeader != nil {
		dbPath = tmpDir + "/db-file"
		dbFile, err := os.Create(dbPath)
		if err != nil {
			t.Fatalf("Failed to create DB file: %v", err)
		}
		defer dbFile.Close()

		if _, err := io.CopyN(dbFile, buf, int64(hdr.DbHeader.SizeBytes)); err != nil {
			t.Fatalf("Failed to copy DB file data: %v", err)
		}
	}

	// Read WAL files.
	for i, walHdr := range hdr.WalHeaders {
		walPath := filepath.Join(tmpDir, fmt.Sprintf("wal-file-%d", i))
		walFile, err := os.Create(walPath)
		if err != nil {
			t.Fatalf("Failed to create WAL file: %v", err)
		}
		defer walFile.Close()

		if _, err := io.CopyN(walFile, buf, int64(walHdr.SizeBytes)); err != nil {
			t.Fatalf("Failed to copy WAL file data: %v", err)
		}
		walPaths = append(walPaths, walPath)
	}

	return dbPath, walPaths
}
