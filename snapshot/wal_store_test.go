package snapshot

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/command/encoding"
	"github.com/rqlite/rqlite/db"
)

func Test_NewWALSnapshotStore(t *testing.T) {
	dir := makeTempDir()
	defer os.RemoveAll(dir)
	s, err := NewWALSnapshotStore(dir)
	if err != nil {
		t.Fatalf("failed to create snapshot store: %s", err)
	}
	if s.Path() != dir {
		t.Fatalf("unexpected dir, exp=%s got=%s", dir, s.Path())
	}
}

func Test_NewWALSnapshotStore_ListEmpty(t *testing.T) {
	dir := makeTempDir()
	defer os.RemoveAll(dir)
	s, err := NewWALSnapshotStore(dir)
	if err != nil {
		t.Fatalf("failed to create snapshot store: %s", err)
	}
	if s.Path() != dir {
		t.Fatalf("unexpected dir, exp=%s got=%s", dir, s.Path())
	}

	snaps, err := s.List()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots, got %d", len(snaps))
	}
}

// Test_WALSnapshotStore_CreateFull performs detailed testing of the
// snapshot creation process.
func Test_WALSnapshotStore_CreateFullThenIncremental(t *testing.T) {
	dir := makeTempDir()
	defer os.RemoveAll(dir)
	str, err := NewWALSnapshotStore(dir)
	if err != nil {
		t.Fatalf("failed to create snapshot store: %s", err)
	}

	testConfig1 := makeTestConfiguration("1", "2")
	sink, err := str.Create(1, 2, 3, testConfig1, 4, nil)
	if err != nil {
		t.Fatalf("failed to create 1st snapshot: %s", err)
	}

	fSink, ok := sink.(*WALFullSnapshotSink)
	if !ok {
		t.Fatalf("returned sink is not a WALFullSnapshotSink")
	}
	id := fSink.ID()

	testBytes := []byte("test-db")
	n, err := fSink.Write(testBytes)
	if err != nil {
		t.Fatalf("failed to write to sink: %s", err)
	}
	if n != len(testBytes) {
		t.Fatalf("failed to write all bytes to sink")
	}
	if err := fSink.Close(); err != nil {
		t.Fatalf("failed to close sink: %s", err)
	}

	if !dirExists(filepath.Join(dir, id)) {
		t.Fatalf("snapshot directory does not exist")
	}
	if !fileExists(filepath.Join(dir, id, metaFileName)) {
		t.Fatalf("snapshot meta file does not exist")
	}
	if !compareFileToByteSlice(filepath.Join(dir, baseSqliteFile), testBytes) {
		t.Fatalf("snapshot SQLite file does not match")
	}

	snaps, err := str.List()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].ID != id {
		t.Fatalf("unexpected ID, exp=%s got=%s", id, snaps[0].ID)
	}
	if snaps[0].Index != 2 {
		t.Fatalf("unexpected FirstIndex, exp=1 got=%d", snaps[0].Index)
	}
	if snaps[0].Term != 3 {
		t.Fatalf("unexpected Term, exp=3 got=%d", snaps[0].Term)
	}
	if !reflect.DeepEqual(snaps[0].Configuration, testConfig1) {
		t.Fatalf("unexpected Configuration, exp=%s got=%s", testConfig1, snaps[0].Configuration)
	}

	testConfig2 := makeTestConfiguration("3", "4")
	sink, err = str.Create(1, 5, 6, testConfig2, 4, nil)
	if err != nil {
		t.Fatalf("failed to create 2nd snapshot: %s", err)
	}

	iSink, ok := sink.(*WALIncrementalSnapshotSink)
	if !ok {
		t.Fatalf("returned sink is not a WALIncrementalSnapshotSink")
	}
	id = iSink.ID()

	testBytes = []byte("test-wal")
	n, err = iSink.Write(testBytes)
	if err != nil {
		t.Fatalf("failed to write to sink: %s", err)
	}
	if n != len(testBytes) {
		t.Fatalf("failed to write all bytes to sink")
	}
	if err := iSink.Close(); err != nil {
		t.Fatalf("failed to close sink: %s", err)
	}

	if !dirExists(filepath.Join(dir, id)) {
		t.Fatalf("snapshot directory does not exist")
	}
	if !fileExists(filepath.Join(dir, id, metaFileName)) {
		t.Fatalf("snapshot meta file does not exist")
	}
	if !fileExists(filepath.Join(dir, id, snapWALFile)) {
		t.Fatalf("snapshot wal file does not exist")
	}
	if !compareFileToByteSlice(filepath.Join(dir, id, snapWALFile), testBytes) {
		t.Fatalf("snapshot wal file does not match")
	}

	snaps, err = str.List()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].ID != id {
		t.Fatalf("unexpected ID, exp=%s got=%s", id, snaps[0].ID)
	}
	if snaps[0].Index != 5 {
		t.Fatalf("unexpected FirstIndex, exp=1 got=%d", snaps[0].Index)
	}
	if snaps[0].Term != 6 {
		t.Fatalf("unexpected Term, exp=3 got=%d", snaps[0].Term)
	}
	if !reflect.DeepEqual(snaps[0].Configuration, testConfig2) {
		t.Fatalf("unexpected Configuration, exp=%s got=%s", testConfig2, snaps[0].Configuration)
	}

	// Open incremental snapshot, which will trigger a bunch of checks.
	_, rc, err := str.Open(id)
	if err != nil {
		t.Fatalf("failed to open snapshot: %s", err)
	}
	defer rc.Close()
	if !compareReaderToByteSlice(rc, testBytes) {
		t.Fatalf("snapshot wal file does not match")
	}
}

// Test_WALSnapshotStore_Reaping tests that the snapshot store correctly
// reaps snapshots that are no longer needed. Because it's critical that
// reaping is done correctly, this test checks internal implementation
// details.
func Test_WALSnapshotStore_Reaping(t *testing.T) {
	dir := makeTempDir()
	defer os.RemoveAll(dir)
	str, err := NewWALSnapshotStore(dir)
	if err != nil {
		t.Fatalf("failed to create snapshot store: %s", err)
	}

	testConfig := makeTestConfiguration("1", "2")

	createSnapshot := func(index, term uint64, file string) {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("failed to read file: %s", err)
		}
		sink, err := str.Create(1, index, term, testConfig, 4, nil)
		if err != nil {
			t.Fatalf("failed to create 2nd snapshot: %s", err)
		}
		if _, err = sink.Write(b); err != nil {
			t.Fatalf("failed to write to sink: %s", err)
		}
		sink.Close()
	}

	createSnapshot(1, 1, "testdata/reaping/backup.db")
	createSnapshot(3, 2, "testdata/reaping/wal-00")
	createSnapshot(5, 3, "testdata/reaping/wal-01")
	createSnapshot(7, 4, "testdata/reaping/wal-02")
	createSnapshot(9, 5, "testdata/reaping/wal-03")

	// There should be 5 snapshot directories, one of which should be
	// a full, and the rest incremental.
	snaps, err := str.getSnapshots()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if exp, got := 5, len(snaps); exp != got {
		t.Fatalf("expected %d snapshots, got %d", exp, got)
	}
	for _, snap := range snaps[0:4] {
		if snap.Full {
			t.Fatalf("snapshot %s is full", snap.ID)
		}
	}
	if !snaps[4].Full {
		t.Fatalf("snapshot %s is incremental", snaps[4].ID)
	}

	// Reap just the first snapshot, which is full.
	n, err := str.ReapSnapshots(4)
	if err != nil {
		t.Fatalf("failed to reap full snapshot: %s", err)
	}
	if exp, got := 1, n; exp != got {
		t.Fatalf("expected %d snapshots to be reaped, got %d", exp, got)
	}
	snaps, err = str.getSnapshots()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if exp, got := 4, len(snaps); exp != got {
		t.Fatalf("expected %d snapshots, got %d", exp, got)
	}

	// Reap all but the last two snapshots. The remaining snapshots
	// should all be incremental.
	n, err = str.ReapSnapshots(2)
	if err != nil {
		t.Fatalf("failed to reap snapshots: %s", err)
	}
	if exp, got := 2, n; exp != got {
		t.Fatalf("expected %d snapshots to be reaped, got %d", exp, got)
	}
	snaps, err = str.getSnapshots()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if exp, got := 2, len(snaps); exp != got {
		t.Fatalf("expected %d snapshots, got %d", exp, got)
	}
	for _, snap := range snaps {
		if snap.Full {
			t.Fatalf("snapshot %s is full", snap.ID)
		}
	}
	if snaps[0].Index != 9 && snaps[1].Term != 5 {
		t.Fatalf("snap 0 is wrong")
	}
	if snaps[1].Index != 7 && snaps[1].Term != 3 {
		t.Fatalf("snap 1 is wrong")
	}

	// Check the contents of the remaining snapshots by creating a new
	// SQLite from the Store
	dbPath, err := str.ReplayWALs()
	if err != nil {
		t.Fatalf("failed to replay WALs: %s", err)
	}
	db, err := db.Open(dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()
	rows, err := db.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[4]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_WALSnapshotStore_ReapingLimitsFail(t *testing.T) {
	dir := makeTempDir()
	defer os.RemoveAll(dir)
	str, err := NewWALSnapshotStore(dir)
	if err != nil {
		t.Fatalf("failed to create snapshot store: %s", err)
	}

	if _, err := str.ReapSnapshots(1); err != ErrRetainCountTooLow {
		t.Fatalf("expected ErrRetainCountTooLow, got %s", err)
	}

	n, err := str.ReapSnapshots(10)
	if err != nil {
		t.Fatalf("expected nil error, got %s", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 snapshots to be reaped, got %d", n)
	}
}

func makeTempDir() string {
	dir, err := os.MkdirTemp("", "wal-snapshot-store-test")
	if err != nil {
		panic(err)
	}
	return dir
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

func dirExists(path string) bool {
	stat, err := os.Stat(path)
	if err != nil {
		return false
	}
	return stat.IsDir()
}

func compareFileToByteSlice(path string, b []byte) bool {
	fd, err := os.Open(path)
	if err != nil {
		return false
	}
	defer fd.Close()
	return compareReaderToByteSlice(fd, b)
}

func compareReaderToByteSlice(r io.Reader, b []byte) bool {
	contents, err := io.ReadAll(r)
	if err != nil {
		return false
	}
	return string(contents) == string(b)
}

func asJSON(v interface{}) string {
	enc := encoding.Encoder{}
	b, err := enc.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}
