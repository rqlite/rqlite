package snapshot

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
)

func Test_NewWALSnapshotStore(t *testing.T) {
	dir := makeTempDir()
	defer os.RemoveAll(dir)
	s := NewWALSnapshotStore(dir)
	if s.Path() != dir {
		t.Fatalf("unexpected dir, exp=%s got=%s", dir, s.Path())
	}
}

func Test_WALSnapshotStore_CreateFullThenIncremental(t *testing.T) {
	dir := makeTempDir()
	defer os.RemoveAll(dir)
	str := NewWALSnapshotStore(dir)

	sink, err := str.Create(1, 2, 3, makeTestConfiguration(), 4, nil)
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
	if !compareFileToByteSlice(filepath.Join(dir, sqliteFilePath), testBytes) {
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
	if !reflect.DeepEqual(snaps[0].Configuration, makeTestConfiguration()) {
		t.Fatalf("unexpected Configuration, exp=%s got=%s", makeTestConfiguration(), snaps[0].Configuration)
	}

	sink, err = str.Create(1, 5, 6, makeTestConfiguration(), 4, nil)
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
	if !fileExists(filepath.Join(dir, id, walFilePath)) {
		t.Fatalf("snapshot wal file does not exist")
	}
	if !compareFileToByteSlice(filepath.Join(dir, id, walFilePath), testBytes) {
		t.Fatalf("snapshot wal file does not match")
	}

	snaps, err = str.List()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshot, got %d", len(snaps))
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
	if !reflect.DeepEqual(snaps[0].Configuration, makeTestConfiguration()) {
		t.Fatalf("unexpected Configuration, exp=%s got=%s", makeTestConfiguration(), snaps[0].Configuration)
	}
}

func makeTempDir() string {
	dir, err := os.MkdirTemp("", "wal-snapshot-store-test")
	if err != nil {
		panic(err)
	}
	return dir
}

// functtion to create a Raft configuration suitable for testing
func makeTestConfiguration() raft.Configuration {
	return raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      "4002",
				Address: "localhost:4002",
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
	contents, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return string(contents) == string(b)
}
