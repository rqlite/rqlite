package snapshot

import (
	"io"
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
