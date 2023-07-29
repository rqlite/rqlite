package snapshot

import (
	"os"
	"path/filepath"
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

func Test_WALSnapshotStore_CreateFull(t *testing.T) {
	dir := makeTempDir()
	defer os.RemoveAll(dir)
	s := NewWALSnapshotStore(dir)

	sink, err := s.Create(1, 2, 3, makeConfiguration(), 4, nil)
	if err != nil {
		t.Fatalf("failed to create full snapshot: %s", err)
	}

	fSink, ok := sink.(*WALFullSnapshotSink)
	if !ok {
		t.Fatalf("returned sink is not a WALFullSnapshotSink")
	}
	id := fSink.ID()

	testBytes := []byte("test")
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
}

func makeTempDir() string {
	dir, err := os.MkdirTemp("", "wal-snapshot-store-test")
	if err != nil {
		panic(err)
	}
	return dir
}

// functtion to create a Raft configuration suitable for testing
func makeConfiguration() raft.Configuration {
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
