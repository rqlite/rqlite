package snapshot2

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/hashicorp/raft"
)

func Test_NewSinkCancel(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if sink.ID() != "snap-1234" {
		t.Fatalf("Unexpected ID: %s", sink.ID())
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Failed to cancel unopened sink: %v", err)
	}
}

func Test_NewSinkClose(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if sink.ID() != "snap-1234" {
		t.Fatalf("Unexpected ID: %s", sink.ID())
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to cancel unopened sink: %v", err)
	}
}

func Test_NewSinkOpenCancel(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Failed to cancel opened sink: %v", err)
	}
}

func Test_NewSinkOpenCloseFail(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}
	if err := sink.Close(); err == nil {
		t.Fatalf("Expected error closing opened sink without data")
	}
}

func Test_SinkFullSnapshot(t *testing.T) {
	store := mustStore(t)
	sink := NewSink(store, makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}

	sqliteFile := mustOpenFile(t, "testdata/db-and-wals/backup.db")
	defer sqliteFile.Close()
	n, err := io.Copy(sink, sqliteFile)
	if err != nil {
		t.Fatalf("Failed to copy SQLite file: %v", err)
	}
	if n != mustGetFileSize(t, "testdata/db-and-wals/backup.db") {
		t.Fatalf("Unexpected number of bytes copied: %d", n)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Check snapshot is available and correct.
	expMeta := makeRaftMeta("snap-1234", 3, 2, 1)
	metas, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(metas) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(metas))
	}
	compareMetas(t, expMeta, metas[0])

	// Check snapshot data is available and correct.
	meta, fd, err := store.Open("snap-1234")
	if err != nil {
		t.Fatalf("Failed to open snapshot: %v", err)
	}
	defer fd.Close()
	compareMetas(t, expMeta, meta)
	if !compareReaderToFile(t, fd, "testdata/db-and-wals/backup.db") {
		t.Fatalf("Snapshot data does not match")
	}

	// Write a second full snapshot, it should be installed without issue.
}

func compareMetas(t *testing.T, m1, m2 *raft.SnapshotMeta) {
	t.Helper()
	if m1.ID != m2.ID {
		t.Fatalf("Unexpected snapshot ID: %s", m1.ID)
	}
	if m1.Index != m2.Index {
		t.Fatalf("Unexpected snapshot index: %d", m1.Index)
	}
	if m1.Term != m2.Term {
		t.Fatalf("Unexpected snapshot term: %d", m1.Term)
	}
	if m1.ConfigurationIndex != m2.ConfigurationIndex {
		t.Fatalf("Unexpected snapshot configuration index: %d", m1.ConfigurationIndex)
	}
	if m1.Version != m2.Version {
		t.Fatalf("Unexpected snapshot version: %d", m1.Version)
	}
}

func compareReaderToFile(t *testing.T, r io.Reader, path string) bool {
	t.Helper()
	fd := mustOpenFile(t, path)
	defer fd.Close()
	return compareReaderToReader(t, r, fd)
}

func compareReaderToReader(t *testing.T, r1, r2 io.Reader) bool {
	t.Helper()
	buf1, err := io.ReadAll(r1)
	if err != nil {
		t.Fatalf("Failed to read from reader 1: %v", err)
	}
	buf2, err := io.ReadAll(r2)
	if err != nil {
		t.Fatalf("Failed to read from reader 2: %v", err)
	}
	return bytes.Equal(buf1, buf2)
}

func mustStore(t *testing.T) *Store {
	t.Helper()
	str, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	return str
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

func mustGetFileSize(t *testing.T, path string) int64 {
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}
	return stat.Size()
}
