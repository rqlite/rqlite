package snapshot

import (
	"bytes"
	"io"
	"os"
	"testing"
)

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

func Test_LatestIndexTerm(t *testing.T) {
	store := mustStore(t)
	li, tm, err := LatestIndexTerm(store.dir)
	if err != nil {
		t.Fatalf("Failed to get latest index: %v", err)
	}
	if li != 0 {
		t.Fatalf("Expected latest index to be 0, got %d", li)
	}
	if tm != 0 {
		t.Fatalf("Expected latest term to be 0, got %d", tm)
	}

	expLi := uint64(3)
	expTm := uint64(2)
	sink := NewSink(store, makeRaftMeta("snap-1234", expLi, expTm, 1))
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
	sqliteFile.Close() // Reaping will fail on Windows if file is not closed.
	if n != mustGetFileSize(t, "testdata/db-and-wals/backup.db") {
		t.Fatalf("Unexpected number of bytes copied: %d", n)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	li, tm, err = LatestIndexTerm(store.dir)
	if err != nil {
		t.Fatalf("Failed to get latest index: %v", err)
	}
	if li != expLi {
		t.Fatalf("Expected latest index to be %d, got %d", expLi, li)
	}
	if tm != expTm {
		t.Fatalf("Expected latest term to be %d, got %d", expTm, tm)
	}
}

func Test_StateReaderNew(t *testing.T) {
	// Create a new StateReader
	s := NewStateReader(nil)
	if s == nil {
		t.Errorf("expected snapshot to be created")
	}
}

// Test_StateReaderPersist_NilData tests that Persist does not error when
// given a nil data buffer.
func Test_StateReaderPersist_NilData(t *testing.T) {
	compactedBuf := bytes.NewBuffer(nil)
	s := NewStateReader(io.NopCloser(compactedBuf))
	if s == nil {
		t.Errorf("expected snapshot to be created")
	}

	mrs := &mockRaftSink{}
	err := s.Persist(mrs)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(mrs.buf.Bytes()) != 0 {
		t.Errorf("expected %d, got %d", 0, len(mrs.buf.Bytes()))
	}
}

func Test_StateReaderPersist_SimpleData(t *testing.T) {
	compactedBuf := bytes.NewBuffer([]byte("hello world"))
	s := NewStateReader(io.NopCloser(compactedBuf))
	if s == nil {
		t.Errorf("expected snapshot to be created")
	}

	mrs := &mockRaftSink{}
	err := s.Persist(mrs)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if mrs.buf.String() != "hello world" {
		t.Errorf("expected %s, got %s", "hello world", mrs.buf.String())
	}
}

type mockRaftSink struct {
	buf bytes.Buffer
}

func (mrs *mockRaftSink) Write(p []byte) (n int, err error) {
	return mrs.buf.Write(p)
}

func (mrs *mockRaftSink) Close() error {
	return nil
}

// implement cancel
func (mrs *mockRaftSink) Cancel() error {
	return nil
}

func (mrs *mockRaftSink) ID() string {
	return ""
}
