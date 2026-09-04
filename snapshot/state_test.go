package snapshot

import (
	"bytes"
	"io"
	"testing"
	"time"
)

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

func Test_Snapshot_Clone(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}
	defer store.Close()

	if err := Clone(dir, "non-existent", 2000, 100); err == nil {
		t.Fatal("expected error cloning non-existent snapshot")
	}

	// Create a single snapshot, List should return it.
	createSnapshotInStore(t, store, "2-1017-1704807719996", 1017, 2, 1, "testdata/db-and-wals/backup.db")
	snaps, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].ID != "2-1017-1704807719996" {
		t.Fatalf("Expected snapshot ID to be 2-1017-1704807719996, got %s", snaps[0].ID)
	}

	// Clone the snapshot, make sure it is installed.
	if err := Clone(dir, snaps[0].ID, 2000, 100); err != nil {
		t.Fatalf("failed to clone snapshot: %s", err)
	}
	if store.Len() != 2 {
		t.Fatalf("Expected store to have 2 snapshots, got %d", store.Len())
	}
	snaps, err = store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].Index != 2000 {
		t.Fatalf("Expected cloned snapshot index to be 2000, got %d", snaps[0].Index)
	}
	if snaps[0].Term != 100 {
		t.Fatalf("Expected cloned snapshot term to be 100, got %d", snaps[0].Term)
	}
}

func Test_SinkIndexTerm(t *testing.T) {
	sink := NewSink(t.TempDir(), makeRaftMeta("test-index-term", 100, 3, 1), nil, nil)
	index, term, err := SinkIndexTerm(sink)
	if err != nil {
		t.Fatalf("unexpected error getting index and term of sink: %s", err.Error())
	}
	if exp, got := uint64(100), index; exp != got {
		t.Fatalf("wrong index, exp %d, got %d", exp, got)
	}
	if exp, got := uint64(3), term; exp != got {
		t.Fatalf("wrong term, exp %d, got %d", exp, got)
	}
}

func Test_SinkIndexTerm_NotIndexTermer(t *testing.T) {
	if _, _, err := SinkIndexTerm(&mockRaftSink{}); err == nil {
		t.Fatalf("expected error from sink which does not know its index and term")
	}
}

func Test_StreamerIndexTerm(t *testing.T) {
	streamer := NewLockingStreamer(nil, nil, makeRaftMeta("test-index-term", 100, 3, 1), time.Second)
	index, term, err := StreamerIndexTerm(streamer)
	if err != nil {
		t.Fatalf("unexpected error getting index and term of sink: %s", err.Error())
	}
	if exp, got := uint64(100), index; exp != got {
		t.Fatalf("wrong index, exp %d, got %d", exp, got)
	}
	if exp, got := uint64(3), term; exp != got {
		t.Fatalf("wrong term, exp %d, got %d", exp, got)
	}
}

func Test_WrappedStreamerIndexTerm(t *testing.T) {
	streamer := wrappedReadCloser{NewLockingStreamer(nil, nil, makeRaftMeta("test-index-term", 100, 3, 1), time.Second)}
	index, term, err := StreamerIndexTerm(&streamer)
	if err != nil {
		t.Fatalf("unexpected error getting index and term of sink: %s", err.Error())
	}
	if exp, got := uint64(100), index; exp != got {
		t.Fatalf("wrong index, exp %d, got %d", exp, got)
	}
	if exp, got := uint64(3), term; exp != got {
		t.Fatalf("wrong term, exp %d, got %d", exp, got)
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

type wrappedReadCloser struct {
	rc io.ReadCloser
}

func (w *wrappedReadCloser) WrappedReadCloser() io.ReadCloser {
	return w.rc
}

func (w *wrappedReadCloser) Read(p []byte) (n int, err error) {
	return 0, nil
}

func (w *wrappedReadCloser) Close() error {
	return nil
}
