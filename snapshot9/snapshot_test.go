package snapshot

import (
	"bytes"
	"io"
	"testing"
)

func Test_SnapshotNew(t *testing.T) {
	// Create a new snapshot
	s := NewSnapshot(nil)
	if s == nil {
		t.Errorf("expected snapshot to be created")
	}
}

// Test_SnapshotPersist_NilData tests that Persist does not error when
// given a nil data buffer.
func Test_SnapshotPersist_NilData(t *testing.T) {
	compactedBuf := bytes.NewBuffer(nil)
	s := NewSnapshot(io.NopCloser(compactedBuf))
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

func Test_SnapshotPersist_SimpleData(t *testing.T) {
	compactedBuf := bytes.NewBuffer([]byte("hello world"))
	s := NewSnapshot(io.NopCloser(compactedBuf))
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
