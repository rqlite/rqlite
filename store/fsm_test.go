package store

import (
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/random"
)

func Test_FSMSnapshot_Finalizer(t *testing.T) {
	finalizerCalled := false
	f := FSMSnapshot{
		Finalizer: func() error {
			finalizerCalled = true
			return nil
		},
		FSMSnapshot: &mockRaftSnapshot{},
		logger:      nil,
	}

	if err := f.Persist(&mockSink{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !finalizerCalled {
		t.Fatalf("finalizer was not called")
	}
}

func Test_FSMSnapshot_OnFailure_NotCalled(t *testing.T) {
	onFailureCalled := false
	f := FSMSnapshot{
		OnFailure: func() {
			onFailureCalled = true
		},
		FSMSnapshot: &mockRaftSnapshot{},
		logger:      nil,
	}

	if err := f.Persist(&mockSink{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	f.Release()
	if onFailureCalled {
		t.Fatalf("OnFailure was called")
	}
}

func Test_FSMSnapshot_OnFailure_Called(t *testing.T) {
	onFailureCalled := false
	f := FSMSnapshot{
		OnFailure: func() {
			onFailureCalled = true
		},
		FSMSnapshot: &mockRaftSnapshot{},
		logger:      nil,
	}
	f.Release()
	if !onFailureCalled {
		t.Fatalf("OnFailure was not called")
	}
}

func Test_NewStringReadCloser(t *testing.T) {
	rc := NewStringReadCloser("foo")
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(b) != "foo" {
		t.Fatalf("unexpected data: %s", b)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func Test_NewStringReadCloser_ByteByByte(t *testing.T) {
	testString := "lorum ipsum dolor sit amet consectetur adipiscing elit"
	rc := NewStringReadCloser(testString)

	var b []byte
	for {
		buf := make([]byte, random.Intn(10)+1)
		n, err := rc.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("unexpected error: %v", err)
		}
		if err == io.EOF {
			break
		}
		b = append(b, buf[:n]...)
	}

	if string(b) != testString {
		t.Fatalf("unexpected data: %s", b)
	}
}

func Test_NewStringReadCloser_Empty(t *testing.T) {
	rc := NewStringReadCloser("")
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(b) != 0 {
		t.Fatalf("unexpected data: %s", b)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

type mockSink struct {
}

func (m *mockSink) ID() string {
	return ""
}

func (m *mockSink) Cancel() error {
	return nil
}

func (m *mockSink) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (m *mockSink) Close() error {
	return nil
}

type mockRaftSnapshot struct {
}

func (m *mockRaftSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (m *mockRaftSnapshot) Release() {
}
