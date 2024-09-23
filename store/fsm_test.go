package store

import (
	"testing"

	"github.com/hashicorp/raft"
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
