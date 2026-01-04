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

func Test_FSMSnapshot_OnRelease_NoInvoke(t *testing.T) {
	onReleaseCalled := false
	onReleaseCalledInvoked := false
	onReleaseCalledSucceeded := false
	f := FSMSnapshot{
		OnRelease: func(invoked, succeeded bool) {
			onReleaseCalled = true
			onReleaseCalledInvoked = invoked
			onReleaseCalledSucceeded = succeeded
		},
		FSMSnapshot: &mockRaftSnapshot{},
		logger:      nil,
	}

	f.Release()
	if !onReleaseCalled {
		t.Fatalf("OnRelease was not called")
	}
	if onReleaseCalledInvoked {
		t.Fatalf("invoked is true even though Persist was not called")
	}
	if onReleaseCalledSucceeded {
		t.Fatalf("succeeded is true even though Persist was not called")
	}
}

func Test_FSMSnapshot_OnFailure_Called(t *testing.T) {
	onReleaseCalled := false
	f := FSMSnapshot{
		OnRelease: func(invoked, succeeded bool) {
			onReleaseCalled = true
		},
		FSMSnapshot: &mockRaftSnapshot{},
		logger:      nil,
	}
		if err := f.Persist(&mockSink{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	f.Release()
	if !onReleaseCalled {
		t.Fatalf("OnRelease was not called")
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
