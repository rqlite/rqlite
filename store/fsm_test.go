package store

import (
	"errors"
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

	if err := f.Persist(&mockSnapshotSink{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !finalizerCalled {
		t.Fatalf("finalizer was not called")
	}
}

func Test_FSMSnapshot_OnRelease_OK(t *testing.T) {
	onReleaseCalled := false
	invoked := false
	succeeded := false

	f := FSMSnapshot{
		OnRelease: func(i, s bool) {
			onReleaseCalled = true
			invoked = i
			succeeded = s
		},
		FSMSnapshot: &mockRaftSnapshot{},
		logger:      nil,
	}

	if err := f.Persist(&mockSnapshotSink{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	f.Release()
	if !onReleaseCalled {
		t.Fatalf("OnRelease was not called")
	}
	if !invoked {
		t.Fatalf("OnRelease invoked argument incorrect")
	}
	if !succeeded {
		t.Fatalf("OnRelease succeeded argument incorrect")
	}
}

func Test_FSMSnapshot_OnRelease_NotInvoked(t *testing.T) {
	onReleaseCalled := false
	invoked := false

	f := FSMSnapshot{
		OnRelease: func(i, s bool) {
			onReleaseCalled = true
			invoked = i
		},
		FSMSnapshot: &mockRaftSnapshot{},
		logger:      nil,
	}

	f.Release()
	if !onReleaseCalled {
		t.Fatalf("OnRelease was not called")
	}
	if invoked {
		t.Fatalf("OnRelease invoked argument incorrect")
	}
}

func Test_FSMSnapshot_OnRelease_NotSucceeded(t *testing.T) {
	onReleaseCalled := false
	invoked := false
	succeeded := false

	f := FSMSnapshot{
		OnRelease: func(i, s bool) {
			onReleaseCalled = true
			invoked = i
			succeeded = s
		},
		FSMSnapshot: &mockRaftSnapshot{forceErr: true},
		logger:      nil,
	}

	f.Persist(&mockSnapshotSink{})

	f.Release()
	if !onReleaseCalled {
		t.Fatalf("OnRelease was not called")
	}
	if !invoked {
		t.Fatalf("OnRelease invoked argument incorrect")
	}
	if succeeded {
		t.Fatalf("OnRelease succeeded argument incorrect")
	}
}

type mockRaftSnapshot struct {
	forceErr bool
}

func (m *mockRaftSnapshot) Persist(sink raft.SnapshotSink) error {
	if m.forceErr {
		return errors.New("forced error")
	}
	return nil
}

func (m *mockRaftSnapshot) Release() {
}
