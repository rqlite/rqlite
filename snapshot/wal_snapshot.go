package snapshot

import (
	"io"
)

// WALSnapshotState represents the state of a WAL-based snapshot, suitable for
// restoring a node from.
type WALSnapshotState struct {
	rc    io.ReadCloser
	store *WALSnapshotStore

	closed bool
}

// NewWALSnapshotState returns a new WALSnapshotState
func NewWALSnapshotState(rc io.ReadCloser, store *WALSnapshotStore) *WALSnapshotState {
	state := &WALSnapshotState{
		rc:    rc,
		store: store,
	}
	state.store.mu.RLock()
	return state
}

// Read reads the contents of the Snapshot state
func (s *WALSnapshotState) Read(p []byte) (int, error) {
	return s.rc.Read(p)
}

// Close closes the Snapshot. It should be called when the Snapshot is no
// longer needed. It is CRITICAL for the user of the Snapshot to call Close
// when finished with it to ensure that the associated Snapshot store
// can make changes to the Store. If the Snapshot is not closed, the
// associated Store will be locked indefinitely.
func (s *WALSnapshotState) Close() error {
	// It's not entirely clear from the Raft docs if it will close
	// the snapshot after it's been read, so make closes idempotent.
	// If we don't unlocking the an unlocked mutex, will cause a panic.
	if s.closed {
		return nil
	}

	if err := s.rc.Close(); err != nil {
		return err
	}
	s.store.mu.RUnlock()
	s.closed = true
	return nil
}
