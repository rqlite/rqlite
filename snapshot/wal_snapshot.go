package snapshot

import (
	"io"
)

// WALSnapshotState represents the state of a WAL-based snapshot, suitable for
// restoring a node from.
type WALSnapshotState struct {
	rc    io.ReadCloser
	store *WALSnapshotStore
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
// longer needed. It is critical for the user of the Snapshot to call Close
// when finished with it to ensure that the associated Snapshot store
// can make changes to the Store.
func (s *WALSnapshotState) Close() error {
	s.store.mu.RUnlock()
	if err := s.rc.Close(); err != nil {
		return err
	}
	return nil
}
