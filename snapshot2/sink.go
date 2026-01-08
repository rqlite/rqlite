package snapshot2

import "github.com/hashicorp/raft"

// Sink is a sink for writing snapshot data to a Snapshot store.
type Sink struct {
	meta *raft.SnapshotMeta
}

// NewSink creates a new Sink object.
func NewSink(meta *raft.SnapshotMeta) *Sink {
	return &Sink{
		meta: meta,
	}
}

// ID returns the ID of the snapshot.
func (s *Sink) ID() string {
	return s.meta.ID
}

// Write writes snapshot data to the sink.
func (s *Sink) Write(p []byte) (n int, err error) {
	return 0, nil
}

// Close closes the sink.
func (s *Sink) Close() error {
	return nil
}

// Cancel cancels the sink.
func (s *Sink) Cancel() error {
	return nil
}
