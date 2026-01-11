package snapshot2

import "github.com/rqlite/rqlite/v9/snapshot2/proto"

// InstallSink is a sink for writing snapshot data to a Snapshot store
// where that data is being written as part of a restore operation
// or sent over the network to another node for installation.
type InstallSink struct {
	dir      string
	manifest *proto.SnapshotInstall
}

// NewInstallSink creates a new InstallSink object.
func NewInstallSink(dir string, m *proto.SnapshotInstall) *InstallSink {
	return &InstallSink{
		dir:      dir,
		manifest: m,
	}
}

// Open opens the sink for writing.
func (s *InstallSink) Open() error {
	return nil
}

// Write writes data to the sink.
func (s *InstallSink) Write(p []byte) (n int, err error) {
	return 0, nil
}

// Close closes the sink.
func (s *InstallSink) Close() error {
	return nil
}
