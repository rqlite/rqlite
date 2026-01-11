package snapshot2

import "github.com/rqlite/rqlite/v9/snapshot2/proto"

type InstallSink struct {
	dir      string
	manifest *proto.SnapshotManifest
}

func NewWInstallSink(dir string, m *proto.SnapshotManifest) *InstallSink {
	return &InstallSink{
		dir:      dir,
		manifest: m,
	}
}

func (s *InstallSink) Write(p []byte) (n int, err error) {
	return 0, nil
}

// Close closes the sink.
func (s *InstallSink) Close() error {
	return nil
}
