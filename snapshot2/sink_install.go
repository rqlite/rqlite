package snapshot2

import "github.com/rqlite/rqlite/v9/snapshot2/proto"

type InstallSink struct {
	dir      string
	manifest *proto.SnapshotInstall
}

func NewInstallSink(dir string, m *proto.SnapshotInstall) *InstallSink {
	return &InstallSink{
		dir:      dir,
		manifest: m,
	}
}

func (s *InstallSink) Open() error {
	return nil
}

func (s *InstallSink) Write(p []byte) (n int, err error) {
	return 0, nil
}

// Close closes the sink.
func (s *InstallSink) Close() error {
	return nil
}
