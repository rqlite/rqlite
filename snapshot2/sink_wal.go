package snapshot2

import "github.com/rqlite/rqlite/v9/snapshot2/proto"

type WALSink struct {
	dir      string
	manifest *proto.SnapshotManifest
}

func NewWALSink(dir string, m *proto.SnapshotManifest) *WALSink {
	return &WALSink{
		dir:      dir,
		manifest: m,
	}
}

func (s *WALSink) Write(p []byte) (n int, err error) {
	return 0, nil
}

// Close closes the sink.
func (s *WALSink) Close() error {
	return nil
}
