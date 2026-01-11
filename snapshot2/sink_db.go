package snapshot2

import "github.com/rqlite/rqlite/v9/snapshot2/proto"

type DBSink struct {
	dir      string
	manifest *proto.SnapshotManifest
}

func NewDBSink(dir string, m *proto.SnapshotManifest) *DBSink {
	return &DBSink{
		dir:      dir,
		manifest: m,
	}
}

func (s *DBSink) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (s *DBSink) Close() error {
	return nil
}
