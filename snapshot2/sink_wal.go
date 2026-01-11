package snapshot2

import (
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v9/snapshot2/proto"
)

type WALSink struct {
	dir      string
	manifest *proto.SnapshotWALFile
	file     *os.File
}

func NewWALSink(dir string, m *proto.SnapshotWALFile) *WALSink {
	return &WALSink{
		dir:      dir,
		manifest: m,
	}
}

func (s *WALSink) Open() error {
	f, err := os.Create(filepath.Join(s.dir, "wal"))
	if err != nil {
		return err
	}
	s.file = f
	return nil
}

func (s *WALSink) Write(p []byte) (n int, err error) {
	return s.file.Write(p)
}

// Close closes the sink.
func (s *WALSink) Close() error {
	return s.file.Close()
}
