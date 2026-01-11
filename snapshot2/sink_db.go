package snapshot2

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v9/internal/rsum"
	"github.com/rqlite/rqlite/v9/snapshot2/proto"
)

type DBSink struct {
	dir      string
	manifest *proto.SnapshotDBFile
	file     *os.File
}

func NewDBSink(dir string, m *proto.SnapshotDBFile) *DBSink {
	return &DBSink{
		dir:      dir,
		manifest: m,
	}
}

func (s *DBSink) Open() error {
	f, err := os.Create(filepath.Join(s.dir, "db.sqlite3"))
	if err != nil {
		return err
	}
	s.file = f
	return nil
}

func (s *DBSink) Write(p []byte) (n int, err error) {
	return s.file.Write(p)
}

func (s *DBSink) Close() error {
	crc32, err := rsum.CRC32(s.file.Name())
	if err != nil {
		return err
	}
	if crc32 != s.manifest.Crc32 {
		return fmt.Errorf("db file checksum mismatch: got %d, want %d", crc32, s.manifest.Crc32)
	}
	return s.file.Close()
}
