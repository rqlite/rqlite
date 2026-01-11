package snapshot2

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v9/db"
	"github.com/rqlite/rqlite/v9/internal/rsum"
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
	defer s.file.Close()

	sz, err := fileSize(s.file.Name())
	if err != nil {
		return err
	}
	if sz != int64(s.manifest.SizeBytes) {
		return fmt.Errorf("file size mismatch: got %d, want %d", sz, s.manifest.SizeBytes)
	}

	if !db.IsValidSQLiteWALFile(s.file.Name()) {
		return fmt.Errorf("file is not a valid SQLite WAL file")
	}

	if s.manifest.Crc32 != 0 {
		crc32, err := rsum.CRC32(s.file.Name())
		if err != nil {
			return err
		}
		if crc32 != s.manifest.Crc32 {
			return fmt.Errorf("file checksum mismatch: got %d, want %d", crc32, s.manifest.Crc32)
		}
	}
	return nil
}
