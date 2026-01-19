package snapshot2

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v9/db"
	"github.com/rqlite/rqlite/v9/internal/rsum"
	"github.com/rqlite/rqlite/v9/snapshot2/proto"
)

// WALSink is a sink for writing locally-generated WAL snapshot data to a Snapshot store.
type WALSink struct {
	dir    string
	header *proto.Header
	file   *os.File
}

// NewWALSink creates a new WALSink object.
func NewWALSink(dir string, m *proto.Header) *WALSink {
	return &WALSink{
		dir:    dir,
		header: m,
	}
}

// Open opens the sink for writing.
func (s *WALSink) Open() error {
	f, err := os.Create(filepath.Join(s.dir, walfileName))
	if err != nil {
		return err
	}
	s.file = f
	return nil
}

// Write writes data to the sink.
func (s *WALSink) Write(p []byte) (n int, err error) {
	return s.file.Write(p)
}

// Close closes the sink.
//
// On Close, the file size and basic validity checks are performed. If
// the header includes a CRC32 checksum, that is also verified.
func (s *WALSink) Close() error {
	defer s.file.Close()

	sz, err := fileSize(s.file.Name())
	if err != nil {
		return err
	}
	if sz != int64(s.header.SizeBytes) {
		return fmt.Errorf("file size mismatch: got %d, want %d", sz, s.header.SizeBytes)
	}

	if !db.IsValidSQLiteWALFile(s.file.Name()) {
		return fmt.Errorf("file is not a valid SQLite WAL file")
	}

	if s.header.Crc32 != 0 {
		crc32, err := rsum.CRC32(s.file.Name())
		if err != nil {
			return err
		}
		if crc32 != s.header.Crc32 {
			return fmt.Errorf("file checksum mismatch: got %d, want %d", crc32, s.header.Crc32)
		}
	}
	return nil
}
