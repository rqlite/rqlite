package snapshot

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v9/db"
)

// IncrementalFileSink is a sink for writing locally-generated WAL snapshot data
// to a Snapshot store. This is a fast path for when it's the local node that is
// generating the incremental snapshot. This sink simply moves the WAL file to the
// snapshot directory.
type IncrementalFileSink struct {
	dir     string
	srcPath string
	dstPath string
}

// NewIncrementalFileSink creates a new IncrementalFileSink object.
func NewIncrementalFileSink(dir string, path string) *IncrementalFileSink {
	return &IncrementalFileSink{
		dir:     dir,
		srcPath: path,
		dstPath: filepath.Join(dir, walfileName),
	}
}

// Open opens the sink.
func (s *IncrementalFileSink) Open() error {
	return nil
}

// Write writes data to the sink. The Write method exists to satisfy the
// io.Writer interface but it should not be called. To detect incorrect usage,
// it always returns an error.
func (s *IncrementalFileSink) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("Write method should not be called on IncrementalFileSink")
}

// Close closes the sink. On Close, the source WAL file is moved to the snapshot
// directory and basic validity checks are performed.
func (s *IncrementalFileSink) Close() error {
	if err := os.Rename(s.srcPath, s.dstPath); err != nil {
		return err
	}
	if !db.IsValidSQLiteWALFile(s.dstPath) {
		return fmt.Errorf("file is not a valid SQLite WAL file")
	}
	return nil
}

// WALFile returns the path to the installed WAL file.
func (s *IncrementalFileSink) WALFile() string {
	return s.dstPath
}
