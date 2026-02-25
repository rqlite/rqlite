package snapshot

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/rsum"
)

const (
	crcSuffix = ".crc32"
)

var (
	stagingSeq atomic.Uint64
)

// StagingDir represents a WAL staging directory containing timestamped
// .wal files paired with .crc32 checksum sidecar files. It encapsulates
// the naming conventions, validation, and file-management operations
// shared by the store (producer) and snapshot sink (consumer).
type StagingDir struct {
	dir    string
	logger *log.Logger
}

// NewStagingDir wraps an existing directory path as a StagingDir.
// No I/O is performed; the caller is responsible for creating or
// removing the directory via Path().
func NewStagingDir(dir string) *StagingDir {
	return &StagingDir{
		dir:    dir,
		logger: log.New(log.Writer(), "[snapshot-staging] ", log.LstdFlags),
	}
}

// Path returns the underlying directory path.
func (s *StagingDir) Path() string {
	return s.dir
}

// CreateWAL creates a new timestamped .wal file in the staging directory
// and returns a WALWriter that computes a CRC32 checksum on the fly.
// The returned path is the absolute path to the new WAL file.
//
// On success the caller must call Close, which writes the .crc32 sidecar,
// syncs the WAL file, and syncs the directory.
//
// If writing fails, the caller must call Cancel to remove the partial WAL
// file. Cancel is a no-op after a successful Close. A typical pattern is:
//
//	w, path, err := sd.CreateWAL()
//	if err != nil { ... }
//	defer w.Cancel()
//	// ... write data ...
//	if err := w.Close(); err != nil { ... }
func (s *StagingDir) CreateWAL() (*WALWriter, string, error) {
	walPath := filepath.Join(s.dir, fmt.Sprintf("%024d-%06d.wal",
		time.Now().UnixNano(), stagingSeq.Add(1)))
	fd, err := os.Create(walPath)
	if err != nil {
		return nil, "", err
	}
	crcW := rsum.NewCRC32Writer(fd)
	w := &WALWriter{
		fd:   fd,
		crcW: crcW,
		dir:  s.dir,
	}
	return w, walPath, nil
}

// WALFiles returns the sorted list of .wal files in the staging directory.
func (s *StagingDir) WALFiles() ([]string, error) {
	files, err := filepath.Glob(filepath.Join(s.dir, "*"+walfileSuffix))
	if err != nil {
		return nil, fmt.Errorf("failed to list staged WAL files: %w", err)
	}
	return files, nil
}

// Validate checks every .wal file in the staging directory:
//   - It must be a valid SQLite WAL file.
//   - A matching .crc32 sidecar must exist and its recorded checksum
//     must match the actual file contents.
func (s *StagingDir) Validate() error {
	walFiles, err := s.WALFiles()
	if err != nil {
		return err
	}
	if len(walFiles) > 1 {
		s.logger.Printf("found %d WAL files in staging directory, validating multi-WAL snapshot",
			len(walFiles))
	}
	for _, walPath := range walFiles {
		if !db.IsValidSQLiteWALFile(walPath) {
			return fmt.Errorf("%s is not a valid SQLite WAL file", walPath)
		}
		crcPath := walPath + crcSuffix
		ok, err := rsum.CompareCRC32SumFile(walPath, crcPath)
		if err != nil {
			return fmt.Errorf("comparing CRC32 sum for %s: %w", walPath, err)
		}
		if !ok {
			return fmt.Errorf("CRC32 sum mismatch for %s", walPath)
		}
	}
	return nil
}

// MoveWALFilesTo renames each .wal + .crc32 pair from the staging
// directory into dst. dst must be a directory and must exist.
func (s *StagingDir) MoveWALFilesTo(dst string) error {
	if !dirExists(dst) {
		return fmt.Errorf("destination %s does not exist or is not a directory", dst)
	}
	walFiles, err := s.WALFiles()
	if err != nil {
		return err
	}
	for _, srcPath := range walFiles {
		srcCRCPath := srcPath + crcSuffix
		name := filepath.Base(srcPath)
		dstPath := filepath.Join(dst, name)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
		if err := os.Rename(srcCRCPath, dstPath+crcSuffix); err != nil {
			return err
		}
	}
	return nil
}

// Sync syncs the staging directory file descriptor.
func (s *StagingDir) Sync() error {
	return syncDirMaybe(s.dir)
}

// WALWriter wraps a WAL file descriptor, computing a running CRC32
// checksum over all data written through it. On Close it writes the
// .crc32 sidecar file, syncs the WAL file, and syncs the containing
// directory. If writing fails the caller should call Cancel to remove
// the partial WAL file. Cancel is a no-op after a successful Close.
type WALWriter struct {
	fd     *os.File
	crcW   *rsum.CRC32Writer
	dir    string
	closed bool
}

// Write writes data to the WAL file and updates the running CRC32 checksum.
func (w *WALWriter) Write(p []byte) (int, error) {
	return w.crcW.Write(p)
}

// Close finalizes the WAL file: writes the .crc32 sidecar, syncs the
// WAL file, closes the file descriptor, and syncs the directory.
func (w *WALWriter) Close() error {
	walPath := w.fd.Name()
	if err := rsum.WriteCRC32SumFile(walPath+crcSuffix, w.crcW.Sum32(), rsum.Sync); err != nil {
		return fmt.Errorf("failed to write CRC32 sum file: %w", err)
	}
	if err := w.fd.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}
	if err := w.fd.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}
	w.closed = true
	return syncDirMaybe(w.dir)
}

// Cancel removes the partial WAL file from the staging directory. It is
// a no-op if Close has already been called successfully.
func (w *WALWriter) Cancel() {
	if w.closed {
		return
	}
	w.fd.Close()
	walPath := w.fd.Name()
	os.Remove(walPath)
	os.Remove(walPath + crcSuffix)
}
