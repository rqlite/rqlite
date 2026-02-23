package snapshot

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/rsum"
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
		logger: log.New(log.Writer(), "[staging-dir] ", log.LstdFlags),
	}
}

// Path returns the underlying directory path.
func (s *StagingDir) Path() string {
	return s.dir
}

// CreateWAL creates a new timestamped .wal file in the staging directory
// and returns an io.WriteCloser that computes a CRC32 checksum on the fly.
// The returned path is the absolute path to the new WAL file. On Close the
// writer writes the .crc32 sidecar, syncs the WAL file, and syncs the
// directory.
func (s *StagingDir) CreateWAL() (io.WriteCloser, string, error) {
	walPath := filepath.Join(s.dir, fmt.Sprintf("%024d.wal", time.Now().UnixNano()))
	fd, err := os.Create(walPath)
	if err != nil {
		return nil, "", err
	}
	crcW := rsum.NewCRC32Writer(fd)
	w := &walFileWriter{
		fd:      fd,
		crcW:    crcW,
		walPath: walPath,
		dir:     s.dir,
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
		s.logger.Printf("found %d WAL files in staging directory, processing multi-WAL snapshot",
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
// directory into dst.
func (s *StagingDir) MoveWALFilesTo(dst string) error {
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

// walFileWriter is an io.WriteCloser that wraps a WAL file descriptor,
// computing a running CRC32 checksum. On Close it writes the .crc32
// sidecar file, syncs the WAL file, and syncs the containing directory.
type walFileWriter struct {
	fd      *os.File
	crcW    *rsum.CRC32Writer
	walPath string
	dir     string
}

func (w *walFileWriter) Write(p []byte) (int, error) {
	return w.crcW.Write(p)
}

func (w *walFileWriter) Close() error {
	if err := rsum.WriteCRC32SumFile(w.walPath+crcSuffix, w.crcW.Sum32(), rsum.Sync); err != nil {
		return fmt.Errorf("failed to write CRC32 sum file: %w", err)
	}
	if err := w.fd.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}
	if err := w.fd.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}
	return syncDirMaybe(w.dir)
}
