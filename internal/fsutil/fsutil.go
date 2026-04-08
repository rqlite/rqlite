package fsutil

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// RemoveFile removes the file at the given path if it exists.
func RemoveFile(path string) error {
	if !PathExists(path) {
		return nil
	}
	return os.Remove(path)
}

// PathExists returns true if the given path exists.
func PathExists(p string) bool {
	if _, err := os.Lstat(p); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

// PathExistsWithData returns true if the given path exists and has data.
func PathExistsWithData(p string) bool {
	stat, err := os.Stat(p)
	if err != nil {
		return false
	}
	return stat.Size() > 0
}

// DirExists returns true if an actual directory exists at the given path.
func DirExists(path string) bool {
	stat, err := os.Stat(path)
	return err == nil && stat.IsDir()
}

// EnsureDirExists creates the directory at the given path if it does not exist.
func EnsureDirExists(path string) error {
	if DirExists(path) {
		return nil
	}
	return os.MkdirAll(path, 0755)
}

// FileSize returns the size of the file at the given path.
func FileSize(path string) (int64, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// FileSizeExists returns the size of the given file, or 0 if the file does not
// exist. Any other error is returned.
func FileSizeExists(path string) (int64, error) {
	if !PathExists(path) {
		return 0, nil
	}
	return FileSize(path)
}

// DirSize returns the total size of all files in the given directory.
func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			// If the file doesn't exist, we can ignore it. Snapshot files might
			// disappear during walking.
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// ModTimeSize returns the modification time and size of the file at the given path.
func ModTimeSize(path string) (time.Time, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, 0, err
	}
	return info.ModTime(), info.Size(), nil
}

// ParentDir returns the parent directory of the given directory.
func ParentDir(dir string) string {
	return filepath.Dir(dir)
}

// FileExists returns true if a file exists at path and it is not a directory.
func FileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// DirIsEmpty returns true if the given directory is empty.
func DirIsEmpty(dir string) (bool, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	return len(files) == 0, nil
}

// SyncDir syncs the given directory to stable storage.
func SyncDir(dir string) error {
	fh, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fh.Close()
	return fh.Sync()
}

// RemoveDirSync removes the directory and syncs the parent directory.
func RemoveDirSync(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return SyncDirParentMaybe(dir)
}

// SyncDirParentMaybe syncs the parent directory of the given
// directory, but only on non-Windows platforms.
//
// A note on the SyncDir* functions. This is the same approach
// that Hashicorp Raft uses in its implementation. Since the
// os.Rename() is atomic, the lack of directory-level sync
// means the rename may be rolled back after a power loss on
// Windows. This is OK. The main thing is the rename will
// have either happened or it will not have.
func SyncDirParentMaybe(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	return SyncDir(ParentDir(dir))
}

// SyncDirMaybe syncs the given directory, but only on non-Windows platforms.
func SyncDirMaybe(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	return SyncDir(dir)
}

// FilesIdentical returns true if the two files at the given paths have identical contents.
func FilesIdentical(path1, path2 string) bool {
	b1, err := os.ReadFile(path1)
	if err != nil {
		return false
	}
	b2, err := os.ReadFile(path2)
	if err != nil {
		return false
	}
	return bytes.Equal(b1, b2)
}
