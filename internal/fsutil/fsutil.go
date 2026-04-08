package fsutil

import (
	"io/fs"
	"os"
	"path/filepath"
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
