package snapshot2

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func parentDir(dir string) string {
	return filepath.Dir(dir)
}

func tmpName(path string) string {
	return path + tmpSuffix
}

func nonTmpName(path string) string {
	return strings.TrimSuffix(path, tmpSuffix)
}

func isTmpName(name string) bool {
	return filepath.Ext(name) == tmpSuffix
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func dirExists(path string) bool {
	stat, err := os.Stat(path)
	return err == nil && stat.IsDir()
}

func dirIsEmpty(dir string) (bool, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	return len(files) == 0, nil
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func syncDir(dir string) error {
	fh, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fh.Close()
	return fh.Sync()
}

func removeDirSync(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return syncDirParentMaybe(dir)
}

// syncDirParentMaybe syncs the parent directory of the given
// directory, but only on non-Windows platforms.
func syncDirParentMaybe(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	return syncDir(parentDir(dir))
}

// syncDirMaybe syncs the given directory, but only on non-Windows platforms.
func syncDirMaybe(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	return syncDir(dir)
}

// removeAllPrefix removes all files in the given directory that have the given prefix.
func removeAllPrefix(path, prefix string) error {
	files, err := filepath.Glob(filepath.Join(path, prefix) + "*")
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := os.RemoveAll(f); err != nil {
			return err
		}
	}
	return nil
}

func touchFile(path string) error {
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	return fh.Close()
}
