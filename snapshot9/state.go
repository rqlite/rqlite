package snapshot9

import (
	"os"
	"path/filepath"
)

// RemoveAllTmpSnapshotData removes all temporary Snapshot data from the directory.
// This process is defined as follows: for every directory in dir, if the directory
// is a temporary directory, remove the directory. Then remove all other files
// that contain the name of a temporary directory, minus the temporary suffix,
// as prefix.
func RemoveAllTmpSnapshotData(dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	for _, d := range files {
		// If the directory is a temporary directory, remove it.
		if d.IsDir() && isTmpName(d.Name()) {
			files, err := filepath.Glob(filepath.Join(dir, nonTmpName(d.Name())) + "*")
			if err != nil {
				return err
			}

			fullTmpDirPath := filepath.Join(dir, d.Name())
			for _, f := range files {
				if f == fullTmpDirPath {
					// Delete the directory last as a sign the deletion is complete.
					continue
				}
				if err := os.Remove(f); err != nil {
					return err
				}
			}
			if err := os.RemoveAll(fullTmpDirPath); err != nil {
				return err
			}
		}
	}
	return nil
}
