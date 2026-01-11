package snapshot2

import "path/filepath"

const (
	metaFileName = "meta.json"
	tmpSuffix    = ".tmp"
)

// metaPath returns the path to the meta file in the given directory.
func metaPath(dir string) string {
	return filepath.Join(dir, metaFileName)
}
