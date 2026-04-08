package snapshot

import "path/filepath"

func tmpName(path string) string {
	return path + tmpSuffix
}

func isTmpName(name string) bool {
	return filepath.Ext(name) == tmpSuffix
}
