package snapshot

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/hashicorp/raft"
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

// LatestIndexTerm returns the index and term of the latest snapshot in the given directory.
func LatestIndexTerm(dir string) (uint64, uint64, error) {
	meta, err := getSnapshots(dir)
	if err != nil {
		return 0, 0, err
	}
	if len(meta) == 0 {
		return 0, 0, nil
	}
	return meta[len(meta)-1].Index, meta[len(meta)-1].Term, nil
}

func getSnapshots(dir string) ([]*raft.SnapshotMeta, error) {
	// Get the eligible snapshots
	snapshots, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// Populate the metadata
	var snapMeta []*raft.SnapshotMeta
	for _, snap := range snapshots {
		// Ignore any files
		if !snap.IsDir() {
			continue
		}

		// Ignore any temporary snapshots
		dirName := snap.Name()
		if isTmpName(dirName) {
			continue
		}

		// Try to read the meta data
		meta, err := readMeta(filepath.Join(dir, dirName))
		if err != nil {
			return nil, fmt.Errorf("failed to read meta for snapshot %s: %s", dirName, err)
		}

		// Append, but only return up to the retain count
		snapMeta = append(snapMeta, meta)
	}

	sort.Sort(snapMetaSlice(snapMeta))
	return snapMeta, nil
}
