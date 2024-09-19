package snapshot9

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/hashicorp/raft"
)

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

// RemoveAllTmpSnapshotData removes all temporary Snapshot directories from the given
// directory.
func RemoveAllTmpSnapshotData(dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, d := range files {
		if d.IsDir() && isTmpName(d.Name()) {
			if err := os.RemoveAll(filepath.Join(dir, d.Name())); err != nil {
				return err
			}
		}
	}
	return nil
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
		snapMeta = append(snapMeta, meta)
	}

	sort.Sort(snapMetaSlice(snapMeta))
	return snapMeta, nil
}

type cmpSnapshotMeta raft.SnapshotMeta

func (c *cmpSnapshotMeta) Less(other *cmpSnapshotMeta) bool {
	if c.Term != other.Term {
		return c.Term < other.Term
	}
	if c.Index != other.Index {
		return c.Index < other.Index
	}
	return c.ID < other.ID
}

type snapMetaSlice []*raft.SnapshotMeta

// Implement the sort interface for []*fileSnapshotMeta.
func (s snapMetaSlice) Len() int {
	return len(s)
}

func (s snapMetaSlice) Less(i, j int) bool {
	si := (*cmpSnapshotMeta)(s[i])
	sj := (*cmpSnapshotMeta)(s[j])
	return si.Less(sj)
}

func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
