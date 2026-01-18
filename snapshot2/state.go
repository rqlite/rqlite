package snapshot2

import (
	"fmt"
	"path/filepath"
	"sort"
)

// ResolveSnapshots resolves a snapshot ID into its constituent DB file and WAL files. At a minimum,
// a DB file is returned. If the snapshot is incremental, associated WAL files are also returned. The
// order in the slice is the order in which the WAL files should be applied to the DB file.
func ResolveSnapshots(rootDir string, snapshotID string) (dbfile string, walFiles []string, err error) {
	snapshots, err := getSnapshots(rootDir)
	if err != nil {
		return "", nil, err
	}

	// Walk from newest to oldest.
	snapshotMetas := snapMetaSlice(snapshots)
	sort.Sort(sort.Reverse(snapshotMetas))

	found := false
	for _, snapMeta := range snapshots {
		if snapMeta.ID != snapshotID && !found {
			continue
		}
		found = true

		if snapMeta.Type == SnapshotMetaTypeFull {
			// Full DB file so, we're done.
			return filepath.Join(rootDir, snapMeta.ID, dbfileName), walFiles, nil
		}
		// Must be incremental. We need to prepend the WAL file name to the list and
		// keep walking backwards
		walFile := filepath.Join(rootDir, snapMeta.ID, walfileName)
		walFiles = append([]string{walFile}, walFiles...)
		continue
	}

	if found {
		return "", nil, fmt.Errorf("no full snapshot found in chain for snapshot ID %s", snapshotID)
	}
	return "", nil, ErrSnapshotNotFound
}
