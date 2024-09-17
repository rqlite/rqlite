package snapshot9

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/db"
)

// Upgrade writes a copy of the 8.x-format Snapshot directory at 'old' to a
// 9.x-format new Snapshot directory at 'new'. If the upgrade is successful,
// the 'old' directory is removed before the function returns.
func Upgrade8To9(old, new, newDB string, logger *log.Logger) (retErr error) {
	newTmpDir := tmpName(new)
	defer func() {
		if retErr != nil {
			stats.Add(upgradeFail, 1)
			if err := os.RemoveAll(newTmpDir); err != nil && !os.IsNotExist(err) {
				logger.Printf("failed to remove temporary upgraded snapshot directory at %s due to outer error (%s) cleanup: %s",
					newTmpDir, retErr, err)
			}
		}
	}()

	// If a temporary version of the new snapshot exists, remove it. This implies a
	// previous upgrade attempt was interrupted. We will need to start over.
	if dirExists(newTmpDir) {
		logger.Printf("detected temporary upgraded snapshot directory at %s, removing it", newTmpDir)
		if err := os.RemoveAll(newTmpDir); err != nil {
			return fmt.Errorf("failed to remove temporary upgraded snapshot directory %s: %s", newTmpDir, err)
		}
	}

	if dirExists(old) {
		logger.Printf("old snapshot directory exists at %s", old)
		oldIsEmpty, err := dirIsEmpty(old)
		if err != nil {
			return fmt.Errorf("failed to check if old snapshot directory %s is empty: %s", old, err)
		}

		if oldIsEmpty {
			logger.Printf("old snapshot directory %s is empty, nothing to upgrade", old)
			if err := os.RemoveAll(old); err != nil {
				return fmt.Errorf("failed to remove empty old snapshot directory %s: %s", old, err)
			}
			return nil
		}

		if dirExists(new) {
			logger.Printf("new snapshot directory %s exists", old)
			if err := os.RemoveAll(old); err != nil {
				return fmt.Errorf("failed to remove old snapshot directory %s: %s", old, err)
			}
			logger.Printf("removed old snapshot directory %s as no upgrade is needed", old)
			return nil
		}
	} else {
		logger.Printf("old v8 snapshot directory does not exist at %s, nothing to upgrade", old)
		return nil
	}

	// Start the upgrade process.
	if err := os.MkdirAll(newTmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create temporary snapshot directory %s: %s", newTmpDir, err)
	}

	oldMeta, err := getNewest8Snapshot(old)
	if err != nil {
		return fmt.Errorf("failed to get newest snapshot from old snapshots directory %s: %s", old, err)
	}
	if oldMeta == nil {
		// No snapshot to upgrade, this shouldn't happen since we checked for an empty old
		// directory earlier.
		return fmt.Errorf("no snapshot to upgrade in old snapshots directory %s", old)
	}
	logger.Printf("upgrading snapshot %s", oldMeta.ID)

	// Write out the new meta file in the new snapshot directory.
	newSnapshotDir := filepath.Join(newTmpDir, oldMeta.ID)
	if err := os.MkdirAll(newSnapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create new snapshot directory %s: %s", newSnapshotDir, err)
	}
	if err := writeMeta(newSnapshotDir, oldMeta); err != nil {
		return fmt.Errorf("failed to write new snapshot meta file to %s: %s", newSnapshotDir, err)
	}

	// Validate the SQLite file in the old snapshot.
	oldDBPath := filepath.Join(old, oldMeta.ID+".db")
	if !db.IsValidSQLiteFile(oldDBPath) {
		return fmt.Errorf("old snapshot database %s is not a valid SQLite file", oldDBPath)
	}
	if w, err := db.IsWALModeEnabledSQLiteFile(oldDBPath); err != nil {
		return fmt.Errorf("failed to check if old snapshot database %s has WAL mode enabled: %s", oldDBPath, err)
	} else if !w {
		return fmt.Errorf("old snapshot database %s does not have WAL mode enabled", oldDBPath)
	}

	// Install a proof for it.
	proof, err := NewProofFromFile(oldDBPath)
	if err != nil {
		return fmt.Errorf("failed to create proof from old snapshot database %s: %s", oldDBPath, err)
	}
	b, err := proof.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal proof for old snapshot database %s: %s", oldDBPath, err)
	}
	proofPath := filepath.Join(newSnapshotDir, "state.bin")
	if err := os.WriteFile(proofPath, b, 0644); err != nil {
		return fmt.Errorf("failed to install proof at %s for new snapshot: %s", proofPath, err)
	}

	// Move the SQLite database to the specified location.
	if err := os.Rename(oldDBPath, newDB); err != nil {
		return fmt.Errorf("failed to move old snapshot database %s to %s: %s", oldDBPath, newDB, err)
	}

	// Move the upgraded snapshot directory into place.
	if err := os.Rename(newTmpDir, new); err != nil {
		return fmt.Errorf("failed to move temporary snapshot directory %s to %s: %s", newTmpDir, new, err)
	}
	if err := syncDirParentMaybe(new); err != nil {
		return fmt.Errorf("failed to sync parent directory of new snapshot directory %s: %s", new, err)
	}

	// We're done! Remove old.
	if err := removeDirSync(old); err != nil {
		return fmt.Errorf("failed to remove old snapshot directory %s: %s", old, err)
	}
	logger.Printf("upgraded snapshot directory %s to %s", old, new)
	stats.Add(upgradeOk, 1)
	return nil
}

// getNewest8Snapshot returns the newest snapshot Raft meta in the given directory.
func getNewest8Snapshot(dir string) (*raft.SnapshotMeta, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// Populate the metadata
	var snapshots []*raft.SnapshotMeta
	for _, entry := range entries {
		// Ignore any files
		if !entry.IsDir() {
			continue
		}

		// Ignore any temporary snapshots
		dirName := entry.Name()
		if isTmpName(dirName) {
			continue
		}

		// Try to read the meta data
		meta, err := readMeta(filepath.Join(dir, dirName))
		if err != nil {
			return nil, fmt.Errorf("failed to read meta for snapshot %s: %s", dirName, err)
		}

		snapshots = append(snapshots, meta)
	}
	if len(snapshots) == 0 {
		return nil, nil
	}
	return raftMetaSlice(snapshots).Newest(), nil
}

// raftMetaSlice is a sortable slice of Raft Meta, which are sorted
// by term, index, and then ID. Snapshots are sorted from oldest to newest.
type raftMetaSlice []*raft.SnapshotMeta

func (s raftMetaSlice) Newest() *raft.SnapshotMeta {
	if len(s) == 0 {
		return nil
	}
	sort.Sort(s)
	return s[len(s)-1]
}

func (s raftMetaSlice) Len() int {
	return len(s)
}

func (s raftMetaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s raftMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
