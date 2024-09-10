package snapshot9

import (
	"fmt"
	"log"
	"os"

	"github.com/hashicorp/raft"
)

// Upgrade writes a copy of the 8.x-format Snapshot directory at 'old' to a
// 9.x-format new Snapshot directory at 'new'. If the upgrade is successful,
// the 'old' directory is removed before the function returns.
func Upgrade8To9(old, new string, logger *log.Logger) (retErr error) {
	newTmpDir := tmpName(new)
	defer func() {
		if retErr != nil {
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

	return nil
}

// getNewest8Snapshot returns the newest snapshot Raft meta in the given directory.
func getNewest8Snapshot(dir string) (*raft.SnapshotMeta, error) {
	return nil, nil
}
