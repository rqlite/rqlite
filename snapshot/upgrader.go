package snapshot

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
)

// Upgrade writes a copy of the 7.x-format Snapshot dircectory at 'old' to a
// new Snapshot directory at 'new'. If the upgrade is successful, the
// 'old' directory is removed before the function returns.
func Upgrade(old, new string, logger *log.Logger) error {
	if !dirExists(old) {
		return nil
	}

	if dirExists(old) {
		oldEmpty, err := dirIsEmpty(old)
		if err != nil {
			return fmt.Errorf("failed to check if old snapshot directory %s is empty: %s", old, err)
		}

		if oldEmpty || dirExists(new) {
			if err := os.RemoveAll(old); err != nil {
				return fmt.Errorf("failed to remove old snapshot directory %s: %s", old, err)
			}
			logger.Printf("removed old snapshot directory %s as no upgrade is needed", old)
			return nil
		}
	}

	newTmpDir := tmpName(new)
	newGenerationDir := filepath.Join(newTmpDir, firstGeneration)

	// If a temporary version of the new directory exists, remove it. This
	// implies a previous upgrade attempt was interrupted. We will need to
	// start over.
	if dirExists(newTmpDir) {
		if err := os.RemoveAll(newTmpDir); err != nil {
			return fmt.Errorf("failed to remove temporary upgraded snapshot directory %s: %s", newTmpDir, err)
		}
		logger.Printf("removed temporary upgraded snapshot directory %s", tmpName(new))
	}

	// Start the upgrade process.
	logger.Printf("upgrading snapshot directory %s to %s", old, new)
	if err := os.MkdirAll(newTmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create temporary snapshot directory %s: %s", newTmpDir, err)
	}

	oldMeta, err := getNewest7Snapshot(old)
	if err != nil {
		return fmt.Errorf("failed to get newest snapshot from old snapshot directory %s: %s", old, err)
	}
	if oldMeta == nil {
		// No snapshot to upgrade, this shouldn't happen since we checked for an empty old
		// directory earlier.
		return fmt.Errorf("no snapshot to upgrade in old snapshot directory %s", old)
	}

	// Write out the new meta file.
	newSnapshotPath := filepath.Join(newGenerationDir, oldMeta.ID)
	if err := os.MkdirAll(newSnapshotPath, 0755); err != nil {
		return fmt.Errorf("failed to create new snapshot directory %s: %s", newSnapshotPath, err)
	}
	newMeta := &Meta{
		SnapshotMeta: *oldMeta,
		Full:         true,
	}
	if err := writeMeta(newSnapshotPath, newMeta); err != nil {
		return fmt.Errorf("failed to write new snapshot meta file: %s", err)
	}

	// Write SQLite data into generation directory, as the base SQLite file.
	newSqliteBasePath := filepath.Join(newGenerationDir, baseSqliteFile)
	_ = newSqliteBasePath

	// Read and decompress SQLite data into newSqliteBasePath
	// Perform basic checks of data: is it a valid SQLite file? Run a PRAGMA integrity_check?

	// Move the upgraded snapshot directory into place.
	if err := os.Rename(newTmpDir, new); err != nil {
		return fmt.Errorf("failed to move temporary snapshot directory %s to %s: %s", newTmpDir, new, err)
	}

	// We're done! Remove old.
	if err := os.RemoveAll(old); err != nil {
		return fmt.Errorf("failed to remove old snapshot directory %s: %s", old, err)
	}
	return nil
}

// getNewest7Snapshot returns the newest snapshot Raft meta in the given directory.
func getNewest7Snapshot(dir string) (*raft.SnapshotMeta, error) {
	return nil, nil
}

func dirIsEmpty(dir string) (bool, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	return len(files) == 0, nil
}
