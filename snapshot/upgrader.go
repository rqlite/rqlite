package snapshot

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v9/db"
)

const (
	v7StateFile = "state.bin"
)

// Upgrade7To8 writes a copy of the 7.x-format Snapshot directory at 'old' to a
// 8.x-format new Snapshot directory at 'new'. If the upgrade is successful,
// the 'old' directory is removed before the function returns.
func Upgrade7To8(old, new string, logger *log.Logger) (retErr error) {
	defer func() {
		if retErr != nil {
			stats.Add(upgradeFail, 1)
		}
	}()
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

	if !dirExists(old) {
		logger.Printf("old v7 snapshot directory does not exist at %s, nothing to upgrade", old)
		return nil
	}

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
		logger.Printf("new snapshot directory %s exists", new)
		if err := os.RemoveAll(old); err != nil {
			return fmt.Errorf("failed to remove old snapshot directory %s: %s", old, err)
		}
		logger.Printf("removed old snapshot directory %s as no upgrade is needed", old)
		return nil
	}

	// Start the upgrade process.
	if err := os.MkdirAll(newTmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create temporary snapshot directory %s: %s", newTmpDir, err)
	}

	oldMeta, err := getNewest7Snapshot(old)
	if err != nil {
		return fmt.Errorf("failed to get newest snapshot from old snapshots directory %s: %s", old, err)
	}
	if oldMeta == nil {
		// No snapshot to upgrade, this shouldn't happen since we checked for an empty old
		// directory earlier.
		return fmt.Errorf("no snapshot to upgrade in old snapshots directory %s", old)
	}

	// Write out the new meta file in the new snapshot directory.
	newSnapshotPath := filepath.Join(newTmpDir, oldMeta.ID)
	if err := os.MkdirAll(newSnapshotPath, 0755); err != nil {
		return fmt.Errorf("failed to create new snapshot directory %s: %s", newSnapshotPath, err)
	}
	if err := writeMeta(newSnapshotPath, oldMeta); err != nil {
		return fmt.Errorf("failed to write new snapshot meta file to %s: %s", newSnapshotPath, err)
	}

	// Ensure all file handles are closed before any directory is renamed or removed.
	if err := func() error {
		// Write SQLite database file into new snapshot dir.
		newSqlitePath := filepath.Join(newTmpDir, oldMeta.ID+".db")
		newSqliteFd, err := os.Create(newSqlitePath)
		if err != nil {
			return fmt.Errorf("failed to create new SQLite file %s: %s", newSqlitePath, err)
		}
		defer newSqliteFd.Close()

		// Copy the old state file into the new generation directory.
		oldStatePath := filepath.Join(old, oldMeta.ID, v7StateFile)
		stateFd, err := os.Open(oldStatePath)
		if err != nil {
			return fmt.Errorf("failed to open old state file %s: %s", oldStatePath, err)
		}
		defer stateFd.Close()
		sz, err := fileSize(oldStatePath)
		if err != nil {
			return fmt.Errorf("failed to get size of old state file %s: %s", oldStatePath, err)
		}
		logger.Printf("successfully opened old state file at %s (%d bytes in size)", oldStatePath, sz)

		headerLength := int64(16)
		if sz < headerLength {
			return fmt.Errorf("old state file %s is too small to be valid", oldStatePath)
		} else if sz == headerLength {
			logger.Printf("old state file %s contains no database data, no data to upgrade", oldStatePath)
		} else {
			// Skip past the header and length of the old state file.
			if _, err := stateFd.Seek(headerLength, 0); err != nil {
				return fmt.Errorf("failed to seek to beginning of old SQLite data %s: %s", oldStatePath, err)
			}
			gzipReader, err := gzip.NewReader(stateFd)
			if err != nil {
				return fmt.Errorf("failed to create gzip reader from old SQLite data at %s: %s", oldStatePath, err)
			}
			defer gzipReader.Close()
			if _, err := io.Copy(newSqliteFd, gzipReader); err != nil {
				return fmt.Errorf("failed to copy old SQLite file %s to new SQLite file %s: %s", oldStatePath,
					newSqlitePath, err)
			}
			if !db.IsValidSQLiteFile(newSqlitePath) {
				return fmt.Errorf("migrated SQLite file %s is not valid", newSqlitePath)
			}
		}

		// Ensure database file exists and convert to WAL mode.
		if err := db.EnsureWALMode(newSqlitePath); err != nil {
			return fmt.Errorf("failed to convert migrated SQLite file %s to WAL mode: %s", newSqlitePath, err)
		}
		return nil
	}(); err != nil {
		return err
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

// Upgrade8To10 writes a copy of the 8.x-format Snapshot directory at 'old' to a
// 10.x-format Snapshot directory at 'new'. In v8 format, the SQLite database file
// is stored at the root of the snapshot directory as '<id>.db', alongside a snapshot
// metadata directory '<id>/meta.json'. In v10 format, the database file is stored
// inside the snapshot directory as '<id>/data.db'. If the upgrade is successful,
// the 'old' directory is removed before the function returns.
func Upgrade8To10(old, new string, logger *log.Logger) (retErr error) {
	defer func() {
		if retErr != nil {
			stats.Add(upgradeFail, 1)
		}
	}()
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

	if !dirExists(old) {
		logger.Printf("old v8 snapshot directory does not exist at %s, nothing to upgrade", old)
		return nil
	}

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
		logger.Printf("new snapshot directory %s exists", new)
		if err := os.RemoveAll(old); err != nil {
			return fmt.Errorf("failed to remove old snapshot directory %s: %s", old, err)
		}
		logger.Printf("removed old snapshot directory %s as no upgrade is needed", old)
		return nil
	}

	// Find the newest v8 snapshot.
	snapID, snapMeta, err := getNewest8Snapshot(old)
	if err != nil {
		return fmt.Errorf("failed to get newest snapshot from old snapshots directory %s: %s", old, err)
	}
	if snapID == "" {
		logger.Printf("no v8-format snapshots found in %s, nothing to upgrade", old)
		return nil
	}

	// Start the upgrade process: build the v10 layout in a temp directory.
	if err := os.MkdirAll(newTmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create temporary snapshot directory %s: %s", newTmpDir, err)
	}

	newSnapshotDir := filepath.Join(newTmpDir, snapID)
	if err := os.MkdirAll(newSnapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create new snapshot directory %s: %s", newSnapshotDir, err)
	}

	if err := writeMeta(newSnapshotDir, snapMeta); err != nil {
		return fmt.Errorf("failed to write new snapshot meta file to %s: %s", newSnapshotDir, err)
	}

	// Copy the SQLite database file into the new snapshot directory as data.db.
	oldDBPath := filepath.Join(old, snapID+".db")
	newDBPath := filepath.Join(newSnapshotDir, dbfileName)
	if err := func() error {
		src, err := os.Open(oldDBPath)
		if err != nil {
			return fmt.Errorf("failed to open old SQLite file %s: %s", oldDBPath, err)
		}
		defer src.Close()

		dst, err := os.Create(newDBPath)
		if err != nil {
			return fmt.Errorf("failed to create new SQLite file %s: %s", newDBPath, err)
		}
		defer dst.Close()

		if _, err := io.Copy(dst, src); err != nil {
			return fmt.Errorf("failed to copy SQLite file %s to %s: %s", oldDBPath, newDBPath, err)
		}
		return dst.Sync()
	}(); err != nil {
		return err
	}

	if !db.IsValidSQLiteFile(newDBPath) {
		return fmt.Errorf("migrated SQLite file %s is not valid", newDBPath)
	}
	logger.Printf("copied v8 snapshot database %s to %s", oldDBPath, newDBPath)

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

// getNewest8Snapshot returns the ID and Raft meta of the newest v8-format snapshot
// in the given directory. A v8 snapshot is identified by a directory containing
// meta.json with a corresponding '<id>.db' file at the root level.
func getNewest8Snapshot(dir string) (string, *raft.SnapshotMeta, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", nil, err
	}

	// Build a set of .db files at root level for quick lookup.
	dbFiles := make(map[string]bool)
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".db" {
			dbFiles[strings.TrimSuffix(entry.Name(), ".db")] = true
		}
	}

	var snapshots []*raft.SnapshotMeta
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		if !dbFiles[id] {
			continue
		}
		mp := filepath.Join(dir, id, metaFileName)
		if !fileExists(mp) {
			continue
		}

		fh, err := os.Open(mp)
		if err != nil {
			return "", nil, err
		}
		defer fh.Close()

		meta := &raft.SnapshotMeta{}
		if err := json.NewDecoder(fh).Decode(meta); err != nil {
			return "", nil, err
		}
		snapshots = append(snapshots, meta)
	}
	if len(snapshots) == 0 {
		return "", nil, nil
	}
	newest := raftMetaSlice(snapshots).Newest()
	return newest.ID, newest, nil
}

// getNewest7Snapshot returns the newest snapshot Raft meta in the given directory.
func getNewest7Snapshot(dir string) (*raft.SnapshotMeta, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var snapshots []*raft.SnapshotMeta
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		metaPath := filepath.Join(dir, entry.Name(), metaFileName)
		if !fileExists(metaPath) {
			continue
		}

		fh, err := os.Open(metaPath)
		if err != nil {
			return nil, err
		}
		defer fh.Close()

		meta := &raft.SnapshotMeta{}
		dec := json.NewDecoder(fh)
		if err := dec.Decode(meta); err != nil {
			return nil, err
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
