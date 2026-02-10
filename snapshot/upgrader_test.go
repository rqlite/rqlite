package snapshot

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

func Test_Upgrade_NothingToDo(t *testing.T) {
	logger := log.New(os.Stderr, "[snapshot-store-upgrader] ", 0)
	if err := Upgrade7To8("/does/not/exist", "/does/not/exist/either", logger); err != nil {
		t.Fatalf("failed to upgrade nonexistent directories: %s", err)
	}

	oldEmpty := t.TempDir()
	newEmpty := t.TempDir()
	if err := Upgrade7To8(oldEmpty, newEmpty, logger); err != nil {
		t.Fatalf("failed to upgrade empty directories: %s", err)
	}
}

func Test_Upgrade_OK(t *testing.T) {
	t.Skip("upgrader tests deferred until new snapshot store is complete")
	logger := log.New(os.Stderr, "[snapshot-store-upgrader] ", 0)
	v7Snapshot := "testdata/upgrade/v7.20.3-snapshots"
	v7SnapshotID := "2-18-1686659761026"
	oldTemp := filepath.Join(t.TempDir(), "snapshots")
	newTemp := filepath.Join(t.TempDir(), "rsnapshots")

	// Copy directory because successful test runs will delete it.
	copyDir(v7Snapshot, oldTemp)

	// Upgrade it.
	if err := Upgrade7To8(oldTemp, newTemp, logger); err != nil {
		t.Fatalf("failed to upgrade empty directories: %s", err)
	}

	// Create new SnapshotStore from the upgraded directory, to verify its
	// contents.
	store, err := NewStore(newTemp)
	if err != nil {
		t.Fatalf("failed to create new snapshot store: %s", err)
	}

	snapshots, err := store.List()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}
	if got, exp := snapshots[0].ID, v7SnapshotID; got != exp {
		t.Fatalf("expected snapshot ID %s, got %s", exp, got)
	}

	meta, rc, err := store.Open(snapshots[0].ID)
	if err != nil {
		t.Fatalf("failed to open snapshot: %s", err)
	}
	rc.Close() // Removing test resources, when running on Windows, will fail otherwise.
	if exp, got := v7SnapshotID, meta.ID; exp != got {
		t.Fatalf("expected meta ID %s, got %s", exp, got)
	}
}

func Test_Upgrade_EmptyOK(t *testing.T) {
	t.Skip("upgrader tests deferred until new snapshot store is complete")
	logger := log.New(os.Stderr, "[snapshot-store-upgrader] ", 0)
	v7Snapshot := "testdata/upgrade/v7.20.3-empty-snapshots"
	v7SnapshotID := "2-18-1686659761026"
	oldTemp := filepath.Join(t.TempDir(), "snapshots")
	newTemp := filepath.Join(t.TempDir(), "rsnapshots")

	// Copy directory because successful test runs will delete it.
	copyDir(v7Snapshot, oldTemp)

	// Upgrade it.
	if err := Upgrade7To8(oldTemp, newTemp, logger); err != nil {
		t.Fatalf("failed to upgrade empty directories: %s", err)
	}

	// Create new SnapshotStore from the upgraded directory, to verify its
	// contents.
	store, err := NewStore(newTemp)
	if err != nil {
		t.Fatalf("failed to create new snapshot store: %s", err)
	}

	snapshots, err := store.List()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}
	if got, exp := snapshots[0].ID, v7SnapshotID; got != exp {
		t.Fatalf("expected snapshot ID %s, got %s", exp, got)
	}

	meta, rc, err := store.Open(snapshots[0].ID)
	if err != nil {
		t.Fatalf("failed to open snapshot: %s", err)
	}
	rc.Close() // Removing test resources, when running on Windows, will fail otherwise.
	if exp, got := v7SnapshotID, meta.ID; exp != got {
		t.Fatalf("expected meta ID %s, got %s", exp, got)
	}
}

func Test_Upgrade8To10_NothingToDo(t *testing.T) {
	logger := log.New(os.Stderr, "[snapshot-store-upgrader-test] ", 0)

	// Nonexistent directory is a no-op.
	if err := Upgrade8To10("/does/not/exist", logger); err != nil {
		t.Fatalf("failed to upgrade nonexistent directory: %s", err)
	}

	// Empty directory is a no-op.
	dir := t.TempDir()
	if err := Upgrade8To10(dir, logger); err != nil {
		t.Fatalf("failed to upgrade empty directory: %s", err)
	}

	// Directory with only v10-format snapshots is a no-op.
	v10Dir := t.TempDir()
	mustCreateV10Snapshot(t, v10Dir, "2-18-1686659761026", 18, 2)
	if err := Upgrade8To10(v10Dir, logger); err != nil {
		t.Fatalf("failed to upgrade already-v10 directory: %s", err)
	}
	// Verify the v10 snapshot is still intact.
	store, err := NewStore(v10Dir)
	if err != nil {
		t.Fatalf("failed to create store from v10 dir: %s", err)
	}
	if store.Len() != 1 {
		t.Fatalf("expected 1 snapshot, got %d", store.Len())
	}
}

func Test_Upgrade8To10_OK(t *testing.T) {
	logger := log.New(os.Stderr, "[snapshot-store-upgrader-test] ", 0)
	dir := t.TempDir()

	snapshotID := "2-18-1686659761026"
	mustCreateV8Snapshot(t, dir, snapshotID, 18, 2)

	// Verify v8 layout: .db at root, meta.json in subdirectory, no data.db in subdirectory.
	if !fileExists(filepath.Join(dir, snapshotID+".db")) {
		t.Fatal("expected .db file at root")
	}
	if !fileExists(filepath.Join(dir, snapshotID, metaFileName)) {
		t.Fatal("expected meta.json in snapshot directory")
	}
	if fileExists(filepath.Join(dir, snapshotID, dbfileName)) {
		t.Fatal("did not expect data.db in snapshot directory before upgrade")
	}

	// Upgrade.
	if err := Upgrade8To10(dir, logger); err != nil {
		t.Fatalf("failed to upgrade: %s", err)
	}

	// Verify v10 layout: no .db at root, data.db inside snapshot directory.
	if fileExists(filepath.Join(dir, snapshotID+".db")) {
		t.Fatal("expected .db file at root to be removed after upgrade")
	}
	if !fileExists(filepath.Join(dir, snapshotID, dbfileName)) {
		t.Fatal("expected data.db in snapshot directory after upgrade")
	}

	// Verify the store can open the upgraded snapshot.
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %s", err)
	}
	snaps, err := store.List()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}
	if snaps[0].ID != snapshotID {
		t.Fatalf("expected snapshot ID %s, got %s", snapshotID, snaps[0].ID)
	}
	meta, rc, err := store.Open(snaps[0].ID)
	if err != nil {
		t.Fatalf("failed to open snapshot: %s", err)
	}
	rc.Close()
	if meta.ID != snapshotID {
		t.Fatalf("expected meta ID %s, got %s", snapshotID, meta.ID)
	}
}

func Test_Upgrade8To10_Multiple(t *testing.T) {
	logger := log.New(os.Stderr, "[snapshot-store-upgrader-test] ", 0)
	dir := t.TempDir()

	id1 := "1-10-1000000000000"
	id2 := "2-20-2000000000000"
	mustCreateV8Snapshot(t, dir, id1, 10, 1)
	mustCreateV8Snapshot(t, dir, id2, 20, 2)

	if err := Upgrade8To10(dir, logger); err != nil {
		t.Fatalf("failed to upgrade: %s", err)
	}

	// Both should be upgraded.
	for _, id := range []string{id1, id2} {
		if fileExists(filepath.Join(dir, id+".db")) {
			t.Fatalf("expected .db file at root removed for %s", id)
		}
		if !fileExists(filepath.Join(dir, id, dbfileName)) {
			t.Fatalf("expected data.db in snapshot directory for %s", id)
		}
	}

	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %s", err)
	}
	if store.Len() != 2 {
		t.Fatalf("expected 2 snapshots, got %d", store.Len())
	}
}

func Test_Upgrade8To10_CrashRecovery(t *testing.T) {
	logger := log.New(os.Stderr, "[snapshot-store-upgrader-test] ", 0)
	dir := t.TempDir()

	snapshotID := "2-18-1686659761026"

	// Simulate a crash where the rename completed but the root .db was not removed:
	// both <id>.db at root and <id>/data.db exist.
	mustCreateV8Snapshot(t, dir, snapshotID, 18, 2)
	mustCopyFileT(t, filepath.Join(dir, snapshotID+".db"), filepath.Join(dir, snapshotID, dbfileName))

	if err := Upgrade8To10(dir, logger); err != nil {
		t.Fatalf("failed to upgrade: %s", err)
	}

	// Root .db should be cleaned up.
	if fileExists(filepath.Join(dir, snapshotID+".db")) {
		t.Fatal("expected .db file at root to be removed")
	}
	if !fileExists(filepath.Join(dir, snapshotID, dbfileName)) {
		t.Fatal("expected data.db in snapshot directory")
	}

	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %s", err)
	}
	if store.Len() != 1 {
		t.Fatalf("expected 1 snapshot, got %d", store.Len())
	}
}

func Test_Upgrade8To10_Idempotent(t *testing.T) {
	logger := log.New(os.Stderr, "[snapshot-store-upgrader-test] ", 0)
	dir := t.TempDir()

	snapshotID := "2-18-1686659761026"
	mustCreateV8Snapshot(t, dir, snapshotID, 18, 2)

	// Upgrade twice.
	if err := Upgrade8To10(dir, logger); err != nil {
		t.Fatalf("first upgrade failed: %s", err)
	}
	if err := Upgrade8To10(dir, logger); err != nil {
		t.Fatalf("second upgrade failed: %s", err)
	}

	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %s", err)
	}
	if store.Len() != 1 {
		t.Fatalf("expected 1 snapshot, got %d", store.Len())
	}
}

// mustCreateV8Snapshot creates a v8-format snapshot in dir: <id>.db at root
// and <id>/meta.json in a subdirectory.
func mustCreateV8Snapshot(t *testing.T, dir, snapshotID string, idx, term uint64) {
	t.Helper()
	snapDir := filepath.Join(dir, snapshotID)
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		t.Fatalf("failed to create snapshot dir: %v", err)
	}
	meta := &raft.SnapshotMeta{
		ID:    snapshotID,
		Index: idx,
		Term:  term,
	}
	if err := writeMeta(snapDir, meta); err != nil {
		t.Fatalf("failed to write meta: %v", err)
	}
	// Place the SQLite DB file at the root level as <id>.db (v8 format).
	mustCopyFileT(t, "testdata/db-and-wals/full2.db", filepath.Join(dir, snapshotID+".db"))
}

// mustCreateV10Snapshot creates a v10-format snapshot in dir: <id>/meta.json
// and <id>/data.db.
func mustCreateV10Snapshot(t *testing.T, dir, snapshotID string, idx, term uint64) {
	t.Helper()
	snapDir := filepath.Join(dir, snapshotID)
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		t.Fatalf("failed to create snapshot dir: %v", err)
	}
	meta := &raft.SnapshotMeta{
		ID:    snapshotID,
		Index: idx,
		Term:  term,
	}
	if err := writeMeta(snapDir, meta); err != nil {
		t.Fatalf("failed to write meta: %v", err)
	}
	mustCopyFileT(t, "testdata/db-and-wals/full2.db", filepath.Join(snapDir, dbfileName))
}

func mustCopyFileT(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("failed to read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0644); err != nil {
		t.Fatalf("failed to write %s: %v", dst, err)
	}
}

/* MIT License
 *
 * Copyright (c) 2017 Roland Singer [roland.singer@desertbit.com]
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

// copyFile copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file. The file mode will be copied from the source and
// the copied data is synced/flushed to stable storage.
func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		if e := out.Close(); e != nil {
			err = e
		}
	}()

	_, err = io.Copy(out, in)
	if err != nil {
		return
	}

	err = out.Sync()
	if err != nil {
		return
	}

	si, err := os.Stat(src)
	if err != nil {
		return
	}
	err = os.Chmod(dst, si.Mode())
	if err != nil {
		return
	}

	return
}

// copyDir recursively copies a directory tree, attempting to preserve permissions.
// Source directory must exist, destination directory must *not* exist.
// Symlinks are ignored and skipped.
func copyDir(src string, dst string) (err error) {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !si.IsDir() {
		return fmt.Errorf("source is not a directory")
	}

	_, err = os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	if err == nil {
		return fmt.Errorf("destination already exists")
	}

	err = os.MkdirAll(dst, si.Mode())
	if err != nil {
		return
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			err = copyDir(srcPath, dstPath)
			if err != nil {
				return
			}
		} else {
			// Skip symlinks.
			if entry.Type()&fs.ModeSymlink != 0 {
				continue
			}

			err = copyFile(srcPath, dstPath)
			if err != nil {
				return
			}
		}
	}

	return
}
