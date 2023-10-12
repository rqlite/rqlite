package snapshot2

import (
	"os"
	"testing"
)

func Test_RemoveAllTmpSnapshotData(t *testing.T) {
	dir := t.TempDir()
	if err := RemoveAllTmpSnapshotData(dir); err != nil {
		t.Fatalf("Failed to remove all tmp snapshot data: %v", err)
	}
	if !pathExists(dir) {
		t.Fatalf("Expected dir to exist, but it does not")
	}
	directories, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}
	if len(directories) != 0 {
		t.Fatalf("Expected dir to be empty, got %d files", len(directories))
	}

	mustTouchDir(t, dir+"/dir")
	mustTouchFile(t, dir+"/file")
	if err := RemoveAllTmpSnapshotData(dir); err != nil {
		t.Fatalf("Failed to remove all tmp snapshot data: %v", err)
	}
	if !pathExists(dir + "/dir") {
		t.Fatalf("Expected dir to exist, but it does not")
	}
	if !pathExists(dir + "/file") {
		t.Fatalf("Expected file to exist, but it does not")
	}

	mustTouchDir(t, dir+"/snapshot1234.tmp")
	mustTouchFile(t, dir+"/snapshot1234.db")
	mustTouchFile(t, dir+"/snapshot1234.db-wal")
	mustTouchFile(t, dir+"/snapshot1234-5678")
	if err := RemoveAllTmpSnapshotData(dir); err != nil {
		t.Fatalf("Failed to remove all tmp snapshot data: %v", err)
	}
	if !pathExists(dir + "/dir") {
		t.Fatalf("Expected dir to exist, but it does not")
	}
	if !pathExists(dir + "/file") {
		t.Fatalf("Expected file to exist, but it does not")
	}
	if pathExists(dir + "/snapshot1234.tmp") {
		t.Fatalf("Expected snapshot1234.tmp to not exist, but it does")
	}
	if pathExists(dir + "/snapshot1234.db") {
		t.Fatalf("Expected snapshot1234.db to not exist, but it does")
	}
	if pathExists(dir + "/snapshot1234.db-wal") {
		t.Fatalf("Expected snapshot1234.db-wal to not exist, but it does")
	}
	if pathExists(dir + "/snapshot1234-5678") {
		t.Fatalf("Expected /snapshot1234-5678 to not exist, but it does")
	}

	mustTouchFile(t, dir+"/snapshotABCD.tmp")
	if err := RemoveAllTmpSnapshotData(dir); err != nil {
		t.Fatalf("Failed to remove all tmp snapshot data: %v", err)
	}
	if !pathExists(dir + "/snapshotABCD.tmp") {
		t.Fatalf("Expected /snapshotABCD.tmp to exist, but it does not")
	}
}

func Test_NewStore(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("Failed to create new store: %v", err)
	}

	if store.Dir() != dir {
		t.Errorf("Expected store directory to be %s, got %s", dir, store.Dir())
	}
}

func Test_StoreEmpty(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewStore(dir)

	snaps, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snaps) != 0 {
		t.Errorf("Expected no snapshots, got %d", len(snaps))
	}

	_, _, err = store.Open("non-existent")
	if err == nil {
		t.Fatalf("Expected error opening non-existent snapshot, got nil")
	}

	n, err := store.Reap()
	if err != nil {
		t.Fatalf("Failed to reap snapshots from empty store: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected no snapshots reaped, got %d", n)
	}

	if _, err := store.Stats(); err != nil {
		t.Fatalf("Failed to get stats from empty store: %v", err)
	}
}

func mustTouchFile(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Create(path); err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
}

func mustTouchDir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
