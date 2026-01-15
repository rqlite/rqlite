package snapshot2

import "testing"

func Test_SnapshotResolver_EmptyStore(t *testing.T) {
	rootDir := t.TempDir()
	_, _, err := SnapshotResolver(rootDir, "nonexistent-snapshot-store-is-empty")
	if err != ErrSnapshotNotFound {
		t.Fatalf("expected ErrSnapshotNotFound, got: %v", err)
	}
}
