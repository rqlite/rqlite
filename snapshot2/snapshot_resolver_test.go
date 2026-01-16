package snapshot2

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

func Test_SnapshotResolver_EmptyStore(t *testing.T) {
	rootDir := t.TempDir()
	_, _, err := SnapshotResolver(rootDir, "nonexistent-snapshot-store-is-empty")
	if err != ErrSnapshotNotFound {
		t.Fatalf("expected ErrSnapshotNotFound, got: %v", err)
	}
}

func Test_SnapshotResolver_SingleFull(t *testing.T) {
	snapshotID := "full-snapshot-1"
	rootDir := t.TempDir()

	mustCreateSnapshotFull(t, rootDir, snapshotID, 1, 1)

	dbfile, walFiles, err := SnapshotResolver(rootDir, snapshotID)
	if err != nil {
		t.Fatalf("unexpected error from SnapshotResolver: %v", err)
	}
	if dbfile != filepath.Join(rootDir, snapshotID, dbfileName) {
		t.Fatalf("expected dbfile %s, got %s", filepath.Join(rootDir, snapshotID, dbfileName), dbfile)
	}
	if len(walFiles) != 0 {
		t.Fatalf("expected no wal files, got %v", walFiles)
	}
}

func Test_SnapshotResolver_DoubleFull(t *testing.T) {
	snapshotID1 := "full-snapshot-1"
	snapshotID2 := "full-snapshot-2"
	rootDir := t.TempDir()

	mustCreateSnapshotFull(t, rootDir, snapshotID1, 1, 1)
	mustCreateSnapshotFull(t, rootDir, snapshotID2, 2, 1)

	dbfile, walFiles, err := SnapshotResolver(rootDir, snapshotID2)
	if err != nil {
		t.Fatalf("unexpected error from SnapshotResolver: %v", err)
	}
	if dbfile != filepath.Join(rootDir, snapshotID2, dbfileName) {
		t.Fatalf("expected dbfile %s, got %s", filepath.Join(rootDir, snapshotID2, dbfileName), dbfile)
	}
	if len(walFiles) != 0 {
		t.Fatalf("expected no wal files, got %v", walFiles)
	}

	// Check the first full snapshot also.
	dbfile, walFiles, err = SnapshotResolver(rootDir, snapshotID1)
	if err != nil {
		t.Fatalf("unexpected error from SnapshotResolver: %v", err)
	}
	if dbfile != filepath.Join(rootDir, snapshotID1, dbfileName) {
		t.Fatalf("expected dbfile %s, got %s", filepath.Join(rootDir, snapshotID1, dbfileName), dbfile)
	}
	if len(walFiles) != 0 {
		t.Fatalf("expected no wal files, got %v", walFiles)
	}
}

func Test_SnapshotResolver_SingleFullSingleInc(t *testing.T) {
	snapshotFullID := "00000-1"
	snapshotIncID := "00000-2"
	rootDir := t.TempDir()

	mustCreateSnapshotFull(t, rootDir, snapshotFullID, 1, 1)
	mustCreateSnapshotInc(t, rootDir, snapshotIncID, 2, 1)

	dbfile, walFiles, err := SnapshotResolver(rootDir, snapshotIncID)
	if err != nil {
		t.Fatalf("unexpected error from SnapshotResolver: %v", err)
	}
	if dbfile != filepath.Join(rootDir, snapshotFullID, dbfileName) {
		t.Fatalf("expected dbfile %s, got %s", filepath.Join(rootDir, snapshotFullID, dbfileName), dbfile)
	}
	if len(walFiles) != 1 {
		t.Fatalf("expected 1 wal file, got %v", walFiles)
	}
	expectedWalFile := filepath.Join(rootDir, snapshotIncID, walfileName)
	if walFiles[0] != expectedWalFile {
		t.Fatalf("expected wal file %s, got %s", expectedWalFile, walFiles[0])
	}
}

func Test_SnapshotResolver_SingleFullDoubleInc(t *testing.T) {
	snapshotFullID := "00000-1"
	snapshotIncID1 := "00000-2"
	snapshotIncID2 := "00000-3"
	rootDir := t.TempDir()

	mustCreateSnapshotFull(t, rootDir, snapshotFullID, 1, 1)
	mustCreateSnapshotInc(t, rootDir, snapshotIncID1, 2, 1)
	mustCreateSnapshotInc(t, rootDir, snapshotIncID2, 3, 1)

	dbfile, walFiles, err := SnapshotResolver(rootDir, snapshotIncID2)
	if err != nil {
		t.Fatalf("unexpected error from SnapshotResolver: %v", err)
	}
	if dbfile != filepath.Join(rootDir, snapshotFullID, dbfileName) {
		t.Fatalf("expected dbfile %s, got %s", filepath.Join(rootDir, snapshotFullID, dbfileName), dbfile)
	}
	if len(walFiles) != 2 {
		t.Fatalf("expected 2 wal files, got %v", walFiles)
	}
	expectedWalFile1 := filepath.Join(rootDir, snapshotIncID1, walfileName)
	if walFiles[0] != expectedWalFile1 {
		t.Fatalf("expected wal file %s, got %s", expectedWalFile1, walFiles[0])
	}
	expectedWalFile2 := filepath.Join(rootDir, snapshotIncID2, walfileName)
	if walFiles[1] != expectedWalFile2 {
		t.Fatalf("expected wal file %s, got %s", expectedWalFile2, walFiles[1])
	}

	// Test also looking for the intermediate incremental snapshot.
	dbfile, walFiles, err = SnapshotResolver(rootDir, snapshotIncID1)
	if err != nil {
		t.Fatalf("unexpected error from SnapshotResolver: %v", err)
	}
	if dbfile != filepath.Join(rootDir, snapshotFullID, dbfileName) {
		t.Fatalf("expected dbfile %s, got %s", filepath.Join(rootDir, snapshotFullID, dbfileName), dbfile)
	}
	if len(walFiles) != 1 {
		t.Fatalf("expected 1 wal file, got %v", walFiles)
	}
	if walFiles[0] != expectedWalFile1 {
		t.Fatalf("expected wal file %s, got %s", expectedWalFile1, walFiles[0])
	}

	// Check that looking for a non-existent snapshot fails.
	_, _, err = SnapshotResolver(rootDir, "non-existent-snapshot")
	if err != ErrSnapshotNotFound {
		t.Fatalf("expected ErrSnapshotNotFound, got: %v", err)
	}
}

func Test_SnapshotResolver_SingleFullDoubleInc_IgnoresEarlier(t *testing.T) {
	snapshotFullID0 := "00000-0"
	snapshotFullID1 := "00000-1"
	snapshotIncID1 := "00000-2"
	snapshotIncID2 := "00000-3"
	rootDir := t.TempDir()

	mustCreateSnapshotFull(t, rootDir, snapshotFullID0, 1, 1)
	mustCreateSnapshotFull(t, rootDir, snapshotFullID1, 2, 1)
	mustCreateSnapshotInc(t, rootDir, snapshotIncID1, 3, 1)
	mustCreateSnapshotInc(t, rootDir, snapshotIncID2, 4, 1)

	dbfile, walFiles, err := SnapshotResolver(rootDir, snapshotIncID2)
	if err != nil {
		t.Fatalf("unexpected error from SnapshotResolver: %v", err)
	}
	if dbfile != filepath.Join(rootDir, snapshotFullID1, dbfileName) {
		t.Fatalf("expected dbfile %s, got %s", filepath.Join(rootDir, snapshotFullID1, dbfileName), dbfile)
	}
	if len(walFiles) != 2 {
		t.Fatalf("expected 2 wal files, got %v", walFiles)
	}
	expectedWalFile1 := filepath.Join(rootDir, snapshotIncID1, walfileName)
	if walFiles[0] != expectedWalFile1 {
		t.Fatalf("expected wal file %s, got %s", expectedWalFile1, walFiles[0])
	}
	expectedWalFile2 := filepath.Join(rootDir, snapshotIncID2, walfileName)
	if walFiles[1] != expectedWalFile2 {
		t.Fatalf("expected wal file %s, got %s", expectedWalFile2, walFiles[1])
	}
}

func Test_SnapshotResolver_NoFullInChain(t *testing.T) {
	snapshotIncID1 := "00000-1"
	snapshotIncID2 := "00000-2"
	rootDir := t.TempDir()

	mustCreateSnapshotInc(t, rootDir, snapshotIncID1, 1, 1)
	mustCreateSnapshotInc(t, rootDir, snapshotIncID2, 2, 1)

	_, _, err := SnapshotResolver(rootDir, snapshotIncID2)
	if err == nil {
		t.Fatalf("expected error from SnapshotResolver, got nil")
	}
}

func mustCopyFile(t *testing.T, src, dest string) {
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("failed to read file %s: %v", src, err)
	}
	if err := os.WriteFile(dest, data, 0644); err != nil {
		t.Fatalf("failed to write file %s: %v", dest, err)
	}
}

func mustCreateSnapshotFull(t *testing.T, rootDir, snapshotID string, idx, term uint64) {
	mustCreateSnapshot(t, rootDir, snapshotID, "testdata/db-and-wals/full2.db", dbfileName, idx, term)
}

func mustCreateSnapshotInc(t *testing.T, rootDir, snapshotID string, idx, term uint64) {
	mustCreateSnapshot(t, rootDir, snapshotID, "testdata/db-and-wals/wal-00", walfileName, idx, term)
}

func mustCreateSnapshot(t *testing.T, rootDir string, snapshotID, srcName, dstName string, idx, term uint64) {
	snapshotDir := filepath.Join(rootDir, snapshotID)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatalf("failed to create snapshot dir: %v", err)
	}

	mustCopyFile(t, srcName, filepath.Join(snapshotDir, dstName))
	meta := &raft.SnapshotMeta{
		ID:    snapshotID,
		Index: idx,
		Term:  term,
	}
	if err := writeMeta(snapshotDir, meta); err != nil {
		t.Fatalf("failed to write snapshot meta: %v", err)
	}
}
