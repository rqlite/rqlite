package snapshot9

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v8/random"
)

func Test_RemoveAllTmpSnapshotData_NonexistDir(t *testing.T) {
	err := RemoveAllTmpSnapshotData("nonexist")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func Test_RemoveAllTmpSnapshotData_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	err := RemoveAllTmpSnapshotData(dir)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	_, err = os.Stat(dir)
	if err != nil {
		t.Fatalf("expected exist, got %v", err)
	}
}

func Test_RemoveAllTmpSnapshotData_DirWithoutTmp(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, random.String())
	mustTouchFile(t, f)

	err := RemoveAllTmpSnapshotData(dir)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	_, err = os.Stat(f)
	if err != nil {
		t.Fatalf("expected exist, got %v", err)
	}
}

func Test_RemoveAllTmpSnapshotData_WithTmpDir(t *testing.T) {
	dir := t.TempDir()
	tmpSnapDir := filepath.Join(dir, tmpName(random.String()))
	if err := os.Mkdir(tmpSnapDir, 0700); err != nil {
		t.Fatalf("failed to create tmp dir: %v", err)
	}
	err := RemoveAllTmpSnapshotData(dir)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	_, err = os.Stat(tmpSnapDir)
	if !os.IsNotExist(err) {
		t.Fatalf("expected not exist, got %v", err)
	}
}

func Test_RemoveAllTmpSnapshotData_WithTmpDirMixed(t *testing.T) {
	dir := t.TempDir()
	tmpSnapDir := filepath.Join(dir, tmpName(random.String()))
	if err := os.Mkdir(tmpSnapDir, 0700); err != nil {
		t.Fatalf("failed to create tmp dir: %v", err)
	}
	otherDataDir := filepath.Join(dir, random.String())
	if err := os.Mkdir(otherDataDir, 0700); err != nil {
		t.Fatalf("failed to create other data dir: %v", err)
	}

	err := RemoveAllTmpSnapshotData(dir)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	_, err = os.Stat(tmpSnapDir)
	if !os.IsNotExist(err) {
		t.Fatalf("expected not exist, got %v", err)
	}
	_, err = os.Stat(otherDataDir)
	if err != nil {
		t.Fatalf("expected exist, got %v", err)
	}
}

func Test_RemoveAllTmpSnapshotData_WithMultipleTmpDir(t *testing.T) {
	dir := t.TempDir()
	tmpSnapDir1 := filepath.Join(dir, tmpName(random.String()))
	if err := os.Mkdir(tmpSnapDir1, 0700); err != nil {
		t.Fatalf("failed to create tmp dir: %v", err)
	}
	tmpSnapDir2 := filepath.Join(dir, tmpName(random.String()))
	if err := os.Mkdir(tmpSnapDir2, 0700); err != nil {
		t.Fatalf("failed to create tmp dir: %v", err)
	}

	err := RemoveAllTmpSnapshotData(dir)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	_, err = os.Stat(tmpSnapDir1)
	if !os.IsNotExist(err) {
		t.Fatalf("expected not exist, got %v", err)
	}
	_, err = os.Stat(tmpSnapDir2)
	if !os.IsNotExist(err) {
		t.Fatalf("expected not exist, got %v", err)
	}
}
