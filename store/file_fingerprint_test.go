package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func Test_FileFingerprint_WriteAndRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fp.json")

	original := FileFingerprint{
		ModTime: time.Now().UTC().Truncate(time.Second),
		Size:    123456,
	}

	// Write fingerprint
	if err := original.WriteToFile(path); err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}

	// File should exist
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("expected fingerprint file to exist: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("expected non-empty file")
	}

	// Read it back
	var loaded FileFingerprint
	if err := loaded.ReadFromFile(path); err != nil {
		t.Fatalf("ReadFromFile failed: %v", err)
	}

	// Should match original
	if !loaded.ModTime.Equal(original.ModTime) {
		t.Errorf("ModTime mismatch: got %v, want %v", loaded.ModTime, original.ModTime)
	}
	if loaded.Size != original.Size {
		t.Errorf("Size mismatch: got %d, want %d", loaded.Size, original.Size)
	}
}

func Test_FileFingerprint_ReadFromMissingFile(t *testing.T) {
	var fp FileFingerprint
	err := fp.ReadFromFile("nonexistent.json")
	if err == nil {
		t.Fatalf("expected error reading nonexistent file")
	}
}

func Test_FileFingerprint_WriteToReadOnlyDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fp.json")

	// Make directory read-only
	if err := os.Chmod(dir, 0500); err != nil {
		t.Skipf("skipping: could not chmod dir: %v", err)
	}
	defer os.Chmod(dir, 0700)

	fp := FileFingerprint{ModTime: time.Now(), Size: 1}
	err := fp.WriteToFile(path)
	if err == nil {
		t.Fatalf("expected error writing to read-only dir")
	}
}
