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
		t.Fatalf("ModTime mismatch: got %v, want %v", loaded.ModTime, original.ModTime)
	}
	if loaded.Size != original.Size {
		t.Fatalf("Size mismatch: got %d, want %d", loaded.Size, original.Size)
	}

	if !loaded.Compare(original.ModTime, original.Size) {
		t.Fatalf("Compare returned false for matching values")
	}

	if loaded.Compare(original.ModTime, original.Size+1) {
		t.Fatalf("Compare returned true for non-matching size")
	}

	if loaded.Compare(original.ModTime.Add(time.Second), original.Size) {
		t.Fatalf("Compare returned true for non-matching mod time")
	}
}

func Test_FileFingerprint_ReadFromMissingFile(t *testing.T) {
	var fp FileFingerprint
	err := fp.ReadFromFile("nonexistent.json")
	if err == nil {
		t.Fatalf("expected error reading nonexistent file")
	}
}
