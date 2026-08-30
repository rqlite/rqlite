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
		CRC32:   78901234,
		Index:   10,
		Term:    2,
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
	if loaded.Index != original.Index {
		t.Fatalf("Index mismatch: got %d, want %d", loaded.Index, original.Index)
	}
	if loaded.Term != original.Term {
		t.Fatalf("Term mismatch: got %d, want %d", loaded.Term, original.Term)
	}

	if !loaded.Compare(original.ModTime, original.Size, original.CRC32) {
		t.Fatalf("Compare returned false for matching values")
	}

	if loaded.Compare(original.ModTime, original.Size+1, original.CRC32) {
		t.Fatalf("Compare returned true for non-matching size")
	}

	if loaded.Compare(original.ModTime.Add(time.Second), original.Size, original.CRC32) {
		t.Fatalf("Compare returned true for non-matching mod time")
	}

	if loaded.Compare(original.ModTime, original.Size, original.CRC32+1) {
		t.Fatalf("Compare returned true for non-matching CRC32")
	}

	// Test backward compatibility with zero CRC32
	loaded.CRC32 = 0
	if !loaded.Compare(original.ModTime, original.Size, original.CRC32+1) {
		t.Fatalf("Compare returned false for zero CRC32")
	}
}

func Test_FileFingerprint_ReadFromMissingFile(t *testing.T) {
	var fp FileFingerprint
	err := fp.ReadFromFile("nonexistent.json")
	if err == nil {
		t.Fatalf("expected error reading nonexistent file")
	}
}

// Test_FileFingerprint_ValidFor tests the rule which decides whether the SQLite
// file a node finds on disk at startup can be used as-is, or whether the node
// must restore from the Snapshot Store instead.
func Test_FileFingerprint_ValidFor(t *testing.T) {
	mt := time.Now().UTC().Truncate(time.Second)
	fp := FileFingerprint{
		ModTime: mt,
		Size:    123456,
		CRC32:   78901234,
		Index:   10,
		Term:    2,
	}
	legacy := FileFingerprint{
		ModTime: mt,
		Size:    fp.Size,
	}

	tests := []struct {
		name  string
		fp    FileFingerprint
		mt    time.Time
		sz    int64
		index uint64
		exp   bool
	}{
		{
			name: "Match",
			fp:   fp, mt: mt, sz: fp.Size, index: fp.Index, exp: true,
		},
		{
			name: "ModTimeChanged",
			fp:   fp, mt: mt.Add(time.Second), sz: fp.Size, index: fp.Index, exp: false,
		},
		{
			name: "SizeChanged",
			fp:   fp, mt: mt, sz: fp.Size + 1, index: fp.Index, exp: false,
		},
		{
			// The Snapshot Store has moved past the file, which means a snapshot
			// reached the Store but was never restored into the FSM. Using the file
			// as-is would mean adopting an index it has never seen.
			// See https://github.com/rqlite/rqlite/issues/2747.
			name: "SnapshotStoreAhead",
			fp:   fp, mt: mt, sz: fp.Size, index: fp.Index + 1, exp: false,
		},
		{
			// The file is ahead of the newest snapshot, which is the normal state
			// after log entries have been applied since the last snapshot.
			name: "SnapshotStoreBehind",
			fp:   fp, mt: mt, sz: fp.Size, index: fp.Index - 1, exp: true,
		},
		{
			// A fingerprint written before the index was recorded cannot be shown to
			// correspond to the newest snapshot, so a full restore is needed. This
			// costs an upgrading node one restore on its first startup.
			name: "LegacyFingerprint",
			fp:   legacy, mt: mt, sz: legacy.Size, index: 1, exp: false,
		},
		{
			// A legacy fingerprint whose file has also changed is invalid for both
			// reasons.
			name: "LegacyFingerprintSizeChanged",
			fp:   legacy, mt: mt, sz: legacy.Size + 1, index: 1, exp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fp.ValidFor(tt.mt, tt.sz, tt.index); got != tt.exp {
				t.Fatalf("ValidFor(%s, %d, %d) returned %v, expected %v",
					tt.mt, tt.sz, tt.index, got, tt.exp)
			}
		})
	}
}
