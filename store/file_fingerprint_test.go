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
	if loaded.CRC32 != original.CRC32 {
		t.Fatalf("CRC32 mismatch: got %d, want %d", loaded.CRC32, original.CRC32)
	}
	if loaded.Index != original.Index {
		t.Fatalf("Index mismatch: got %d, want %d", loaded.Index, original.Index)
	}
	if loaded.Term != original.Term {
		t.Fatalf("Term mismatch: got %d, want %d", loaded.Term, original.Term)
	}
}

// Test_FileFingerprint_WriteAndRead_Legacy checks that a fingerprint written
// before the index and term were recorded is still readable, and reads back with
// both set to zero.
func Test_FileFingerprint_WriteAndRead_Legacy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fp.json")
	if err := os.WriteFile(path, []byte(`{"mod_time":"2026-08-30T09:00:00Z","size":123456,"crc32":78901234}`), 0644); err != nil {
		t.Fatalf("failed to write legacy fingerprint: %v", err)
	}

	var fp FileFingerprint
	if err := fp.ReadFromFile(path); err != nil {
		t.Fatalf("ReadFromFile failed: %v", err)
	}
	if exp, got := int64(123456), fp.Size; exp != got {
		t.Fatalf("Size mismatch: got %d, want %d", got, exp)
	}
	if exp, got := uint32(78901234), fp.CRC32; exp != got {
		t.Fatalf("CRC32 mismatch: got %d, want %d", got, exp)
	}
	if fp.Index != 0 || fp.Term != 0 {
		t.Fatalf("expected zero index and term, got %d and %d", fp.Index, fp.Term)
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
		term  uint64
		exp   bool
	}{
		{
			name: "Match",
			fp:   fp, mt: mt, sz: fp.Size, index: fp.Index, term: fp.Term, exp: true,
		},
		{
			name: "ModTimeChanged",
			fp:   fp, mt: mt.Add(time.Second), sz: fp.Size, index: fp.Index, term: fp.Term, exp: false,
		},
		{
			name: "SizeChanged",
			fp:   fp, mt: mt, sz: fp.Size + 1, index: fp.Index, term: fp.Term, exp: false,
		},
		{
			// The Snapshot Store has moved past the file, which means a snapshot
			// reached the Store but was never restored into the FSM. Using the file
			// as-is would mean adopting an index it has never seen.
			// See https://github.com/rqlite/rqlite/issues/2747.
			name: "SnapshotStoreAhead",
			fp:   fp, mt: mt, sz: fp.Size, index: fp.Index + 1, term: fp.Term, exp: false,
		},
		{
			// The file was fingerprinted for a snapshot which never became visible
			// in the Snapshot Store. The file holds state the Store does not, and
			// Raft would replay logs the file has already seen.
			name: "SnapshotStoreBehind",
			fp:   fp, mt: mt, sz: fp.Size, index: fp.Index - 1, term: fp.Term, exp: false,
		},
		{
			name: "TermChanged",
			fp:   fp, mt: mt, sz: fp.Size, index: fp.Index, term: fp.Term + 1, exp: false,
		},
		{
			// A fingerprint written before the index and term were recorded cannot be
			// shown to correspond to the newest snapshot, so a full restore is needed.
			// This costs an upgrading node one restore on its first startup.
			name: "LegacyFingerprint",
			fp:   legacy, mt: mt, sz: legacy.Size, index: 10, term: 2, exp: false,
		},
		{
			// A legacy fingerprint whose file has also changed is invalid for both
			// reasons.
			name: "LegacyFingerprintSizeChanged",
			fp:   legacy, mt: mt, sz: legacy.Size + 1, index: 10, term: 2, exp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fp.ValidFor(tt.mt, tt.sz, tt.index, tt.term); got != tt.exp {
				t.Fatalf("ValidFor(%s, %d, %d, %d) returned %v, expected %v",
					tt.mt, tt.sz, tt.index, tt.term, got, tt.exp)
			}
		})
	}
}
