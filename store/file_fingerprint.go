package store

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// FileFingerprint describes a file, and the snapshot that file corresponds to,
// at the moment the file was fingerprinted. It can be written to or read from
// disk as JSON. Fingerprints written by older versions may not have the CRC32,
// Index and Term fields, so they are optional.
type FileFingerprint struct {
	ModTime time.Time `json:"mod_time"`
	Size    int64     `json:"size"`
	CRC32   uint32    `json:"crc32,omitempty"`
	Index   uint64    `json:"index,omitempty"`
	Term    uint64    `json:"term,omitempty"`
}

// String implements the Stringer interface.
func (f *FileFingerprint) String() string {
	return fmt.Sprintf("FileFingerprint{mod time: %s, size: %d, CRC32: %d, index: %d, term: %d}",
		f.ModTime, f.Size, f.CRC32, f.Index, f.Term)
}

// WriteToFile saves the fingerprint to a file and fsyncs it to disk.
func (f *FileFingerprint) WriteToFile(path string) error {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}

	// Create or truncate the file.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write data and sync.
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}

// ReadFromFile loads the fingerprint from a file at the given path.
func (f *FileFingerprint) ReadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, f)
}

// ValidFor returns whether the file described by this fingerprint can be used
// as-is at startup, given that the newest snapshot in the Snapshot Store is at
// the given index and term. Two things must hold: the file must be unchanged
// since it was fingerprinted, and it must be the file which corresponds to that
// snapshot.
//
// If the two have parted company then a snapshot reached the Snapshot Store
// without being restored into the FSM, or a snapshot was fingerprinted but never
// became visible in the Store. Either way the file and the Store describe
// different states, and only a restore from the Store can resolve it. See
// https://github.com/rqlite/rqlite/issues/2747.
//
// A fingerprint written before the index and term were recorded has both set to
// zero and so is never valid while any snapshot exists. Such a node performs one
// full restore on its first startup after upgrading.
//
// The CRC32 is deliberately not checked here: calculating it means reading the
// whole file, so that check is made separately.
func (f *FileFingerprint) ValidFor(mt time.Time, sz int64, index, term uint64) bool {
	return f.ModTime.Equal(mt) && f.Size == sz && f.Index == index && f.Term == term
}
