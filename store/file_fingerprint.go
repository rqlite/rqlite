package store

import (
	"encoding/json"
	"os"
	"time"
)

// FileFingerprint describes a file, and the snapshot that file corresponds to,
// at the moment the file was fingerprinted. It can be written to or read from
// disk as JSON. Older versions may not have the CRC32, Index and Term fields,
// so they are optional.
type FileFingerprint struct {
	ModTime time.Time `json:"mod_time"`
	Size    int64     `json:"size"`
	CRC32   uint32    `json:"crc32,omitempty"`
	Index   uint64    `json:"index,omitempty"`
	Term    uint64    `json:"term,omitempty"`
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

// Compare checks if the given modification time and size match the fingerprint.
// If the CRC32 in the fingerprint is zero, it is ignored in the comparison to
// allow for backward compatibility.
func (f *FileFingerprint) Compare(mt time.Time, sz int64, crc uint32) bool {
	return f.ModTime.Equal(mt) && f.Size == sz && (f.CRC32 == crc || f.CRC32 == 0)
}

// ValidFor returns whether the file described by this fingerprint can be used
// as-is at startup, given that the newest snapshot in the Snapshot Store is at
// the given index. Two things must hold: the file must be unchanged since it was
// fingerprinted, and it must correspond to a snapshot at least as recent as that
// index. If the Snapshot Store has moved past the file then a snapshot reached
// the Store but was never restored into the FSM, and using the file as-is would
// mean adopting an index the file has never seen.
//
// A fingerprint written before the index was recorded has a zero Index, and so
// is never valid while any snapshot exists. Such a node performs one full
// restore on its first startup after upgrading.
//
// The CRC32 is deliberately not checked here: calculating it means reading the
// whole file, so that check is made separately.
func (f *FileFingerprint) ValidFor(mt time.Time, sz int64, index uint64) bool {
	return f.ModTime.Equal(mt) && f.Size == sz && f.Index >= index
}
