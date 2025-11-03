package store

import (
	"encoding/json"
	"os"
	"time"
)

// FileFingerprint stores a file's modification time and size.
// It can be written to or read from disk as JSON. Older versions
// may not have the CRC32 field, so it is optional in comparisons.
type FileFingerprint struct {
	ModTime time.Time `json:"mod_time"`
	Size    int64     `json:"size"`
	CRC32   uint32    `json:"crc32,omitempty"`
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
