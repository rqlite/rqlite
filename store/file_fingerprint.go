package store

import (
	"encoding/json"
	"os"
	"time"
)

// FileFingerprint stores a file's modification time and size.
// It can be written to or read from disk as JSON.
type FileFingerprint struct {
	ModTime time.Time `json:"mod_time"`
	Size    int64     `json:"size"`
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
func (f *FileFingerprint) Compare(mt time.Time, sz int64) bool {
	return f.ModTime.Equal(mt) && f.Size == sz
}
