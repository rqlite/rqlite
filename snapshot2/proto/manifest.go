package proto

import (
	"os"

	"github.com/rqlite/rqlite/v9/internal/rsum"
)

// NewSnapshotDBFileFromFile creates a SnapshotDBFile manifest entry from the given file path.
func NewSnapshotDBFileFromFile(path string, crc32 bool) (*SnapshotDBFile, error) {
	s := &SnapshotDBFile{}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	s.SizeBytes = uint64(info.Size())

	if crc32 {
		crc, err := rsum.CRC32(path)
		if err != nil {
			return nil, err
		}
		s.Crc32 = crc
	}
	return s, nil
}

// NewSnapshotWALFileFromFile creates a SnapshotWALFile manifest entry from the given file path.
func NewSnapshotWALFileFromFile(path string, crc32 bool) (*SnapshotWALFile, error) {
	s := &SnapshotWALFile{}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	s.SizeBytes = uint64(info.Size())

	if crc32 {
		crc, err := rsum.CRC32(path)
		if err != nil {
			return nil, err
		}
		s.Crc32 = crc
	}
	return s, nil
}
