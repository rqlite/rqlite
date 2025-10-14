package index

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
)

const (
	magicNumber uint32 = 0x01415152 // "RAQ\x01"
	recordSize  uint32 = 16         // bytes
)

var (
	// ErrEmptyFile indicates that the index file is empty.
	ErrEmptyFile = errors.New("index file is empty")

	// ErrCorrupt indicates that the index file is corrupt.
	ErrCorrupt = errors.New("index file is corrupt")

	// ErrBadChecksum indicates that the index file has a bad checksum.
	ErrBadChecksum = errors.New("index file has bad checksum")
)

// IndexFile represents a file that contains a single uint64 value, with
// a magic number and CRC32 checksum for integrity.
type IndexFile struct {
	fd *os.File
}

// NewIndexFile opens or creates an index file at the given path.
func NewIndexFile(path string) (*IndexFile, error) {
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	return &IndexFile{fd: fd}, nil
}

// Path returns the path of the index file.
func (i *IndexFile) Path() string {
	return i.fd.Name()
}

// WriteValue writes the given uint64 value to the index file.
// It does not call fsync.
func (i *IndexFile) WriteValue(v uint64) error {
	var buf [recordSize]byte
	binary.LittleEndian.PutUint32(buf[0:4], magicNumber)
	binary.LittleEndian.PutUint64(buf[4:12], v)
	binary.LittleEndian.PutUint32(buf[12:16], crc32.ChecksumIEEE(buf[:12]))
	_, err := i.fd.WriteAt(buf[:], 0)
	if err != nil {
		return err
	}
	return i.fd.Truncate(int64(recordSize)) // make size stable after first create
}

// ReadValue reads the uint64 value from the index file.
// It returns an error if the file is invalid or corrupted.
func (i *IndexFile) ReadValue() (uint64, error) {
	info, err := i.fd.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() == 0 {
		return 0, ErrEmptyFile
	}

	var buf [recordSize]byte
	if _, err := i.fd.ReadAt(buf[:], 0); err != nil {
		return 0, err
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != magicNumber {
		return 0, ErrCorrupt
	}
	want := binary.LittleEndian.Uint32(buf[12:16])
	got := crc32.ChecksumIEEE(buf[:12])
	if got != want {
		return 0, ErrBadChecksum
	}
	return binary.LittleEndian.Uint64(buf[4:12]), nil
}

// Close closes the index file.
func (i *IndexFile) Close() error {
	return i.fd.Close()
}
