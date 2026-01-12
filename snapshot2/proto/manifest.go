package proto

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/rqlite/rqlite/v9/internal/rsum"
	pb "google.golang.org/protobuf/proto"
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

// WriteTo writes the SnapshotDBFile to the given writer.
func (s *SnapshotDBFile) WriteTo(w io.Writer) (int64, error) {
	data, err := pb.Marshal(s)
	if err != nil {
		return 0, err
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	n1, err := w.Write(lenBuf[:])
	if err != nil {
		return int64(n1), err
	}
	n2, err := w.Write(data)
	return int64(n1 + n2), err
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

// WriteTo writes the SnapshotWALFile to the given writer.
func (s *SnapshotWALFile) WriteTo(w io.Writer) (int64, error) {
	data, err := pb.Marshal(s)
	if err != nil {
		return 0, err
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	n1, err := w.Write(lenBuf[:])
	if err != nil {
		return int64(n1), err
	}
	n2, err := w.Write(data)
	return int64(n1 + n2), err
}

// SnapshotInstall represents the files needed to install a snapshot.
func NewSnapshotInstall(db string, wal ...string) (*SnapshotInstall, error) {
	dbM, err := NewSnapshotDBFileFromFile(db, true)
	if err != nil {
		return nil, err
	}
	si := &SnapshotInstall{
		DbFile: dbM,
	}
	for _, w := range wal {
		walM, err := NewSnapshotWALFileFromFile(w, true)
		if err != nil {
			return nil, err
		}
		si.WalFiles = append(si.WalFiles, walM)
	}
	return si, nil
}

// NewSnapshotManifestFromDB creates a SnapshotManifest containing a DB file manifest entry.
func NewSnapshotManifestFromDB(path string) (*SnapshotManifest, error) {
	dbFile, err := NewSnapshotDBFileFromFile(path, false)
	if err != nil {
		return nil, err
	}
	manifest := &SnapshotManifest{
		FormatVersion: 1,
		Type: &SnapshotManifest_DbPath{
			DbPath: dbFile,
		},
	}
	return manifest, nil
}

// NewSnapshotManifestFromWAL creates a SnapshotManifest containing a WAL file manifest entry.
func NewSnapshotManifestFromWAL(path string) (*SnapshotManifest, error) {
	walFile, err := NewSnapshotWALFileFromFile(path, false)
	if err != nil {
		return nil, err
	}
	manifest := &SnapshotManifest{
		FormatVersion: 1,
		Type: &SnapshotManifest_WalPath{
			WalPath: walFile,
		},
	}
	return manifest, nil
}

// NewSnapshotManifestFromInstall creates a SnapshotManifest containing an Install manifest entry.
func NewSnapshotManifestFromInstall(db string, wal ...string) (*SnapshotManifest, error) {
	si, err := NewSnapshotInstall(db, wal...)
	if err != nil {
		return nil, err
	}
	manifest := &SnapshotManifest{
		FormatVersion: 1,
		Type: &SnapshotManifest_Install{
			Install: si,
		},
	}
	return manifest, nil
}

// WriteTo writes the SnapshotManifest to the given writer.
func (s *SnapshotManifest) WriteTo(w io.Writer) (int64, error) {
	data, err := pb.Marshal(s)
	if err != nil {
		return 0, err
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	n1, err := w.Write(lenBuf[:])
	if err != nil {
		return int64(n1), err
	}
	n2, err := w.Write(data)
	return int64(n1 + n2), err
}
