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

// WriteTo writes the SnapshotInstall to the given writer.
func (s *SnapshotInstall) WriteTo(w io.Writer) (int64, error) {
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

// NewSnapshotManifestWithInstall creates a SnapshotManifest containing an Install manifest entry.
func NewSnapshotManifestWithInstall(db string, wal ...string) (*SnapshotManifest, error) {
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

// Marshal marshals the SnapshotManifest to a byte slice.
func (s *SnapshotManifest) Marshal() ([]byte, error) {
	return pb.Marshal(s)
}

// Unmarshal unmarshals the SnapshotManifest from a byte slice.
func (s *SnapshotManifest) Unmarshal(data []byte) error {
	return pb.Unmarshal(data, s)
}

// Size returns the size in bytes of the marshaled SnapshotManifest.
func (s *SnapshotManifest) Size() (int, error) {
	data, err := s.Marshal()
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// SizeBE returns the size in bytes of the marshaled SnapshotManifest
// as a big-endian byte slice.
func (s *SnapshotManifest) SizeBE() ([]byte, error) {
	size, err := s.Size()
	if err != nil {
		return nil, err
	}
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(size))
	return sizeBuf[:], nil
}

// TotalSize returns the total size in bytes of all files described in the manifest.
// This is the number of bytes which needs to be read to obtain the manifest marshaled
// as bytes and all associated files.
func (s *SnapshotManifest) TotalSize() (int64, error) {
	// Start with manifest size.
	sz, err := s.Size()
	if err != nil {
		return 0, err
	}
	var total int64 = int64(sz)

	switch t := s.Type.(type) {
	case *SnapshotManifest_DbPath:
		total += int64(t.DbPath.SizeBytes)
	case *SnapshotManifest_WalPath:
		total += int64(t.WalPath.SizeBytes)
	case *SnapshotManifest_Install:
		total += int64(t.Install.DbFile.SizeBytes)
		for _, w := range t.Install.WalFiles {
			total += int64(w.SizeBytes)
		}
	}
	return total, nil
}

// SnapshotManifestReader implements io.ReadCloser for reading a SnapshotManifest
// and its associated files. It sens the marshaled manifest first, followed by the files
// as contiguous data.
type SnapshotManifestReader struct {
	m        *SnapshotManifest
	bPm      []byte
	dbFile   *os.File
	walFiles []*os.File
	readPos  int64
}

// NewSnapshotManifestReader creates a new SnapshotManifestReader for the given manifest.
func NewSnapshotManifestReader(m *SnapshotManifest) *SnapshotManifestReader {
	return &SnapshotManifestReader{
		m: m,
	}
}

func (s *SnapshotManifestReader) Open() error {
	// Marshal the manifest
	buf, err := s.m.Marshal()
	if err != nil {
		return err
	}
	s.bPm = buf

	return nil
}

// Read reads from the SnapshotManifest and its associated files. Calls to Read()
// return data in the following sequence: 4-byte integer, big-endian, indicating the
// size of the marshaled manifest, followed by the marshaled manifest itself, followed by
// the DB file (if any), followed by any WAL files (if any). Once all data has been read,
// Read() returns io.EOF.
func (s *SnapshotManifestReader) Read(p []byte) (n int, err error) {
	return 0, nil
}

// Close closes the SnapshotManifestReader.
func (s *SnapshotManifestReader) Close() error {
	return nil
}
