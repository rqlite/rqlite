package proto

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/rqlite/rqlite/v9/internal/rsum"
	pb "google.golang.org/protobuf/proto"
)

const (
	// HeaderLen is the length in bytes of the SnapshotHeader length prefix.
	HeaderSizeLen = 4
)

// NewHeaderFromFile creates a new Header for the given file path. If crc32 is true,
// the CRC32 checksum of the file is calculated and included in the Header.
func NewHeaderFromFile(path string, crc32 bool) (*Header, error) {
	if path == "" {
		return nil, nil
	}
	h := &Header{}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	h.SizeBytes = uint64(info.Size())

	if crc32 {
		crc, err := rsum.CRC32(path)
		if err != nil {
			return nil, err
		}
		h.Crc32 = crc
	}
	return h, nil
}

// NewSnapshotHeader creates a new SnapshotHeader for the given DB and WAL file paths.
func NewSnapshotHeader(dbPath string, walPaths ...string) (*SnapshotHeader, error) {
	dhHeader, err := NewHeaderFromFile(dbPath, true)
	if err != nil {
		return nil, err
	}
	sh := &SnapshotHeader{
		DbHeader: dhHeader,
	}
	for _, w := range walPaths {
		wh, err := NewHeaderFromFile(w, true)
		if err != nil {
			return nil, err
		}
		sh.WalHeaders = append(sh.WalHeaders, wh)
	}
	return sh, nil
}

// Size returns the size in bytes of the marshaled SnapshotHeader.
func (s *SnapshotHeader) Size() (int, error) {
	data, err := pb.Marshal(s)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// TotalSize returns the total size of the marshaled header and of all files
// described by the header. This is the number of bytes which needs to be
// read to obtain the header marshaled as bytes and all associated file data.
func (s *SnapshotHeader) TotalSize() (int64, error) {
	// Start with header size.
	sz, err := s.Size()
	if err != nil {
		return 0, err
	}
	var total int64 = int64(sz)

	if s.DbHeader != nil {
		total += int64(s.DbHeader.SizeBytes)
	}
	for _, w := range s.WalHeaders {
		total += int64(w.SizeBytes)
	}
	return total, nil
}

// Marshal marshals the SnapshotHeader to a byte slice.
func (s *SnapshotHeader) Marshal() ([]byte, error) {
	return pb.Marshal(s)
}

// SnapshotStreamer implements io.ReadCloser for streaming a snapshot's
// data, including the header and associated files.
type SnapshotStreamer struct {
	dbPath   string
	walPaths []string
	currWAL  int

	hdr *SnapshotHeader

	dbFD   *os.File
	walFDs []*os.File

	multiR io.Reader
}

// NewSnapshotStreamer creates a new SnapshotStreamer for the given DB and WAL file paths.
func NewSnapshotStreamer(dbPath string, walPaths ...string) (*SnapshotStreamer, error) {
	sh, err := NewSnapshotHeader(dbPath, walPaths...)
	if err != nil {
		return nil, err
	}
	return &SnapshotStreamer{
		dbPath:   dbPath,
		walPaths: walPaths,
		hdr:      sh,
	}, nil
}

// Open opens the SnapshotStreamer.
func (s *SnapshotStreamer) Open() error {
	var err error
	if s.dbPath != "" {
		s.dbFD, err = os.Open(s.dbPath)
		if err != nil {
			return err
		}
	}

	for _, w := range s.walPaths {
		walFD, err := os.Open(w)
		if err != nil {
			return err
		}
		s.walFDs = append(s.walFDs, walFD)
	}

	// Build the multi-reader which will return data in the correct order.

	hdrBuf, err := s.hdr.Marshal()
	if err != nil {
		return err
	}
	hdrBufR := bytes.NewReader(hdrBuf)

	var hdrLenBEBuf [HeaderSizeLen]byte
	binary.BigEndian.PutUint32(hdrLenBEBuf[:], uint32(len(hdrBuf)))
	hdrLenBufR := bytes.NewReader(hdrLenBEBuf[:])

	var readers []io.Reader
	readers = append(readers, hdrLenBufR)
	readers = append(readers, hdrBufR)
	if s.dbFD != nil {
		readers = append(readers, s.dbFD)
	}

	for _, w := range s.walFDs {
		readers = append(readers, w)
	}
	s.multiR = io.MultiReader(readers...)

	return nil
}

// Read reads from the SnapshotStreamer and its associated files. Calls to Read()
// return data in the following sequence: 4-byte integer, big-endian, indicating the
// size of the marshaled header, followed by the marshaled header itself, followed by
// the DB file (if any), followed by any WAL files. Once all data has been read, Read()
// returns io.EOF.
func (s *SnapshotStreamer) Read(p []byte) (n int, err error) {
	return s.multiR.Read(p)
}

// Close closes the SnapshotStreamer.
func (s *SnapshotStreamer) Close() error {
	if s.dbFD != nil {
		s.dbFD.Close()
	}
	for _, w := range s.walFDs {
		w.Close()
	}
	return nil
}

// Len returns the total number of bytes that will be read from the SnapshotStreamer
// before EOF is reached.
func (s *SnapshotStreamer) Len() (int64, error) {
	sz, err := s.hdr.TotalSize()
	if err != nil {
		return 0, err
	}
	return HeaderSizeLen + int64(sz), nil
}
