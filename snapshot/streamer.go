package snapshot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/rqlite/rqlite/v10/internal/rsum"
	"github.com/rqlite/rqlite/v10/snapshot/proto"
	pb "google.golang.org/protobuf/proto"
)

const (
	// HeaderSizeLen is the length in bytes of the SnapshotHeader length prefix.
	HeaderSizeLen = 4

	// MaxHeaderSize is the largest marshaled SnapshotHeader, in bytes, which will
	// be read from a snapshot stream. A header holds a few bytes for each file in
	// the snapshot, so this is far larger than any real header.
	MaxHeaderSize = 1024 * 1024
)

// NewHeaderFromFile creates a new Header for the given file path. If crc32 is true,
// the CRC32 checksum of the file is calculated and included in the Header.
func NewHeaderFromFile(path string, crc32 bool) (*proto.Header, error) {
	if path == "" {
		return nil, fmt.Errorf("path must be non-empty")
	}
	h := &proto.Header{}
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

// NewHeaderFromChecksummedFile creates a new Header using a pre-loaded ChecksummedFile.
// The file size is read from disk; the CRC32 comes from the ChecksummedFile, except
// when the file's sidecar is Disabled — in that case the CRC32 is computed live so
// that the receiver's verification still has a real checksum to compare against.
func NewHeaderFromChecksummedFile(hf *ChecksummedFile) (*proto.Header, error) {
	if hf.Path == "" {
		return nil, fmt.Errorf("path must be non-empty")
	}
	info, err := os.Stat(hf.Path)
	if err != nil {
		return nil, err
	}
	crc := hf.CRC32
	if hf.sidecar != nil && hf.sidecar.Disabled {
		crc, err = rsum.CRC32(hf.Path)
		if err != nil {
			return nil, err
		}
	}
	return &proto.Header{
		SizeBytes: uint64(info.Size()),
		Crc32:     crc,
	}, nil
}

// NewChecksummedSnapshotHeader creates a new SnapshotHeader using pre-loaded ChecksummedFile values.
// dbFile must have a non-empty Path. walFiles may be empty. The header payload is always
// FullSnapshot.
func NewChecksummedSnapshotHeader(dbFile *ChecksummedFile, walFiles ...*ChecksummedFile) (*proto.SnapshotHeader, error) {
	if dbFile.Path == "" {
		return nil, fmt.Errorf("dbFile.Path must be non-empty")
	}

	sh := &proto.SnapshotHeader{
		FormatVersion: 1,
	}

	dbHeader, err := NewHeaderFromChecksummedFile(dbFile)
	if err != nil {
		return nil, err
	}
	full := &proto.FullSnapshot{
		DbHeader: dbHeader,
	}
	for _, w := range walFiles {
		wh, err := NewHeaderFromChecksummedFile(w)
		if err != nil {
			return nil, err
		}
		full.WalHeaders = append(full.WalHeaders, wh)
	}
	sh.Payload = &proto.SnapshotHeader_Full{Full: full}
	return sh, nil
}

// NewSnapshotHeader creates a new SnapshotHeader for the given DB and WAL file paths.
// dbPath must be non-empty. walPaths may be empty. The header payload is always
// FullSnapshot.
func NewSnapshotHeader(dbPath string, walPaths ...string) (*proto.SnapshotHeader, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("dbPath must be non-empty")
	}

	sh := &proto.SnapshotHeader{
		FormatVersion: 1,
	}

	dbHeader, err := NewHeaderFromFile(dbPath, true)
	if err != nil {
		return nil, err
	}
	full := &proto.FullSnapshot{
		DbHeader: dbHeader,
	}
	for _, w := range walPaths {
		wh, err := NewHeaderFromFile(w, true)
		if err != nil {
			return nil, err
		}
		full.WalHeaders = append(full.WalHeaders, wh)
	}
	sh.Payload = &proto.SnapshotHeader_Full{Full: full}
	return sh, nil
}

// marshalSnapshotHeader marshals the SnapshotHeader to a byte slice.
func marshalSnapshotHeader(s *proto.SnapshotHeader) ([]byte, error) {
	return pb.Marshal(s)
}

// UnmarshalSnapshotHeader unmarshals a SnapshotHeader from the given byte slice.
func UnmarshalSnapshotHeader(data []byte) (*proto.SnapshotHeader, error) {
	sh := &proto.SnapshotHeader{}
	if err := pb.Unmarshal(data, sh); err != nil {
		return nil, err
	}
	return sh, nil
}

// snapshotHeaderPayloadSize returns the total size of the marshaled header and
// of all files described by the header. This is the number of bytes which needs
// to be read to obtain the header marshaled as bytes and all associated file data.
func snapshotHeaderPayloadSize(s *proto.SnapshotHeader) (int64, error) {
	data, err := pb.Marshal(s)
	if err != nil {
		return 0, err
	}
	total := int64(len(data))

	if p, ok := s.Payload.(*proto.SnapshotHeader_Full); ok {
		if p.Full.DbHeader != nil {
			total += int64(p.Full.DbHeader.SizeBytes)
		}
		for _, w := range p.Full.WalHeaders {
			total += int64(w.SizeBytes)
		}
	}
	return total, nil
}

// SnapshotStreamer implements io.ReadCloser for streaming a snapshot's
// data, including the header and associated files. The expected sequence
// of data returned by Read() is
//   - 4-byte integer, big-endian, indicating the size of the marshaled header
//   - the marshaled header itself
//   - the DB file (if any)
//   - any WAL files
type SnapshotStreamer struct {
	dbPath   string
	walPaths []string
	currWAL  int

	hdr *proto.SnapshotHeader

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

// NewChecksummedSnapshotStreamer creates a new SnapshotStreamer using pre-loaded ChecksummedFile values,
// avoiding redundant CRC32 computation for files that already have checksums on disk.
func NewChecksummedSnapshotStreamer(dbFile *ChecksummedFile, walFiles ...*ChecksummedFile) (*SnapshotStreamer, error) {
	sh, err := NewChecksummedSnapshotHeader(dbFile, walFiles...)
	if err != nil {
		return nil, err
	}
	walPaths := make([]string, len(walFiles))
	for i, w := range walFiles {
		walPaths[i] = w.Path
	}
	return &SnapshotStreamer{
		dbPath:   dbFile.Path,
		walPaths: walPaths,
		hdr:      sh,
	}, nil
}

// Open opens the SnapshotStreamer.
func (s *SnapshotStreamer) Open() (retErr error) {
	defer func() {
		if retErr != nil {
			if s.dbFD != nil {
				s.dbFD.Close()
			}
			for _, w := range s.walFDs {
				w.Close()
			}
		}
	}()

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

	hdrBuf, err := marshalSnapshotHeader(s.hdr)
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
	var firstErr error
	if s.dbFD != nil {
		if err := s.dbFD.Close(); err != nil {
			firstErr = err
		}
	}
	for _, w := range s.walFDs {
		if err := w.Close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Len returns the total number of bytes that will be read from the SnapshotStreamer
// before EOF is reached.
func (s *SnapshotStreamer) Len() (int64, error) {
	sz, err := snapshotHeaderPayloadSize(s.hdr)
	if err != nil {
		return 0, err
	}
	return HeaderSizeLen + sz, nil
}
