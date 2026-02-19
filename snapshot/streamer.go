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

// NewSnapshotHeader creates a new SnapshotHeader for the given DB and WAL file paths.
// dbPath may be empty, in which case no DB header is included. walPaths may be empty.
//
// This function enforces certain invariants:
//   - At least one of dbPath or walPaths must be non-empty.
//   - If only WALs are provided, then only one WAL is allowed. This represents an
//     incremental snapshot.
//
// When dbPath is non-empty, the header payload is FullSnapshot. When only a single
// WAL path is provided, the payload is IncrementalSnapshot.
func NewSnapshotHeader(dbPath string, walPaths ...string) (*proto.SnapshotHeader, error) {
	if dbPath == "" && len(walPaths) == 0 {
		return nil, fmt.Errorf("at least one of dbPath or walPaths must be provided")
	}

	if dbPath == "" && len(walPaths) > 1 {
		return nil, fmt.Errorf("when dbPath is empty, only one WAL path is allowed")
	}

	sh := &proto.SnapshotHeader{
		FormatVersion: 1,
	}

	if dbPath != "" {
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
	} else {
		wh, err := NewHeaderFromFile(walPaths[0], true)
		if err != nil {
			return nil, err
		}
		sh.Payload = &proto.SnapshotHeader_Incremental{
			Incremental: &proto.IncrementalSnapshot{
				WalHeader: wh,
			},
		}
	}
	return sh, nil
}

// NewIncrementalFileSnapshotHeader creates a new SnapshotHeader for a local
// WAL file that should be moved (not streamed) into the snapshot directory.
// No data follows this header when written to a Sink.
func NewIncrementalFileSnapshotHeader(walPath string) (*proto.SnapshotHeader, error) {
	if walPath == "" {
		return nil, fmt.Errorf("walPath must be non-empty")
	}
	return &proto.SnapshotHeader{
		FormatVersion: 1,
		Payload: &proto.SnapshotHeader_IncrementalFile{
			IncrementalFile: &proto.IncrementalFileSnapshot{
				WalPaths: []string{walPath},
			},
		},
	}, nil
}

// NewNoopSnapshotHeader creates a new SnapshotHeader for a noop snapshot.
// A noop snapshot is created when a snapshot is triggered but no WAL data
// is available. No data follows this header when written to a Sink.
func NewNoopSnapshotHeader() *proto.SnapshotHeader {
	return &proto.SnapshotHeader{
		FormatVersion: 1,
		Payload: &proto.SnapshotHeader_Noop{
			Noop: &proto.NoopSnapshot{},
		},
	}
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
	var total int64 = int64(len(data))

	switch p := s.Payload.(type) {
	case *proto.SnapshotHeader_Full:
		if p.Full.DbHeader != nil {
			total += int64(p.Full.DbHeader.SizeBytes)
		}
		for _, w := range p.Full.WalHeaders {
			total += int64(w.SizeBytes)
		}
	case *proto.SnapshotHeader_Incremental:
		if p.Incremental.WalHeader != nil {
			total += int64(p.Incremental.WalHeader.SizeBytes)
		}
	case *proto.SnapshotHeader_IncrementalFile:
		// No file data follows this header type.
	case *proto.SnapshotHeader_Noop:
		// No file data follows this header type.
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
	return HeaderSizeLen + int64(sz), nil
}

// SnapshotPathStreamer implements io.ReadCloser for streaming a snapshot's
// data, where the snapshot data is a single WAL file. It creates the SnapshotHeader
// on the fly based on the provided WAL file path, and then streams the header
// to readers. The path is set in the field in the Header, and no more data
// follows the header. Instead the client is expected to read the header,
// determine the file path, and then move the file directly.
type SnapshotPathStreamer struct {
	walPath string
	hdr     *proto.SnapshotHeader
	multiR  io.Reader
}

// NewSnapshotPathStreamer creates a new SnapshotPathStreamer for the given WAL file path.
func NewSnapshotPathStreamer(walPath string) (*SnapshotPathStreamer, error) {
	sh, err := NewIncrementalFileSnapshotHeader(walPath)
	if err != nil {
		return nil, err
	}

	hdrBuf, err := marshalSnapshotHeader(sh)
	if err != nil {
		return nil, err
	}
	hdrBufR := bytes.NewReader(hdrBuf)
	var hdrLenBEBuf [HeaderSizeLen]byte
	binary.BigEndian.PutUint32(hdrLenBEBuf[:], uint32(len(hdrBuf)))
	hdrLenBufR := bytes.NewReader(hdrLenBEBuf[:])

	var readers []io.Reader
	readers = append(readers, hdrLenBufR)
	readers = append(readers, hdrBufR)
	multiR := io.MultiReader(readers...)

	return &SnapshotPathStreamer{
		walPath: walPath,
		hdr:     sh,
		multiR:  multiR,
	}, nil
}

// Read reads from the SnapshotPathStreamer. The data returned by Read() is
// the 4-byte integer, big-endian, indicating the size of the marshaled header,
// followed by the marshaled header itself. No more data follows the header,
// and once the header has been fully read, Read() returns io.EOF.
func (s *SnapshotPathStreamer) Read(p []byte) (n int, err error) {
	return s.multiR.Read(p)
}

// Close closes the SnapshotPathStreamer. Since no files are opened by this struct,
// Close() is a no-op.
func (s *SnapshotPathStreamer) Close() error {
	return nil
}

// SnapshotNoopStreamer implements io.ReadCloser for streaming a noop snapshot.
// A noop snapshot contains only the header (no file data). It is used when a
// snapshot is triggered but no WAL data is available.
type SnapshotNoopStreamer struct {
	hdr    *proto.SnapshotHeader
	multiR io.Reader
}

// NewSnapshotNoopStreamer creates a new SnapshotNoopStreamer.
func NewSnapshotNoopStreamer() (*SnapshotNoopStreamer, error) {
	sh := NewNoopSnapshotHeader()

	hdrBuf, err := marshalSnapshotHeader(sh)
	if err != nil {
		return nil, err
	}
	hdrBufR := bytes.NewReader(hdrBuf)
	var hdrLenBEBuf [HeaderSizeLen]byte
	binary.BigEndian.PutUint32(hdrLenBEBuf[:], uint32(len(hdrBuf)))
	hdrLenBufR := bytes.NewReader(hdrLenBEBuf[:])

	var readers []io.Reader
	readers = append(readers, hdrLenBufR)
	readers = append(readers, hdrBufR)
	multiR := io.MultiReader(readers...)

	return &SnapshotNoopStreamer{
		hdr:    sh,
		multiR: multiR,
	}, nil
}

// Read reads from the SnapshotNoopStreamer. The data returned by Read() is
// the 4-byte integer, big-endian, indicating the size of the marshaled header,
// followed by the marshaled header itself. No more data follows the header,
// and once the header has been fully read, Read() returns io.EOF.
func (s *SnapshotNoopStreamer) Read(p []byte) (n int, err error) {
	return s.multiR.Read(p)
}

// Close closes the SnapshotNoopStreamer. Since no files are opened by this
// struct, Close() is a no-op.
func (s *SnapshotNoopStreamer) Close() error {
	return nil
}
