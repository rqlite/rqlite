package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

const (
	magicString   = "rqlite snapshot"
	headerVersion = uint16(1)
	sizeofHeader  = 24
)

// Header is the header of a snapshot file.
type Header struct {
	Magic            [16]byte
	Version          uint16
	Reserved         [4]byte
	StreamHeaderSize uint16
}

// NewHeader returns a new Header instance.
func NewHeader(snapshotHeaderSize uint16) *Header {
	var h Header
	copy(h.Magic[:], magicString)
	h.Version = headerVersion
	h.StreamHeaderSize = snapshotHeaderSize
	return &h
}

// Encode encodes the Header into a byte slice.
func (h *Header) Encode() []byte {
	buf := make([]byte, sizeofHeader)
	copy(buf[:16], h.Magic[:])
	binary.LittleEndian.PutUint16(buf[16:18], h.Version)
	binary.LittleEndian.PutUint16(buf[20:22], h.StreamHeaderSize)
	return buf
}

// String is a string representation of the Header.
func (h *Header) String() string {
	return fmt.Sprintf("Header{Magic: %s, Version: %d, SnapshotHeaderSize: %d}",
		string(h.Magic[:]), h.Version, h.StreamHeaderSize)
}

// DecodeHeader decodes the Header from a byte slice.
func DecodeHeader(buf []byte) (*Header, error) {
	if len(buf) < sizeofHeader {
		return nil, errors.New("buffer too small")
	}
	var h Header
	copy(h.Magic[:], buf[:16])
	h.Version = binary.LittleEndian.Uint16(buf[16:18])
	h.StreamHeaderSize = binary.LittleEndian.Uint16(buf[20:22])
	if string(h.Magic[:len(magicString)]) != magicString {
		return nil, errors.New("invalid magic string")
	}
	return &h, nil
}

type Stream struct {
	header *Header

	readClosers    []io.ReadCloser
	readClosersIdx int
	totalFileSize  int64
}

func NewIncrementalStream(data []byte) (*Stream, error) {
	strHdr := &StreamHeader{
		Payload: &StreamHeader_IncrementalSnapshot{
			IncrementalSnapshot: &IncrementalSnapshot{
				Data: data,
			},
		},
	}
	strHdrPb, err := proto.Marshal(strHdr)
	if err != nil {
		return nil, err
	}
	hdr := NewHeader(uint16(len(strHdrPb)))
	buf := bytes.NewBuffer(append(hdr.Encode(), strHdrPb...))
	return &Stream{
		header:      hdr,
		readClosers: []io.ReadCloser{io.NopCloser(buf)},
	}, nil
}

func NewFullStream(files ...string) (*Stream, error) {
	if len(files) == 0 {
		return nil, errors.New("no files provided")
	}

	str := &Stream{}

	// First file must be the SQLite database file.
	fi, err := os.Stat(files[0])
	if err != nil {
		return nil, err
	}
	dbDataInfo := &FullSnapshot_DataInfo{
		Size: fi.Size(),
	}
	str.totalFileSize += fi.Size()

	// Rest, if any, are WAL files.
	walDataInfos := make([]*FullSnapshot_DataInfo, len(files)-1)
	for i := 1; i < len(files); i++ {
		fi, err := os.Stat(files[i])
		if err != nil {
			return nil, err
		}
		walDataInfos[i-1] = &FullSnapshot_DataInfo{
			Size: fi.Size(),
		}
		str.totalFileSize += fi.Size()
	}
	strHdr := &StreamHeader{
		Payload: &StreamHeader_FullSnapshot{
			FullSnapshot: &FullSnapshot{
				Db:   dbDataInfo,
				Wals: walDataInfos,
			},
		},
	}

	strHdrPb, err := proto.Marshal(strHdr)
	if err != nil {
		return nil, err
	}

	str.header = NewHeader(uint16(len(strHdrPb)))
	buf := bytes.NewBuffer(append(str.header.Encode(), strHdrPb...))

	str.readClosers = append(str.readClosers, io.NopCloser(buf))
	for _, file := range files {
		fd, err := os.Open(file)
		if err != nil {
			for _, rc := range str.readClosers {
				rc.Close() // Ignore the error during cleanup
			}
			return nil, err
		}
		str.readClosers = append(str.readClosers, fd)
	}
	return str, nil
}

func (s *Stream) Size() int64 {
	return int64(sizeofHeader) + int64(s.header.StreamHeaderSize) + s.totalFileSize
}

func (s *Stream) Read(p []byte) (n int, err error) {
	if s.readClosersIdx >= len(s.readClosers) {
		return 0, io.EOF
	}

	n, err = s.readClosers[s.readClosersIdx].Read(p)
	if err != nil {
		if err == io.EOF {
			s.readClosersIdx++
			if s.readClosersIdx < len(s.readClosers) {
				err = nil
			}
		}
	}
	return n, err
}

func (s *Stream) Close() error {
	for _, r := range s.readClosers {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
}
