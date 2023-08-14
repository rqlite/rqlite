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
	Magic              [16]byte
	Version            uint16
	Reserved           [4]byte
	SnapshotHeaderSize uint16
}

// NewHeader returns a new Header instance.
func NewHeader(snapshotHeaderSize uint16) *Header {
	var h Header
	copy(h.Magic[:], magicString)
	h.Version = headerVersion
	h.SnapshotHeaderSize = snapshotHeaderSize
	return &h
}

// Encode encodes the Header into a byte slice.
func (h *Header) Encode() []byte {
	buf := make([]byte, sizeofHeader)
	copy(buf[:16], h.Magic[:])
	binary.LittleEndian.PutUint16(buf[16:18], h.Version)
	binary.LittleEndian.PutUint16(buf[20:22], h.SnapshotHeaderSize)
	return buf
}

// String is a string representation of the Header.
func (h *Header) String() string {
	return fmt.Sprintf("Header{Magic: %s, Version: %d, SnapshotHeaderSize: %d}",
		string(h.Magic[:]), h.Version, h.SnapshotHeaderSize)
}

// DecodeHeader decodes the Header from a byte slice.
func DecodeHeader(buf []byte) (*Header, error) {
	if len(buf) < sizeofHeader {
		return nil, errors.New("buffer too small")
	}
	var h Header
	copy(h.Magic[:], buf[:16])
	h.Version = binary.LittleEndian.Uint16(buf[16:18])
	h.SnapshotHeaderSize = binary.LittleEndian.Uint16(buf[20:22])
	if string(h.Magic[:len(magicString)]) != magicString {
		return nil, errors.New("invalid magic string")
	}
	return &h, nil
}

// IncrementalEncoder is a type that encodes a byte slice (presumbly a WAL) into a readable
// stream.
type IncrementalEncoder struct {
	header *Header
	buf    *bytes.Buffer
}

// NewIncrementalEncoderreturns an uninitialized IncrementalEncoder instance.
func NewIncrementalEncoder() *IncrementalEncoder {
	return &IncrementalEncoder{}
}

// Open initializes the WALEncoder with the given byte slice.
func (i *IncrementalEncoder) Open(data []byte) error {
	fsmSnap := &FSMSnapshot{
		Payload: &FSMSnapshot_IncrementalSnapshot{
			IncrementalSnapshot: &IncrementalSnapshot{
				Data: data,
			},
		},
	}
	fsmPb, err := proto.Marshal(fsmSnap)
	if err != nil {
		return err
	}
	i.header = NewHeader(uint16(len(fsmPb)))

	i.buf = bytes.NewBuffer(append(i.header.Encode(), fsmPb...))
	return nil
}

// Size returns the number of bytes that will be returned when the WALEncoder
// is completely read.
func (w *IncrementalEncoder) Size() int64 {
	return int64(sizeofHeader) + int64(w.header.SnapshotHeaderSize)
}

// Read reads bytes from the WALEncoder.
func (w *IncrementalEncoder) Read(p []byte) (n int, err error) {
	return w.buf.Read(p)
}

// Close closes the WALEncoder.
func (w *IncrementalEncoder) Close() error {
	return nil
}

// FullEncoder is a type that encodes a SQLite database and WAL files into a readable
// stream.
type FullEncoder struct {
	header         *Header
	totalFileSize  int64
	readClosers    []io.ReadCloser
	readClosersIdx int
}

// NewFullEncoder returns an uninitialized FullEncoder instance.
func NewFullEncoder() *FullEncoder {
	return &FullEncoder{}
}

// Open initializes the FullEncoder with the given files
func (f *FullEncoder) Open(files ...string) error {
	if len(files) == 0 {
		return errors.New("no files provided")
	}

	// First file must be the SQLite database file.
	fi, err := os.Stat(files[0])
	if err != nil {
		return err
	}
	dbDataInfo := &FullSnapshot_DataInfo{
		Size: fi.Size(),
	}
	f.totalFileSize += fi.Size()

	// Rest, if any, are WAL files.
	walDataInfos := make([]*FullSnapshot_DataInfo, len(files)-1)
	for i := 1; i < len(files); i++ {
		fi, err := os.Stat(files[i])
		if err != nil {
			return err
		}
		walDataInfos[i-1] = &FullSnapshot_DataInfo{
			Size: fi.Size(),
		}
		f.totalFileSize += fi.Size()
	}
	fsmSnap := &FSMSnapshot{
		Payload: &FSMSnapshot_FullSnapshot{
			FullSnapshot: &FullSnapshot{
				Db:   dbDataInfo,
				Wals: walDataInfos,
			},
		},
	}

	fsmPb, err := proto.Marshal(fsmSnap)
	if err != nil {
		return err
	}

	f.header = NewHeader(uint16(len(fsmPb)))
	buf := bytes.NewBuffer(append(f.header.Encode(), fsmPb...))

	f.readClosers = append(f.readClosers, io.NopCloser(buf))
	for _, file := range files {
		fd, err := os.Open(file)
		if err != nil {
			for _, rc := range f.readClosers {
				rc.Close() // Ignore the error during cleanup
			}
			return err
		}
		f.readClosers = append(f.readClosers, fd)
	}
	return nil
}

// Size returns the number of bytes that will be returned when the FullEncoder
// is completely read.
func (f *FullEncoder) Size() int64 {
	return int64(sizeofHeader) + int64(f.header.SnapshotHeaderSize) + f.totalFileSize
}

// Read reads bytes from the FullEncoder.
func (f *FullEncoder) Read(p []byte) (n int, err error) {
	if f.readClosersIdx >= len(f.readClosers) {
		return 0, io.EOF
	}

	n, err = f.readClosers[f.readClosersIdx].Read(p)
	if err != nil {
		if err == io.EOF {
			f.readClosersIdx++
			if f.readClosersIdx < len(f.readClosers) {
				err = nil
			}
		}
	}
	return n, err
}

// Close closes the FullEncoder.
func (f *FullEncoder) Close() error {
	for _, r := range f.readClosers {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
}
