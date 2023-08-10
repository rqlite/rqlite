package streamer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

const version = 1

type Encoder struct {
	currentReader io.Reader
	files         []string
	fileIndex     int
	header        *Header
	totalSize     int64
	fd            []*os.File
}

// NewEncoder returns an uninitialized Encoder instance.
func NewEncoder() *Encoder {
	return &Encoder{}
}

// Open initializes the Encoder with the given files.
func (e *Encoder) Open(files ...string) error {
	e.files = files
	e.header = &Header{Version: version}
	table := crc64.MakeTable(crc64.ISO)

	for _, file := range files {
		fi, err := os.Stat(file)
		if err != nil {
			return err
		}

		f, err := os.Open(file)
		if err != nil {
			return err
		}

		hash := crc64.New(table)
		if _, err := io.Copy(hash, f); err != nil {
			return err
		}

		if _, err := f.Seek(0, 0); err != nil {
			return err
		}

		e.fd = append(e.fd, f)
		e.header.Files = append(e.header.Files, &File{
			Size: fi.Size(),
			Crc:  hash.Sum(nil),
		})

		e.totalSize += fi.Size()
	}

	marshaledHeader, err := proto.Marshal(e.header)
	if err != nil {
		return err
	}

	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(marshaledHeader)))

	headerBytes := append(lengthBytes, marshaledHeader...)
	e.totalSize += int64(len(headerBytes))
	e.currentReader = bytes.NewBuffer(headerBytes)

	return nil
}

// Read implements the io.Reader interface for the Encoder type.
func (e *Encoder) Read(p []byte) (n int, err error) {
	n, err = e.currentReader.Read(p)
	if err == io.EOF {
		if e.fileIndex < len(e.files) {
			e.currentReader = e.fd[e.fileIndex]
			// Read from the next file, starting at offset n in p
			m, err := e.currentReader.Read(p[n:])

			e.fileIndex++
			return n + m, err
		}
		return n, io.EOF
	}
	return n, err
}

// EncodedSize returns the aggregate number of bytes that will be returned by all calls to Read.
func (e *Encoder) EncodedSize() (int64, error) {
	if e.header == nil {
		return 0, errors.New("encoder not open")
	}
	return e.totalSize, nil
}

// Close implements the io.Closer interface for the Encoder type.
func (e *Encoder) Close() error {
	var firstErr error
	for _, f := range e.fd {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
