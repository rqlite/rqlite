package streamer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc64"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

const version = 1

// VerifyCRC verifies that the given data matches the given CRC.
func VerifyCRC(data, crc []byte) error {
	table := crc64.MakeTable(crc64.ISO)
	// calculate the CRC of the data
	hash := crc64.New(table)
	if _, err := hash.Write(data); err != nil {
		return err
	}
	if !bytes.Equal(hash.Sum(nil), crc) {
		return errors.New("CRC mismatch")
	}
	return nil
}

// Hasher returns a new Hasher that calculates the CRC of the data written to it.
func Hasher() hash.Hash {
	table := crc64.MakeTable(crc64.ISO)
	return crc64.New(table)
}

// Encoder is a type that encodes a set of files into a single stream.
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

// EncodedSize returns the aggregate number of bytes that will be
// returned by all calls to Read.
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

// Decoder is a type that decodes a stream of files , and allows them
// to be read one at a time.
type Decoder struct {
	r              io.Reader
	currFile       *File
	bytesRemaining int64
	header         *Header
}

// NewDecoder returns an instantiated Decoder
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// Open opens the decoder
func (d *Decoder) Open() error {
	lengthBytes := make([]byte, 4)
	_, err := io.ReadFull(d.r, lengthBytes)
	if err != nil {
		return err
	}
	headerLength := binary.BigEndian.Uint32(lengthBytes)

	headerBytes := make([]byte, headerLength)
	_, err = io.ReadFull(d.r, headerBytes)
	if err != nil {
		return err
	}
	d.header = &Header{}
	return proto.Unmarshal(headerBytes, d.header)
}

// Next advances to the next entry in the Decoder. The File.Size determines
// how many bytes can be read for the next file. Any remaining data in the
// current file is automatically discarded. At the end of the file, Next
// returns the error io.EOF.
func (d *Decoder) Next() (*File, error) {
	if d.currFile != nil && d.bytesRemaining > 0 {
		// Discard any remaining data in the current file
		_, err := io.CopyN(io.Discard, d.r, d.bytesRemaining)
		if err != nil {
			return nil, err
		}
	}

	// Any more files?
	if len(d.header.Files) == 0 {
		return nil, io.EOF
	}

	// Get the next file and set the bytes remaining
	d.currFile = d.header.Files[0]
	d.bytesRemaining = d.currFile.Size
	d.header.Files = d.header.Files[1:]
	return d.currFile, nil
}

// Read reads from the current file in the Decoder. It returns (0, io.EOF)
// when it reaches the end of that file, until Next is called to advance
// to the next file.
func (d *Decoder) Read(b []byte) (int, error) {
	if d.currFile == nil || d.bytesRemaining == 0 {
		return 0, io.EOF
	}

	// Limit the number of bytes read to the remaining bytes in the
	// current file.
	if int64(len(b)) > d.bytesRemaining {
		b = b[:d.bytesRemaining]
	}

	n, err := d.r.Read(b)
	if err != nil {
		return n, err
	}

	// Update the bytes remaining for the current file
	d.bytesRemaining -= int64(n)

	// Return EOF only if all bytes of the current file have been read
	if d.bytesRemaining == 0 {
		err = io.EOF
	}

	return n, err
}

// Close the decoder
func (d *Decoder) Close() error {
	return nil
}
