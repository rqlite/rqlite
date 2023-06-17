package snapshot

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"unsafe"
)

// V1Encoder creates a new V1 snapshot.
type V1Encoder struct {
	data []byte
}

// NewV1Encoder returns an initialized V1 encoder
func NewV1Encoder(b []byte) *V1Encoder {
	return &V1Encoder{
		data: b,
	}
}

// WriteTo writes the snapshot to the given writer.
func (v *V1Encoder) WriteTo(w io.Writer) (int64, error) {
	var totalN int64

	// Indicate that the data is compressed by writing max uint64 value first.
	if err := binary.Write(w, binary.LittleEndian, uint64(math.MaxUint64)); err != nil {
		return 0, fmt.Errorf("failed to write max uint64: %w", err)
	}
	totalN += 8 // 8 bytes for uint64

	// Get compressed copy of data.
	cdata, err := v.compressedData()
	if err != nil {
		return 0, fmt.Errorf("failed to get compressed data: %w", err)
	}

	// Write size of compressed data.
	if err := binary.Write(w, binary.LittleEndian, uint64(len(cdata))); err != nil {
		return 0, fmt.Errorf("failed to write compressed data size: %w", err)
	}
	totalN += 8 // 8 bytes for uint64

	if len(cdata) != 0 {
		// Write compressed data.
		n, err := w.Write(cdata)
		if err != nil {
			return 0, fmt.Errorf("failed to write compressed data: %w", err)
		}
		totalN += int64(n)
	}

	return totalN, nil
}

func (v *V1Encoder) compressedData() ([]byte, error) {
	if v.data == nil {
		return nil, nil
	}

	var buf bytes.Buffer
	gz, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}

	if _, err := gz.Write(v.data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// V1Decoder reads a V1 snapshot.
type V1Decoder struct {
	r io.Reader
}

// NewV1Decoder returns an initialized V1 decoder
func NewV1Decoder(r io.Reader) *V1Decoder {
	return &V1Decoder{
		r: r,
	}
}

// WriteTo writes the decoded snapshot data to the given writer.
func (v *V1Decoder) WriteTo(w io.Writer) (int64, error) {
	var uint64Size uint64
	inc := int64(unsafe.Sizeof(uint64Size))

	// Read all the data into RAM, since we have to decode known-length
	// chunks of various forms.
	var offset int64
	b, err := ioutil.ReadAll(v.r)
	if err != nil {
		return 0, fmt.Errorf("readall: %s", err)
	}

	// Get size of data, checking for compression.
	compressed := false
	sz, err := readUint64(b[offset : offset+inc])
	if err != nil {
		return 0, fmt.Errorf("read compression check: %s", err)
	}
	offset = offset + inc

	if sz == math.MaxUint64 {
		compressed = true
		// Data is actually compressed, read actual size next.
		sz, err = readUint64(b[offset : offset+inc])
		if err != nil {
			return 0, fmt.Errorf("read compressed size: %s", err)
		}
		offset = offset + inc
	}

	// Now read in the data, decompressing if necessary.
	var totalN int64
	if sz > 0 {
		if compressed {
			gz, err := gzip.NewReader(bytes.NewReader(b[offset : offset+int64(sz)]))
			if err != nil {
				return 0, err
			}

			n, err := io.Copy(w, gz)
			if err != nil {
				return 0, fmt.Errorf("data decompress: %s", err)
			}
			totalN += n

			if err := gz.Close(); err != nil {
				return 0, err
			}
		} else {
			// write the data directly
			n, err := w.Write(b[offset : offset+int64(sz)])
			if err != nil {
				return 0, fmt.Errorf("uncompressed data write: %s", err)
			}
			totalN += int64(n)
		}
	}
	return totalN, nil
}

func readUint64(b []byte) (uint64, error) {
	var sz uint64
	if err := binary.Read(bytes.NewReader(b), binary.LittleEndian, &sz); err != nil {
		return 0, err
	}
	return sz, nil
}
