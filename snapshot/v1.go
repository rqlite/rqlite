package snapshot

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
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
