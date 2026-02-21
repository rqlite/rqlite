package zstd

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Compressor reads from an io.Reader, compresses the data using zstd, and
// provides the compressed output via its own Read method. The output is
// prefixed with a uint64 (big-endian) containing the compressed payload
// size, so that the corresponding Decompressor can limit reads to exactly
// the compressed frame — preventing the zstd decoder from blocking while
// probing for a subsequent frame on a network stream.
type Compressor struct {
	buf *bytes.Reader

	nRx int64
	nTx int64
}

// NewCompressor returns an instantiated Compressor that reads from r and
// compresses the data using zstd.
func NewCompressor(r io.Reader) *Compressor {
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))

	// Read all source data and compress.
	src, _ := io.ReadAll(r)
	compressed := enc.EncodeAll(src, nil)

	// Build the output: 8-byte big-endian length + compressed payload.
	out := make([]byte, 8+len(compressed))
	binary.BigEndian.PutUint64(out[:8], uint64(len(compressed)))
	copy(out[8:], compressed)

	return &Compressor{
		buf: bytes.NewReader(out),
		nRx: int64(len(src)),
	}
}

// Read reads compressed data (length prefix + zstd payload).
func (c *Compressor) Read(p []byte) (int, error) {
	n, err := c.buf.Read(p)
	c.nTx += int64(n)
	return n, err
}

// Close is a no-op provided for interface compatibility.
func (c *Compressor) Close() error {
	return nil
}
