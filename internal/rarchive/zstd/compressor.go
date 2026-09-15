package zstd

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/klauspost/compress/zstd"
)

// DefaultBufferSize is the default buffer size used by the Compressor.
const DefaultBufferSize = 262144

// Compressor reads from an io.Reader, compresses the data using zstd, and
// provides the compressed output via its own Read method. The output is
// prefixed with a uint64 (big-endian) containing the uncompressed payload
// size. The corresponding Decompressor uses this to limit how many
// decompressed bytes it returns, so that it never asks the zstd decoder
// to probe for a subsequent frame (which would block on a network stream).
type Compressor struct {
	r     io.Reader
	bufSz int
	buf   *bytes.Buffer
	enc   *zstd.Encoder

	nRx int64
	nTx int64
}

// NewCompressor returns an instantiated Compressor that reads from r and
// compresses the data using zstd. The uncompressedSize parameter is written
// as an 8-byte big-endian header before the compressed stream. bufSz
// controls how many bytes are read from r per Read call; use
// DefaultBufferSize if unsure.
func NewCompressor(r io.Reader, uncompressedSize int64, bufSz int) (*Compressor, error) {
	buf := new(bytes.Buffer)

	// Write the uncompressed size header into the buffer so it is
	// returned on the first Read call.
	var hdr [8]byte
	binary.BigEndian.PutUint64(hdr[:], uint64(uncompressedSize))
	buf.Write(hdr[:])

	enc, err := zstd.NewWriter(buf, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	return &Compressor{
		r:     r,
		bufSz: bufSz,
		buf:   buf,
		enc:   enc,
	}, nil
}

// Read reads compressed data (header + zstd payload).
func (c *Compressor) Read(p []byte) (n int, err error) {
	if c.buf.Len() == 0 && c.enc != nil {
		nn, err := io.CopyN(c.enc, c.r, int64(c.bufSz))
		c.nRx += nn
		if err != nil {
			// Source exhausted or errored — finalize the frame.
			if err := c.Close(); err != nil {
				return 0, err
			}
			if err != io.EOF {
				return 0, err
			}
		} else if nn > 0 {
			if err := c.enc.Flush(); err != nil {
				return 0, err
			}
		}
	}
	return c.buf.Read(p)
}

// Close closes the Compressor.
func (c *Compressor) Close() error {
	if c.enc == nil {
		return nil
	}
	if err := c.enc.Close(); err != nil {
		return err
	}
	c.enc = nil
	return nil
}
