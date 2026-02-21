package zstd

import (
	"encoding/binary"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Compressor reads from an io.Reader, compresses the data using zstd, and
// provides the compressed output via its own Read method. The output is
// prefixed with a uint64 (big-endian) containing the uncompressed payload
// size. The corresponding Decompressor uses this to limit how many
// decompressed bytes it returns, so that it never asks the zstd decoder
// to probe for a subsequent frame (which would block on a network stream).
type Compressor struct {
	pr *io.PipeReader

	nRx int64
	nTx int64
}

// NewCompressor returns an instantiated Compressor that reads from r and
// compresses the data using zstd. The uncompressedSize parameter is written
// as an 8-byte big-endian header before the compressed stream.
func NewCompressor(r io.Reader, uncompressedSize int64) *Compressor {
	pr, pw := io.Pipe()

	enc, _ := zstd.NewWriter(pw, zstd.WithEncoderLevel(zstd.SpeedFastest))

	c := &Compressor{
		pr: pr,
	}

	go func() {
		// Write the uncompressed size header.
		var hdr [8]byte
		binary.BigEndian.PutUint64(hdr[:], uint64(uncompressedSize))
		if _, err := pw.Write(hdr[:]); err != nil {
			pw.CloseWithError(err)
			return
		}

		// Stream compressed data.
		_, err := io.Copy(enc, r)
		enc.Close()
		pw.CloseWithError(err)
	}()

	return c
}

// Read reads compressed data (header + zstd payload).
func (c *Compressor) Read(p []byte) (int, error) {
	n, err := c.pr.Read(p)
	c.nTx += int64(n)
	return n, err
}

// Close closes the Compressor, releasing resources.
func (c *Compressor) Close() error {
	return c.pr.Close()
}
