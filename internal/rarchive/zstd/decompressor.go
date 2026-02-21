package zstd

import (
	"encoding/binary"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/rqlite/rqlite/v10/internal/progress"
)

// Decompressor reads zstd-compressed data from an io.Reader and provides
// decompressed data via its own Read method. It expects the stream to be
// prefixed with a uint64 (big-endian) uncompressed size, as written by
// the corresponding Compressor. The size is used to limit the number of
// decompressed bytes returned, so that the zstd decoder is never asked
// to probe for a subsequent frame (which would block on a network stream).
type Decompressor struct {
	cr     *progress.CountingReader
	dec    *zstd.Decoder
	reader io.Reader // LimitReader over the decoder output

	nRx int64
	nTx int64
}

// NewDecompressor returns an instantiated Decompressor that reads from r and
// decompresses the data using zstd.
func NewDecompressor(r io.Reader) *Decompressor {
	return &Decompressor{
		cr: progress.NewCountingReader(r),
	}
}

// Read reads decompressed data.
func (d *Decompressor) Read(p []byte) (nn int, err error) {
	defer func() {
		d.nTx += int64(nn)
	}()
	if d.cr == nil {
		return 0, io.EOF
	}
	if d.reader == nil {
		// Read the 8-byte header to learn the uncompressed payload size.
		var hdr [8]byte
		if _, err := io.ReadFull(d.cr, hdr[:]); err != nil {
			return 0, err
		}
		uncompressedSize := int64(binary.BigEndian.Uint64(hdr[:]))

		d.dec, err = zstd.NewReader(d.cr, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return 0, err
		}

		// Limit decompressed output so we never ask the decoder to
		// probe for the next frame after this one is exhausted.
		d.reader = io.LimitReader(d.dec, uncompressedSize)
	}

	n, err := d.reader.Read(p)
	d.nRx += int64(n)

	if err == io.EOF {
		d.dec.Close()
		d.cr = nil
		d.dec = nil
		d.reader = nil
	}
	return n, err
}
