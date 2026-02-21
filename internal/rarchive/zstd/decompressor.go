package zstd

import (
	"encoding/binary"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/rqlite/rqlite/v10/internal/progress"
)

// Decompressor reads zstd-compressed data from an io.Reader and provides
// decompressed data via its own Read method. It expects the compressed
// stream to be prefixed with a uint64 (big-endian) payload size, as
// written by the corresponding Compressor. The payload size is used to
// limit reads from the source to exactly the compressed frame, preventing
// the zstd decoder from blocking while probing for a subsequent frame on
// a network stream.
type Decompressor struct {
	cr  *progress.CountingReader
	dec *zstd.Decoder

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
	if d.dec == nil {
		// Read the 8-byte length prefix to learn the compressed payload size.
		var hdr [8]byte
		if _, err := io.ReadFull(d.cr, hdr[:]); err != nil {
			return 0, err
		}
		compressedSize := binary.BigEndian.Uint64(hdr[:])

		// Limit the source to exactly the compressed payload so the
		// decoder cannot block probing for a subsequent frame.
		limited := io.LimitReader(d.cr, int64(compressedSize))

		d.dec, err = zstd.NewReader(limited, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return 0, err
		}
	}

	n, err := d.dec.Read(p)
	d.nRx += int64(n)

	if err == io.EOF {
		d.dec.Close()
		d.cr = nil
		d.dec = nil
	}
	return n, err
}
