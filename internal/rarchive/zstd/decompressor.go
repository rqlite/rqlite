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

	hdrRead          bool
	uncompressedSize int64

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

// UncompressedSize returns the size of the uncompressed payload, as recorded
// in the stream header. The header is read from the underlying reader if it
// has not been read yet.
func (d *Decompressor) UncompressedSize() (int64, error) {
	if err := d.readHeader(); err != nil {
		return 0, err
	}
	return d.uncompressedSize, nil
}

// Read reads decompressed data.
func (d *Decompressor) Read(p []byte) (nn int, err error) {
	defer func() {
		d.nTx += int64(nn)
	}()
	if d.cr == nil {
		return 0, io.EOF
	}
	if err := d.readHeader(); err != nil {
		return 0, err
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

// readHeader reads the 8-byte header to learn the uncompressed payload size,
// and sets up the decoder, if that has not already been done.
func (d *Decompressor) readHeader() error {
	if d.hdrRead {
		return nil
	}
	var hdr [8]byte
	if _, err := io.ReadFull(d.cr, hdr[:]); err != nil {
		return err
	}
	d.uncompressedSize = int64(binary.BigEndian.Uint64(hdr[:]))

	dec, err := zstd.NewReader(d.cr, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return err
	}
	d.dec = dec

	// Limit decompressed output so we never ask the decoder to
	// probe for the next frame after this one is exhausted.
	d.reader = io.LimitReader(d.dec, d.uncompressedSize)
	d.hdrRead = true
	return nil
}
