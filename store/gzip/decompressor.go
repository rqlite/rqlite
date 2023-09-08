package gzip

import (
	"compress/gzip"
	"io"
)

type Decompressor struct {
	cr  *CountingReader
	gzr *gzip.Reader

	nRx int64
	nTx int64
}

// NewDecompressor returns an instantied Decompressor that reads from r and
// decompresses the data using gzip.
func NewDecompressor(r io.Reader) *Decompressor {
	return &Decompressor{
		cr: NewCountingReader(r),
	}
}

// Read reads decompressed data.
func (c *Decompressor) Read(p []byte) (nn int, err error) {
	defer func() {
		c.nTx += int64(nn)
	}()

	if c.gzr == nil {
		var err error
		c.gzr, err = gzip.NewReader(c.cr)
		if err != nil {
			return 0, err
		}
		// Setting Multistream to false means the gzip reader will
		// return io.EOF when it reaches the end of the gzip stream.
		// Otherwise the reader hangs. This seems to be needed only
		// for the gzip over a stream.
		c.gzr.Multistream(false)
	}

	n, err := c.gzr.Read(p)
	c.nRx += int64(n)

	if err == io.EOF {
		if err := c.gzr.Close(); err != nil {
			return 0, err
		}
		c.gzr = nil
	}
	return n, err
}

// CountingReader is a wrapper around io.Reader that counts the number of bytes
// read.
type CountingReader struct {
	r io.Reader
	n int64
}

// NewCountingReader returns an instantiated CountingReader that reads from r.
func NewCountingReader(r io.Reader) *CountingReader {
	return &CountingReader{
		r: r,
	}
}

// Read reads data.
func (c *CountingReader) Read(p []byte) (n int, err error) {
	n, err = c.r.Read(p)
	c.n += int64(n)
	return n, err
}
