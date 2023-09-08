package gzip

import (
	"bytes"
	"compress/gzip"
	"io"
)

const DefaultBufferSize = 65536

// Compressor is a wrapper around a gzip.Writer that reads from an io.Reader
// and writes to an internal buffer.  The internal buffer is used to store
// compressed data until it is read by the caller.
type Compressor struct {
	r     io.Reader
	bufSz int
	buf   *bytes.Buffer
	gzw   *gzip.Writer

	nRx int64
	nTx int64
}

// NewCompressor returns an instantiated Compressor that reads from r and
// compresses the data using gzip.
func NewCompressor(r io.Reader, bufSz int) *Compressor {
	buf := new(bytes.Buffer)
	return &Compressor{
		r:     r,
		bufSz: bufSz,
		buf:   buf,
		gzw:   gzip.NewWriter(buf),
	}
}

// Read reads compressed data.
func (c *Compressor) Read(p []byte) (n int, err error) {
	if c.buf.Len() == 0 && c.gzw != nil {
		nn, err := io.CopyN(c.gzw, c.r, int64(c.bufSz))
		c.nRx += nn
		c.nTx += int64(len(p))
		if err != nil {
			// Time to write the footer.
			if err := c.Close(); err != nil {
				return 0, err
			}
			if err != io.EOF {
				// Actual error, let caller handle
				return 0, err
			}
		} else if nn > 0 {
			// We read some data, but didn't hit any error.
			// Just flush the data in the buffer, ready
			// to be read.
			if err := c.gzw.Flush(); err != nil {
				return 0, err
			}
		}
	}
	return c.buf.Read(p)
}

// Close closes the Compressor.
func (c *Compressor) Close() error {
	if c.gzw == nil {
		return nil
	}
	if err := c.gzw.Close(); err != nil {
		return err
	}
	c.gzw = nil
	return nil
}
