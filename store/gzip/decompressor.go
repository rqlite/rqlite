package gzip

import (
	"compress/gzip"
	"io"
)

type Decompressor struct {
	r   io.Reader
	gzr *gzip.Reader
	n   int64
}

// NewDecompressor returns an instantied Decompressor that reads from r and
// decompresses the data using gzip.
func NewDecompressor(r io.Reader) *Decompressor {
	return &Decompressor{
		r: r,
	}
}

// Read reads decompressed data.
func (c *Decompressor) Read(p []byte) (n int, err error) {
	defer func() {
		c.n += int64(n)
	}()

	if c.gzr == nil {
		var err error
		c.gzr, err = gzip.NewReader(c.r)
		if err != nil {
			return 0, err
		}
	}

	n, err = c.gzr.Read(p)
	if err == io.EOF {
		if err := c.gzr.Close(); err != nil {
			return 0, err
		}
		c.gzr = nil
	}
	return n, err
}
