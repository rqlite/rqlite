package gzip

import (
	"compress/gzip"
	"io"

	"github.com/rqlite/rqlite/v8/internal/progress"
)

// Decompressor is a wrapper around a gzip.Reader that reads from an io.Reader
// and decompresses the data.
type Decompressor struct {
	cr  *progress.CountingReader
	gzr *gzip.Reader

	nRx int64
	nTx int64
}

// NewDecompressor returns an instantiated Decompressor that reads from r and
// decompresses the data using gzip.
func NewDecompressor(r io.Reader) *Decompressor {
	return &Decompressor{
		cr: progress.NewCountingReader(r),
	}
}

// Read reads decompressed data.
func (c *Decompressor) Read(p []byte) (nn int, err error) {
	defer func() {
		c.nTx += int64(nn)
	}()
	if c.cr == nil {
		return 0, io.EOF
	}
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
		c.cr = nil // Signal no more re-use of the underlying reader.
		c.gzr = nil
	}
	return n, err
}
