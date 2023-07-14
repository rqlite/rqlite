package chunking

import (
	"io"
)

// CountingReader is a wrapper around an io.Reader that counts the number of
// bytes read.
type CountingReader struct {
	r io.Reader
	n int64
}

// Read reads from the underlying io.Reader.
func (cr *CountingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n += int64(n)
	return n, err
}

// Count returns the number of bytes read.
func (cr *CountingReader) Count() int64 {
	return cr.n
}

// NewCountingReader returns a new CountingReader that reads from r.
func NewCountingReader(r io.Reader) *CountingReader {
	return &CountingReader{
		r: r,
	}
}

// CountingWriter is a wrapper around an io.Writer that counts the number of
// bytes written.
type CountingWriter struct {
	w io.Writer
	n int64
}

// Write writes to the underlying io.Writer.
func (cw *CountingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// Count returns the number of bytes written.
func (cw *CountingWriter) Count() int64 {
	return cw.n
}

// NewCountingWriter returns a new CountingWriter that writes to w.
func NewCountingWriter(w io.Writer) *CountingWriter {
	return &CountingWriter{
		w: w,
	}
}
