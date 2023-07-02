package snapshot

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
)

const (
	RqliteHeaderVersionSize  = 32
	RqliteHeaderReservedSize = 32

	RqliteSnapshotVersion2 = "rqlite snapshot version 2"
)

// FileIsV2Snapshot returns true if the given path is a V2 snapshot.
func FileIsV2Snapshot(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	return ReaderIsV2Snapshot(file)
}

// ReaderIsV2Snapshot returns true if the given reader is a V2 snapshot.
// The reader will be advanced 1 byte passed the end of the Version header.
func ReaderIsV2Snapshot(r io.Reader) bool {
	header := make([]byte, RqliteHeaderVersionSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return false
	}
	return string(header[:len(RqliteSnapshotVersion2)]) == RqliteSnapshotVersion2
}

// V2Encoder creates a new V2 snapshot.
type V2Encoder struct {
	path string
}

// NewV2Encoder returns an initialized V2 encoder
func NewV2Encoder(path string) *V2Encoder {
	return &V2Encoder{
		path: path,
	}
}

// WriteTo writes the snapshot to the given writer. Returns the number
// of bytes written, or an error.
func (v *V2Encoder) WriteTo(w io.Writer) (int64, error) {
	file, err := os.Open(v.path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Wrap w in counting writer.
	cw := &CountingWriter{Writer: w}

	if _, err := writeString(cw, RqliteSnapshotVersion2, RqliteHeaderVersionSize); err != nil {
		return 0, err
	}

	// Write reserved space.
	if _, err = cw.Write(make([]byte, RqliteHeaderReservedSize)); err != nil {
		return cw.Count, err
	}

	gw, err := gzip.NewWriterLevel(cw, gzip.BestSpeed)
	if err != nil {
		return cw.Count, err
	}
	defer gw.Close()

	if _, err := io.Copy(gw, file); err != nil {
		return cw.Count, err
	}

	// We're done.
	if err := gw.Close(); err != nil {
		return cw.Count, err
	}
	if err := file.Close(); err != nil {
		return cw.Count, err
	}

	return cw.Count, nil
}

// V2Decoder reads a V2 snapshot.
type V2Decoder struct {
	r io.Reader
}

// NewV2Decoder returns an initialized V2 decoder
func NewV2Decoder(r io.Reader) *V2Decoder {
	return &V2Decoder{
		r: r,
	}
}

// WriteTo writes the decoded snapshot data to the given writer.
func (v *V2Decoder) WriteTo(w io.Writer) (int64, error) {
	if !ReaderIsV2Snapshot(v.r) {
		return 0, fmt.Errorf("data is not a V2 snapshot")
	}

	// Read the reserved space and discard.
	reserved := make([]byte, RqliteHeaderReservedSize)
	if _, err := io.ReadFull(v.r, reserved); err != nil {
		return 0, fmt.Errorf("failed to read reserved space: %w", err)
	}

	gr, err := gzip.NewReader(v.r)
	if err != nil {
		return 0, err
	}
	defer gr.Close()

	// Decompress the database.
	n, err := io.Copy(w, gr)
	if err != nil {
		return 0, fmt.Errorf("failed to write data: %w", err)
	}

	return n, err
}

// function which takes a writer, a string, and a length. If the string is longer
// than the length return an error. Otherwise string the string to the writer and
// fil the remain space up to the lnegth with 0.
func writeString(w io.Writer, s string, l int) (int, error) {
	if len(s) >= l {
		return 0, fmt.Errorf("string too long (%d, %d)", len(s), l)
	}
	if _, err := w.Write([]byte(s)); err != nil {
		return 0, err
	}
	return w.Write(make([]byte, l-len(s)))
}

// CountingWriter counts the number of bytes written to it.
type CountingWriter struct {
	Writer io.Writer
	Count  int64
}

// Write writes to the underlying writer and counts the number of bytes written.
func (cw *CountingWriter) Write(p []byte) (int, error) {
	n, err := cw.Writer.Write(p)
	cw.Count += int64(n)
	return n, err
}
