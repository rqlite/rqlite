package snapshot

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	blueprintName = "blueprint.json"
)

// Blueprint is the metadata for a snapshot.
type Blueprint struct {
	// Version is the snapshot version.
	Version uint64 `json:"version"`

	// Filename is the name of the file containing the SQLite database.
	Filename string `json:"filename"`
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

// WriteTo writes the snapshot to the given writer.
func (v *V2Encoder) WriteTo(w io.Writer) (int64, error) {
	cw := &CountingWriter{Writer: w}
	gw := gzip.NewWriter(cw)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	// Write blueprint.
	bp := &Blueprint{
		Version:  2,
		Filename: filepath.Base(v.path),
	}
	bpb, err := json.Marshal(bp)
	if err != nil {
		return 0, err
	}
	blueprintHeader := &tar.Header{
		Name: blueprintName,
		Size: int64(len(bpb)),
	}
	if err := tw.WriteHeader(blueprintHeader); err != nil {
		return cw.Count, err
	}
	if _, err := tw.Write(bpb); err != nil {
		return cw.Count, err
	}

	// Write database.
	file, err := os.Open(v.path)
	if err != nil {
		return cw.Count, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return cw.Count, err
	}
	sqliteHeader := &tar.Header{
		Name: stat.Name(),
		Size: stat.Size(),
	}
	if err := tw.WriteHeader(sqliteHeader); err != nil {
		return cw.Count, err
	}
	if _, err := io.Copy(tw, file); err != nil {
		return cw.Count, err
	}

	// We're done.
	if err := tw.Close(); err != nil {
		return cw.Count, err
	}
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
	gr, err := gzip.NewReader(v.r)
	if err != nil {
		return 0, err
	}
	defer gr.Close()
	tr := tar.NewReader(gr)

	// Read the blueprint
	header, err := tr.Next()
	if err == io.EOF {
		return 0, fmt.Errorf("expected %s, got EOF", blueprintName)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read blueprint header: %w", err)
	}
	if header.Name != blueprintName {
		return 0, fmt.Errorf("expected %s, got %s", blueprintName, header.Name)
	}
	bp := &Blueprint{}
	if err := json.NewDecoder(tr).Decode(bp); err != nil {
		return 0, fmt.Errorf("failed to decode blueprint: %w", err)
	}
	if bp.Version != 2 {
		return 0, fmt.Errorf("unsupported version (%d)", bp.Version)
	}

	// Read the data
	header, err = tr.Next()
	if err == io.EOF {
		return 0, fmt.Errorf("expected %s, got EOF", blueprintName)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read data header: %w", err)
	}
	if header.Name != bp.Filename {
		return 0, fmt.Errorf("expected %s, got %s", bp.Filename, header.Name)
	}

	// Write the data
	n, err := io.Copy(w, tr)
	if err != nil {
		return 0, fmt.Errorf("failed to write data: %w", err)
	}

	return n, err
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
