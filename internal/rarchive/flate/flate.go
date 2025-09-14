package flate

import (
	"bytes"
	"compress/flate"
	"io"
)

// Compress compresses the given data using flate compression.
func Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}

	_, err = w.Write(data)
	if err != nil {
		w.Close()
		return nil, err
	}

	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decompress decompresses the given flate-compressed data.
func Decompress(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)

	r := flate.NewReader(reader)
	defer r.Close()
	return io.ReadAll(r)
}
