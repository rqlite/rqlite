package rarchive

import (
	"bytes"
	"compress/zlib"
	"io"
)

// CompressZlib compresses the given data using zlib compression.
func CompressZlib(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)

	_, err := w.Write(data)
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

// DecompressZlib decompresses the given zlib-compressed data.
func DecompressZlib(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)

	r, err := zlib.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}
