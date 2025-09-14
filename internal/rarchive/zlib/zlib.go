package zlib

import (
	"bytes"
	"compress/zlib"
	"io"
)

// Compress compresses the given data using zlib compression.
func Compress(data []byte) ([]byte, error) {
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

// Decompress decompresses the given zlib-compressed data.
func Decompress(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)

	r, err := zlib.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

type Reader struct {
	io.Reader
}

// NewReader creates a new zlib reader from the given io.Reader.
func NewReader(data []byte) (*Reader, error) {
	zr, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return &Reader{zr}, nil
}

func (r *Reader) Close() error {
	if closer, ok := r.Reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (r *Reader) Reset(data []byte) error {
	if resetter, ok := r.Reader.(interface {
		Reset(io.Reader) error
	}); ok {
		return resetter.Reset(bytes.NewReader(data))
	}
	return nil
}
