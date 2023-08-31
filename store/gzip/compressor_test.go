package gzip

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"
)

func Test_Compressor_SingleRead(t *testing.T) {
	originalData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")
	reader := bytes.NewReader(originalData)
	compressor := NewCompressor(reader, DefaultBufferSize)

	// Create a buffer to hold compressed data
	compressedBuffer := make([]byte, DefaultBufferSize)

	n, err := compressor.Read(compressedBuffer)
	if err != nil && err != io.EOF {
		t.Fatalf("Unexpected error while reading: %v", err)
	}

	// Decompress the compressed data
	r, err := gzip.NewReader(bytes.NewReader(compressedBuffer[:n]))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	decompressedBuffer := new(bytes.Buffer)
	_, err = io.Copy(decompressedBuffer, r)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	// Verify the decompressed data matches original data
	if !bytes.Equal(decompressedBuffer.Bytes(), originalData) {
		t.Fatalf("Decompressed data does not match original. Got %v, expected %v", decompressedBuffer.Bytes(), originalData)
	}
}

func Test_Compressor_MultipleRead(t *testing.T) {
}
