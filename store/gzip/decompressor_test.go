package gzip

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"
)

func Test_Decompressor(t *testing.T) {
	// Write some gzipped data to a buffer
	testData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	gzw.Write([]byte(testData))
	gzw.Close()

	// Decompress the data
	decompressor := NewDecompressor(&buf)
	decompressedBuffer := new(bytes.Buffer)
	_, err := io.Copy(decompressedBuffer, decompressor)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}

	// Verify the decompressed data matches original data
	if !bytes.Equal(decompressedBuffer.Bytes(), []byte(testData)) {
		t.Fatalf("decompressed data does not match original")
	}
}
