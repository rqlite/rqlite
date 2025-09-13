package rarchive

import (
	"bytes"
	"testing"
)

// Test_CompressDecompressZlib tests basic compression and decompression functionality.
func Test_CompressDecompressZlib(t *testing.T) {
	testData := []byte("Hello, this is a test string for zlib compression!")

	// Compress the data
	compressed, err := CompressZlib(testData)
	if err != nil {
		t.Fatalf("Failed to compress data: %v", err)
	}

	// Verify compression actually reduces size for this test data
	if len(compressed) >= len(testData) {
		t.Logf("Compression did not reduce size: original=%d, compressed=%d", len(testData), len(compressed))
	}

	// Decompress the data
	decompressed, err := DecompressZlib(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress data: %v", err)
	}

	// Verify the data is identical
	if !bytes.Equal(testData, decompressed) {
		t.Errorf("Decompressed data does not match original. Original: %q, Decompressed: %q", testData, decompressed)
	}
}

// Test_CompressZlib_EmptyData tests compression of empty data.
func Test_CompressZlib_EmptyData(t *testing.T) {
	testData := []byte{}

	compressed, err := CompressZlib(testData)
	if err != nil {
		t.Fatalf("Failed to compress empty data: %v", err)
	}

	decompressed, err := DecompressZlib(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress empty data: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Errorf("Decompressed empty data does not match original")
	}
}

// Test_DecompressZlib_InvalidData tests decompression of invalid data.
func Test_DecompressZlib_InvalidData(t *testing.T) {
	invalidData := []byte("this is not zlib compressed data")

	_, err := DecompressZlib(invalidData)
	if err == nil {
		t.Error("Expected an error when decompressing invalid data, but got none")
	}
}
