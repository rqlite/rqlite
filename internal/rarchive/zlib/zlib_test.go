package zlib

import (
	"bytes"
	"io"
	"testing"
)

// Test_CompressDecompress tests basic compression and decompression functionality.
func Test_CompressDecompress(t *testing.T) {
	testData := []byte("Hello, this is a test string for zlib compression!")

	// Compress the data
	compressed, err := Compress(testData)
	if err != nil {
		t.Fatalf("Failed to compress data: %v", err)
	}

	// Verify compression actually reduces size for this test data
	if len(compressed) >= len(testData) {
		t.Logf("Compression did not reduce size: original=%d, compressed=%d", len(testData), len(compressed))
	}

	// Decompress the data
	decompressed, err := Decompress(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress data: %v", err)
	}

	// Verify the data is identical
	if !bytes.Equal(testData, decompressed) {
		t.Errorf("Decompressed data does not match original. Original: %q, Decompressed: %q", testData, decompressed)
	}
}

// Test_Compress_EmptyData tests compression of empty data.
func Test_Compress_EmptyData(t *testing.T) {
	testData := []byte{}

	compressed, err := Compress(testData)
	if err != nil {
		t.Fatalf("Failed to compress empty data: %v", err)
	}

	decompressed, err := Decompress(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress empty data: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Errorf("Decompressed empty data does not match original")
	}
}

// Test_Decompress_InvalidData tests decompression of invalid data.
func Test_Decompress_InvalidData(t *testing.T) {
	invalidData := []byte("this is not zlib compressed data")

	_, err := Decompress(invalidData)
	if err == nil {
		t.Error("Expected an error when decompressing invalid data, but got none")
	}
}

func Test_Reader(t *testing.T) {
	testData := []byte("Hello, this is a test string for zlib compression!")

	// Compress the data
	compressed, err := Compress(testData)
	if err != nil {
		t.Fatalf("Failed to compress data: %v", err)
	}

	// Create a new zlib reader
	reader, err := NewReader(compressed)
	if err != nil {
		t.Fatalf("Failed to create zlib reader: %v", err)
	}
	defer reader.Close()

	// Read all data from the reader and check it matches original.
	decompressed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read from zlib reader: %v", err)
	}
	if !bytes.Equal(testData, decompressed) {
		t.Errorf("Decompressed data does not match original. Original: %q, Decompressed: %q", testData, decompressed)
	}

	// Test resetting the reader with new data.
	testData2 := []byte("Hello, this is another test string for zlib compression!")
	compressed2, err := Compress(testData2)
	if err != nil {
		t.Fatalf("Failed to compress data: %v", err)
	}
	if err := reader.Reset(compressed2); err != nil {
		t.Fatalf("Failed to reset zlib reader: %v", err)
	}

	// Read all data from the reset reader and check it matches new original.
	decompressed2, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read from zlib reader after reset: %v", err)
	}
	if !bytes.Equal(testData2, decompressed2) {
		t.Errorf("Decompressed data after reset does not match original. Original: %q, Decompressed: %q", testData2, decompressed2)
	}
}
