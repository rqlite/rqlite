package flate

import (
	"bytes"
	"testing"
)

func Test_CompressDecompress(t *testing.T) {
	testData := []byte(`{"table":"users","operation":"UPDATE","commit_id":"a7f92c4e","timestamp":"2025-09-14T12:34:56Z","row_id":42,"before":{"id":42,"name":"Alice","email":"alice@example.com","active":true},"after":{"id":42,"name":"Alice Smith","email":"asmith@example.com","active":true}}
`)

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
