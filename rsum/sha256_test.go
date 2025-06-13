package rsum

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestSHA256Success tests the successful calculation of the SHA256 checksum.
func TestSHA256Success(t *testing.T) {
	testContent := "test content"
	testFile := mustWriteTempFile(t, []byte(testContent))

	// Call the SHA256 function
	checksum, err := SHA256(testFile)
	if err != nil {
		t.Fatalf("SHA256 calculation failed: %v", err)
	}

	// Manually calculate the expected checksum
	hash := sha256.New()
	hash.Write([]byte(testContent))
	expectedChecksum := hex.EncodeToString(hash.Sum(nil))

	// Compare the returned checksum with the expected one
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %v, got %v", expectedChecksum, checksum)
	}
}

// TestSHA256FileNotFound tests the case where the input file does not exist.
func TestSHA256FileNotFound(t *testing.T) {
	_, err := SHA256("nonexistentfile.txt")
	if err == nil {
		t.Error("Expected an error for non-existent file, got none")
	}
}

// TestSHA256EmptyFile tests the calculation of the SHA256 checksum for an empty file.
func TestSHA256EmptyFile(t *testing.T) {
	testFile := mustWriteTempFile(t, []byte(""))

	checksum, err := SHA256(testFile)
	if err != nil {
		t.Fatalf("SHA256 calculation failed: %v", err)
	}

	// The SHA256 checksum for an empty input should be the checksum of an empty byte array.
	expectedChecksum := hex.EncodeToString(sha256.New().Sum(nil))

	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %v for empty file, got %v", expectedChecksum, checksum)
	}
}
