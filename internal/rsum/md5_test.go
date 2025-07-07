package rsum

import (
	"crypto/md5"
	"encoding/hex"
	"testing"
)

// TestMD5Success tests the successful calculation of the MD5 checksum.
func TestMD5Success(t *testing.T) {
	testContent := "test content"
	testFile := mustWriteTempFile(t, []byte(testContent))

	// Call the MD5 function
	checksum, err := MD5(testFile)
	if err != nil {
		t.Fatalf("MD5 calculation failed: %v", err)
	}

	// Manually calculate the expected checksum
	hash := md5.New()
	hash.Write([]byte(testContent))
	expectedChecksum := hex.EncodeToString(hash.Sum(nil))

	// Compare the returned checksum with the expected one
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %v, got %v", expectedChecksum, checksum)
	}
}

// TestMD5FileNotFound tests the case where the input file does not exist.
func TestMD5FileNotFound(t *testing.T) {
	_, err := MD5("nonexistentfile.txt")
	if err == nil {
		t.Error("Expected an error for non-existent file, got none")
	}
}

// TestMD5EmptyFile tests the calculation of the MD5 checksum for an empty file.
func TestMD5EmptyFile(t *testing.T) {
	testFile := mustWriteTempFile(t, []byte(""))

	checksum, err := MD5(testFile)
	if err != nil {
		t.Fatalf("MD5 calculation failed: %v", err)
	}

	// The MD5 checksum for an empty input should be the checksum of an empty byte array.
	expectedChecksum := hex.EncodeToString(md5.New().Sum(nil))

	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %v for empty file, got %v", expectedChecksum, checksum)
	}
}
