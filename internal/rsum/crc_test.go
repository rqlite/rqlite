package rsum

import (
	"hash/crc32"
	"os"
	"testing"
)

func Test_CRC32Success(t *testing.T) {
	testContent := "test content"
	testFile := mustWriteTempFile(t, []byte(testContent))

	checksum, err := CRC32(testFile)
	if err != nil {
		t.Fatalf("CRC32 calculation failed: %v", err)
	}

	// Manually calculate the expected checksum
	h := crc32.NewIEEE()
	h.Write([]byte(testContent))
	expectedChecksum := h.Sum32()

	// Compare the returned checksum with the expected one
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %v, got %v", expectedChecksum, checksum)
	}
}

func Test_CRC32FileNotFound(t *testing.T) {
	_, err := CRC32("nonexistentfile.txt")
	if err == nil {
		t.Error("Expected an error for non-existent file, got none")
	}
}

func Test_CRC32EmptyFile(t *testing.T) {
	testFile := mustWriteTempFile(t, []byte(""))

	checksum, err := CRC32(testFile)
	if err != nil {
		t.Fatalf("CRC32 calculation failed: %v", err)
	}

	// The CRC32 checksum for an empty input should be 0.
	expectedChecksum := crc32.NewIEEE().Sum32()
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %v for empty file, got %v", expectedChecksum, checksum)
	}
}

// mustWriteTempFile writes the given bytes to a temporary file, and returns the
// path to the file. If there is an error, it panics. The file will be automatically
// deleted when the test ends.
func mustWriteTempFile(t *testing.T, b []byte) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "rqlite-test")
	if err != nil {
		panic("failed to create temp file")
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		panic("failed to write to temp file")
	}
	return f.Name()
}
