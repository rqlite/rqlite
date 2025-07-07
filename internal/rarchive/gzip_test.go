package rarchive

import (
	"compress/gzip"
	"io"
	"os"
	"testing"
)

// Test_GzipFileNotFound tests the case where the input file does not exist.
func Test_GzipFileNotFound(t *testing.T) {
	_, err := Gzip("nonexistentfile.txt")
	if err == nil {
		t.Error("Expected an error for non-existent file, got none")
	}
}

func Test_GzipSuccess(t *testing.T) {
	testContent := "This is a test content"
	tmpFile, err := os.CreateTemp("", "content")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up the file afterward
	if _, err := tmpFile.WriteString(testContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	gzFile, err := Gzip(tmpFile.Name())
	if err != nil {
		t.Fatalf("Gzip failed: %v", err)
	}
	defer os.Remove(gzFile) // Clean up the gzipped file

	// Open the file, and then read from it using a gzip reader
	f, err := os.Open(gzFile)
	if err != nil {
		t.Fatalf("Failed to open gzipped file: %v", err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}

	// Read the content from the gzip reader
	decompressedData, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("Failed to read decompressed data: %v", err)
	}

	if string(decompressedData) != testContent {
		t.Errorf("Decompressed data mismatch. Expected %q, got %q", testContent, decompressedData)
	}
}

// Test_Gunzip_ValidFile tests if Gunzip successfully decompresses a valid GZIP file.
func Test_Gunzip_ValidFile(t *testing.T) {
	// Create a temporary GZIP file
	tmpFile, err := os.CreateTemp("", "test.gz")
	if err != nil {
		t.Fatalf("Failed to create temp gzip file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up the file afterward

	// Write compressed data to the temporary file
	gw := gzip.NewWriter(tmpFile)
	_, err = gw.Write([]byte("test data"))
	if err != nil {
		t.Fatalf("Failed to write to gzip writer: %v", err)
	}
	gw.Close()
	tmpFile.Close()

	// Now test the Gunzip function
	outFile, err := Gunzip(tmpFile.Name())
	if err != nil {
		t.Fatalf("Gunzip failed: %v", err)
	}
	defer os.Remove(outFile) // Clean up the decompressed file afterward

	// Read and check the contents of the decompressed file
	decompressedData, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("Failed to read decompressed file: %v", err)
	}
	expectedData := "test data"
	if string(decompressedData) != expectedData {
		t.Errorf("Decompressed data mismatch. Expected %q, got %q", expectedData, decompressedData)
	}
}

// Test_Gunzip_FileNotFound tests if Gunzip returns an error when the file does not exist.
func Test_Gunzip_FileNotFound(t *testing.T) {
	_, err := Gunzip("non_existent_file.gz")
	if err == nil {
		t.Error("Expected an error when trying to decompress a non-existent file, but got none")
	}
}

// Test_Gunzip_InvalidGzip tests if Gunzip returns an error when the input file is not a valid GZIP file.
func Test_Gunzip_InvalidGzip(t *testing.T) {
	// Create a temporary non-GZIP file
	tmpFile, err := os.CreateTemp("", "test_invalid.gz")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up the file afterward

	// Write invalid (non-GZIP) data to the file
	_, err = tmpFile.Write([]byte("this is not gzip data"))
	if err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Now test the Gunzip function
	_, err = Gunzip(tmpFile.Name())
	if err == nil {
		t.Error("Expected an error when trying to decompress invalid GZIP data, but got none")
	}
}

// Test_Gunzip_EmptyFile tests if Gunzip returns an error when the input file is empty.
func Test_Gunzip_EmptyFile(t *testing.T) {
	// Create an empty file
	tmpFile, err := os.CreateTemp("", "test_empty.gz")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up the file afterward
	tmpFile.Close()

	// Now test the Gunzip function
	_, err = Gunzip(tmpFile.Name())
	if err == nil {
		t.Error("Expected an error when trying to decompress an empty file, but got none")
	}
}
