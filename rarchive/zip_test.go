package rarchive

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func Test_IsZipFile(t *testing.T) {
	// Create a valid ZIP file
	zipFilename := mustTempFile()
	mustCreateZipFile(zipFilename, "This is a test file inside the zip")
	defer os.Remove(zipFilename)

	// Create a non-ZIP file
	nonZipFilename := mustTempFile()
	mustWriteBytesToFile(nonZipFilename, []byte("This is not a zip file"))
	defer os.Remove(nonZipFilename)

	// Test cases
	tests := []struct {
		filename string
		expected bool
	}{
		{filename: zipFilename, expected: true},
		{filename: nonZipFilename, expected: false},
		{filename: "non_existent_file.zip", expected: false},
	}

	for _, test := range tests {
		t.Run(test.filename, func(t *testing.T) {
			result := IsZipFile(test.filename)
			if result != test.expected {
				t.Errorf("IsZipFile(%s) = %v; want %v", test.filename, result, test.expected)
			}
		})
	}
}

func Test_UnzipToDir(t *testing.T) {
	// Test data: files to include in the ZIP archive
	testFiles := map[string]string{
		"file1.txt": "This is file 1",
		"file2.txt": "This is file 2",
		"file3.txt": "This is file 3",
	}

	// Create the ZIP file in memory
	zipData, err := createZipInMemory(testFiles)
	if err != nil {
		t.Fatalf("Failed to create ZIP file in memory: %v", err)
	}
	zipFileName := mustTempFile()
	defer os.Remove(zipFileName)
	mustWriteBytesToFile(zipFileName, zipData)

	// Create a temporary directory for unzipping
	outputDir, err := os.MkdirTemp("", "unzip_test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(outputDir)

	// Unzip the file to the directory
	err = UnzipToDir(zipFileName, outputDir)
	if err != nil {
		t.Fatalf("UnzipToDir failed: %v", err)
	}

	// Verify the files were unzipped correctly
	for name, content := range testFiles {
		filePath := filepath.Join(outputDir, name)
		data, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("Failed to read unzipped file %s: %v", filePath, err)
		}
		if string(data) != content {
			t.Fatalf("Content of file %s does not match: got %s, want %s", filePath, data, content)
		}
	}
}

func Test_UnzipToDir_InvalidZip(t *testing.T) {
	// Create a temporary directory for unzipping
	outputDir, err := os.MkdirTemp("", "unzip_test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(outputDir)

	// Try to unzip a non-zip file
	err = UnzipToDir("non_existent.zip", outputDir)
	if err == nil {
		t.Fatalf("Expected error when unzipping a non-existent file, got nil")
	}

	// Write a non-zip file to disk
	nonZipFileName := mustTempFile()
	defer os.Remove(nonZipFileName)
	mustWriteBytesToFile(nonZipFileName, []byte("This is not a zip file"))

	// Try to unzip the non-zip file
	err = UnzipToDir(nonZipFileName, outputDir)
	if err == nil {
		t.Fatalf("Expected error when unzipping a non-zip file, got nil")
	}
}

func createZipInMemory(files map[string]string) ([]byte, error) {
	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)

	for name, content := range files {
		writer, err := zipWriter.Create(name)
		if err != nil {
			return nil, err
		}
		_, err = writer.Write([]byte(content))
		if err != nil {
			return nil, err
		}
	}

	err := zipWriter.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func mustCreateZipFile(filename, fileContent string) {
	zipFile, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	writer, err := zipWriter.Create("testfile.txt")
	if err != nil {
		panic(err)
	}

	_, err = io.Copy(writer, bytes.NewReader([]byte(fileContent)))
	if err != nil {
		panic(err)
	}
}

func mustTempFile() string {
	tmpfile, err := os.CreateTemp("", "rqlite-archive-test")
	if err != nil {
		panic(err.Error())
	}
	tmpfile.Close()
	return tmpfile.Name()
}

func mustWriteBytesToFile(filename string, b []byte) {
	err := os.WriteFile(filename, b, 0644)
	if err != nil {
		panic(err)
	}
}
