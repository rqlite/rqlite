package rarchive

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
)

// TestIsTarGzipFile tests the IsTarGzipFile function.
func TestIsTarGzipFile(t *testing.T) {
	// Create a valid tar.gz file for testing
	tarGzipFile := "test.tar.gz"
	createTestTarGzipFile(t, tarGzipFile, map[string]string{
		"file1.txt": "This is file 1",
		"file2.txt": "This is file 2",
	})
	defer os.Remove(tarGzipFile)

	if !IsTarGzipFile(tarGzipFile) {
		t.Errorf("expected %s to be recognized as a gzipped tarball", tarGzipFile)
	}

	// Test with a non-existent file
	if IsTarGzipFile("nonexistent.tar.gz") {
		t.Errorf("expected nonexistent.tar.gz to be recognized as not a gzipped tarball")
	}
}

// TestTarGzipHasSubdirectories tests the TarGzipHasSubdirectories function.
func TestTarGzipHasSubdirectories(t *testing.T) {
	// Create a tar.gz file without subdirectories
	tarGzipFile := "test_nodir.tar.gz"
	createTestTarGzipFile(t, tarGzipFile, map[string]string{
		"file1.txt": "This is file 1",
		"file2.txt": "This is file 2",
	})
	defer os.Remove(tarGzipFile)

	hasSubdirs, err := TarGzipHasSubdirectories(tarGzipFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasSubdirs {
		t.Errorf("expected %s to not have subdirectories", tarGzipFile)
	}

	// Create a tar.gz file with subdirectories
	tarGzipFileWithDir := "test_withdir.tar.gz"
	createTestTarGzipFile(t, tarGzipFileWithDir, map[string]string{
		"dir/file1.txt": "This is file 1 in a dir",
	})
	defer os.Remove(tarGzipFileWithDir)

	hasSubdirs, err = TarGzipHasSubdirectories(tarGzipFileWithDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasSubdirs {
		t.Errorf("expected %s to have subdirectories", tarGzipFileWithDir)
	}
}

// TestUntarGzipToDir tests the UntarGzipToDir function.
func TestUntarGzipToDir(t *testing.T) {
	// Create a tar.gz file for testing
	tarGzipFile := "test_untar.tar.gz"
	createTestTarGzipFile(t, tarGzipFile, map[string]string{
		"file1.txt": "This is file 1",
		"file2.txt": "This is file 2",
	})
	defer os.Remove(tarGzipFile)

	// Create a directory to extract to
	extractDir := t.TempDir()

	err := UntarGzipToDir(tarGzipFile, extractDir)
	if err != nil {
		t.Fatalf("unexpected error during extraction: %v", err)
	}

	// Verify that the files were extracted correctly
	checkFileContent(t, filepath.Join(extractDir, "file1.txt"), "This is file 1")
	checkFileContent(t, filepath.Join(extractDir, "file2.txt"), "This is file 2")
}

// createTestTarGzipFile creates a tar.gz file with the given contents.
func createTestTarGzipFile(t *testing.T, tarGzipFile string, contents map[string]string) {
	f, err := os.Create(tarGzipFile)
	if err != nil {
		t.Fatalf("failed to create tar.gz file: %v", err)
	}
	defer f.Close()

	gw := gzip.NewWriter(f)
	defer gw.Close()

	tw := tar.NewWriter(gw)
	defer tw.Close()

	for name, content := range contents {
		hdr := &tar.Header{
			Name: name,
			Mode: 0600,
			Size: int64(len(content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("failed to write tar header: %v", err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("failed to write tar content: %v", err)
		}
	}
}

// checkFileContent verifies the content of the file at the given path.
func checkFileContent(t *testing.T, path, expectedContent string) {
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file %s: %v", path, err)
	}

	if string(data) != expectedContent {
		t.Errorf("expected file content %q, got %q", expectedContent, string(data))
	}
}
