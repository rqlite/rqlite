package rarchive

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// TestIsTarGzipFile tests the IsTarGzipFile function.
func TestIsTarGzipFile(t *testing.T) {
	// Create a valid tar.gz file for testing
	tarGzipFile := "test.tar.gz"
	createTestTarGzipFile(t, tarGzipFile, map[string]string{
		"file1.txt": "This is file 1",
		"file2.txt": "This is file 2",
	})
	defer fsutil.Remove(tarGzipFile)

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
	defer fsutil.Remove(tarGzipFile)

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
	defer fsutil.Remove(tarGzipFileWithDir)

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
	defer fsutil.Remove(tarGzipFile)

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

// Test_TarGzip_DotSlashEntries checks that a tarball whose entries are prefixed
// with "./" -- as produced by "tar czf out.tar.gz -C dir ." -- is treated as a
// flat archive and extracts cleanly.
func Test_TarGzip_DotSlashEntries(t *testing.T) {
	tarGzipFile := filepath.Join(t.TempDir(), "dotslash.tar.gz")
	createTestTarGzipFileOrdered(t, tarGzipFile, []tarEntry{
		{name: "./", typeflag: tar.TypeDir, mode: 0755},
		{name: "./file1.txt", typeflag: tar.TypeReg, mode: 0600, content: "This is file 1"},
		{name: "./file2.txt", typeflag: tar.TypeReg, mode: 0600, content: "This is file 2"},
	})

	hasSubdirs, err := TarGzipHasSubdirectories(tarGzipFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasSubdirs {
		t.Errorf("expected %s to not have subdirectories", tarGzipFile)
	}

	extractDir := t.TempDir()
	if err := UntarGzipToDir(tarGzipFile, extractDir); err != nil {
		t.Fatalf("unexpected error during extraction: %v", err)
	}
	checkFileContent(t, filepath.Join(extractDir, "file1.txt"), "This is file 1")
	checkFileContent(t, filepath.Join(extractDir, "file2.txt"), "This is file 2")
}

// Test_UntarGzipToDir_NestedFileWithoutDirEntry checks that a file inside a
// subdirectory extracts even when the archive carries no entry for the
// directory itself.
func Test_UntarGzipToDir_NestedFileWithoutDirEntry(t *testing.T) {
	tarGzipFile := filepath.Join(t.TempDir(), "nested.tar.gz")
	createTestTarGzipFileOrdered(t, tarGzipFile, []tarEntry{
		{name: "sub/dir/file1.txt", typeflag: tar.TypeReg, mode: 0600, content: "This is file 1"},
	})

	extractDir := t.TempDir()
	if err := UntarGzipToDir(tarGzipFile, extractDir); err != nil {
		t.Fatalf("unexpected error during extraction: %v", err)
	}
	checkFileContent(t, filepath.Join(extractDir, "sub", "dir", "file1.txt"), "This is file 1")
}

type tarEntry struct {
	name     string
	typeflag byte
	mode     int64
	content  string
}

// createTestTarGzipFileOrdered creates a tar.gz file containing the given
// entries, in the given order.
func createTestTarGzipFileOrdered(t *testing.T, tarGzipFile string, entries []tarEntry) {
	f, err := os.Create(tarGzipFile)
	if err != nil {
		t.Fatalf("failed to create tar.gz file: %v", err)
	}
	defer f.Close()

	gw := gzip.NewWriter(f)
	defer gw.Close()

	tw := tar.NewWriter(gw)
	defer tw.Close()

	for _, e := range entries {
		hdr := &tar.Header{
			Name:     e.name,
			Typeflag: e.typeflag,
			Mode:     e.mode,
			Size:     int64(len(e.content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("failed to write tar header: %v", err)
		}
		if _, err := tw.Write([]byte(e.content)); err != nil {
			t.Fatalf("failed to write tar content: %v", err)
		}
	}
}

func Test_UntarGzipToDirCurrentDirectory(t *testing.T) {
	archive := filepath.Join(t.TempDir(), "input.tar.gz")
	files := map[string]string{"file.txt": "saved", "sub/nested.txt": "nested"}
	createTestTarGzipFile(t, archive, files)
	t.Chdir(t.TempDir())
	if err := UntarGzipToDir(archive, "."); err != nil {
		t.Fatal(err)
	}
	for name, want := range files {
		got, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
}

func Test_UntarGzipToDirCurrentDirectoryRejectsTraversal(t *testing.T) {
	archive := filepath.Join(t.TempDir(), "input.tar.gz")
	files := map[string]string{"../escape": "outside"}
	createTestTarGzipFile(t, archive, files)
	parent := t.TempDir()
	output := filepath.Join(parent, "output")
	if err := os.Mkdir(output, 0755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(output)
	if err := UntarGzipToDir(archive, "."); err == nil {
		t.Fatal("expected path traversal error")
	}
	if _, err := os.Stat(filepath.Join(parent, "escape")); !os.IsNotExist(err) {
		t.Fatalf("archive escaped destination: %v", err)
	}
}
