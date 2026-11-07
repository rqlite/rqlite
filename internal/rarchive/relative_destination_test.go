package rarchive

import (
	"os"
	"path/filepath"
	"testing"
)

func Test_ExtractToCurrentDirectory(t *testing.T) {
	for _, format := range []string{"zip", "tar.gz"} {
		t.Run(format, func(t *testing.T) {
			archive := filepath.Join(t.TempDir(), "input."+format)
			files := map[string]string{"file.txt": "saved", "sub/nested.txt": "nested"}
			var extract func(string, string) error
			if format == "zip" {
				data, err := createZipInMemory(files)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(archive, data, 0600); err != nil {
					t.Fatal(err)
				}
				extract = UnzipToDir
			} else {
				createTestTarGzipFile(t, archive, files)
				extract = UntarGzipToDir
			}
			t.Chdir(t.TempDir())
			if err := extract(archive, "."); err != nil {
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
		})
	}
}

func Test_ExtractToCurrentDirectoryRejectsTraversal(t *testing.T) {
	for _, format := range []string{"zip", "tar.gz"} {
		t.Run(format, func(t *testing.T) {
			archive := filepath.Join(t.TempDir(), "input."+format)
			files := map[string]string{"../escape": "outside"}
			var extract func(string, string) error
			if format == "zip" {
				data, err := createZipInMemory(files)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(archive, data, 0600); err != nil {
					t.Fatal(err)
				}
				extract = UnzipToDir
			} else {
				createTestTarGzipFile(t, archive, files)
				extract = UntarGzipToDir
			}
			parent := t.TempDir()
			output := filepath.Join(parent, "output")
			if err := os.Mkdir(output, 0755); err != nil {
				t.Fatal(err)
			}
			t.Chdir(output)
			if err := extract(archive, "."); err == nil {
				t.Fatal("expected path traversal error")
			}
			if _, err := os.Stat(filepath.Join(parent, "escape")); !os.IsNotExist(err) {
				t.Fatalf("archive escaped destination: %v", err)
			}
		})
	}
}
