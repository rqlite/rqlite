package fsutil

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func Test_CopyDirPreservesSourceInStagingDirectory(t *testing.T) {
	for _, nested := range []bool{false, true} {
		name := "staging directory"
		if nested {
			name = "inside staging directory"
		}
		t.Run(name, func(t *testing.T) {
			dst := filepath.Join(t.TempDir(), "destination")
			src := dst + tmpSuffix
			if nested {
				src = filepath.Join(src, "source")
			}
			if err := os.MkdirAll(src, 0755); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(src, "data")
			if err := os.WriteFile(file, []byte("keep me"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := CopyDir(src, dst); err == nil {
				t.Fatal("expected staging/source overlap to be rejected")
			}
			got, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("copy removed the source: %v", err)
			}
			if string(got) != "keep me" {
				t.Fatalf("source changed: %q", got)
			}
		})
	}
}

func Test_CopyDirPreservesSymlinkedSourceInStagingDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require privileges on Windows")
	}
	dir := t.TempDir()
	dst := filepath.Join(dir, "destination")
	source := filepath.Join(dst+tmpSuffix, "source")
	if err := os.MkdirAll(source, 0755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(source, "data")
	if err := os.WriteFile(file, []byte("keep me"), 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias")
	if err := os.Symlink(source, alias); err != nil {
		t.Fatal(err)
	}
	if err := CopyDir(alias, dst); err == nil {
		t.Fatal("expected staging/source overlap to be rejected")
	}
	if got, err := os.ReadFile(file); err != nil || string(got) != "keep me" {
		t.Fatalf("source changed: %q, %v", got, err)
	}
}
