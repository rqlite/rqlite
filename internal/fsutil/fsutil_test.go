package fsutil

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func Test_PathExists(t *testing.T) {
	dir := t.TempDir()

	// Existing file.
	p := filepath.Join(dir, "file")
	if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if !PathExists(p) {
		t.Fatal("expected PathExists to return true for existing file")
	}

	// Nonexistent path.
	if PathExists(filepath.Join(dir, "nope")) {
		t.Fatal("expected PathExists to return false for nonexistent path")
	}

	// Broken symlink — Lstat should still detect it.
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "link")
	if err := os.WriteFile(target, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if !PathExists(link) {
		t.Fatal("expected PathExists to return true for broken symlink")
	}
}

func Test_FileExists(t *testing.T) {
	dir := t.TempDir()

	// Regular file.
	p := filepath.Join(dir, "file")
	if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if !FileExists(p) {
		t.Fatal("expected FileExists to return true for regular file")
	}

	// Directory should return false.
	if FileExists(dir) {
		t.Fatal("expected FileExists to return false for directory")
	}

	// Nonexistent path.
	if FileExists(filepath.Join(dir, "nope")) {
		t.Fatal("expected FileExists to return false for nonexistent path")
	}
}

func Test_DirExists(t *testing.T) {
	dir := t.TempDir()

	if !DirExists(dir) {
		t.Fatal("expected DirExists to return true for existing directory")
	}

	// Regular file should return false.
	p := filepath.Join(dir, "file")
	if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if DirExists(p) {
		t.Fatal("expected DirExists to return false for regular file")
	}

	// Nonexistent path.
	if DirExists(filepath.Join(dir, "nope")) {
		t.Fatal("expected DirExists to return false for nonexistent path")
	}
}

func Test_PathExistsWithData(t *testing.T) {
	dir := t.TempDir()

	// File with data.
	p := filepath.Join(dir, "file")
	if err := os.WriteFile(p, []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}
	if !PathExistsWithData(p) {
		t.Fatal("expected PathExistsWithData to return true for file with data")
	}

	// Empty file.
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, nil, 0644); err != nil {
		t.Fatal(err)
	}
	if PathExistsWithData(empty) {
		t.Fatal("expected PathExistsWithData to return false for empty file")
	}

	// Nonexistent path.
	if PathExistsWithData(filepath.Join(dir, "nope")) {
		t.Fatal("expected PathExistsWithData to return false for nonexistent path")
	}
}

func Test_FileSize(t *testing.T) {
	dir := t.TempDir()

	data := []byte("hello world") // 11 bytes
	p := filepath.Join(dir, "file")
	if err := os.WriteFile(p, data, 0644); err != nil {
		t.Fatal(err)
	}
	sz, err := FileSize(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sz != 11 {
		t.Fatalf("expected size 11, got %d", sz)
	}

	// Nonexistent path.
	_, err = FileSize(filepath.Join(dir, "nope"))
	if err == nil {
		t.Fatal("expected error for nonexistent path")
	}
}

func Test_FileSizeExists(t *testing.T) {
	dir := t.TempDir()

	data := make([]byte, 42)
	p := filepath.Join(dir, "file")
	if err := os.WriteFile(p, data, 0644); err != nil {
		t.Fatal(err)
	}
	sz, err := FileSizeExists(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sz != 42 {
		t.Fatalf("expected size 42, got %d", sz)
	}

	// Nonexistent path should return (0, nil).
	sz, err = FileSizeExists(filepath.Join(dir, "nope"))
	if err != nil {
		t.Fatalf("expected nil error for nonexistent path, got: %v", err)
	}
	if sz != 0 {
		t.Fatalf("expected size 0 for nonexistent path, got %d", sz)
	}
}

func Test_DirSize(t *testing.T) {
	dir := t.TempDir()

	// Two files in root: 10 + 20 = 30 bytes.
	if err := os.WriteFile(filepath.Join(dir, "a"), make([]byte, 10), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b"), make([]byte, 20), 0644); err != nil {
		t.Fatal(err)
	}

	// Nested file: 5 bytes.
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "c"), make([]byte, 5), 0644); err != nil {
		t.Fatal(err)
	}

	sz, err := DirSize(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sz != 35 {
		t.Fatalf("expected size 35, got %d", sz)
	}
}

func Test_ModTimeSize(t *testing.T) {
	dir := t.TempDir()

	data := make([]byte, 77)
	p := filepath.Join(dir, "file")
	if err := os.WriteFile(p, data, 0644); err != nil {
		t.Fatal(err)
	}

	mt, sz, err := ModTimeSize(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sz != 77 {
		t.Fatalf("expected size 77, got %d", sz)
	}
	if time.Since(mt) > 5*time.Second {
		t.Fatalf("mod time too old: %v", mt)
	}

	// Nonexistent path.
	_, _, err = ModTimeSize(filepath.Join(dir, "nope"))
	if err == nil {
		t.Fatal("expected error for nonexistent path")
	}
}

func Test_LastModified(t *testing.T) {
	dir := t.TempDir()

	p := filepath.Join(dir, "file")
	if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	mt1, err := LastModified(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(1 * time.Second)

	if err := os.WriteFile(p, []byte("y"), 0644); err != nil {
		t.Fatal(err)
	}

	mt2, err := LastModified(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mt2.After(mt1) {
		t.Fatalf("expected mod time to advance: first=%v second=%v", mt1, mt2)
	}

	// Nonexistent path.
	_, err = LastModified(filepath.Join(dir, "nope"))
	if err == nil {
		t.Fatal("expected error for nonexistent path")
	}
}

func Test_EnsureDirExists(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a", "b", "c")

	if err := EnsureDirExists(p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !DirExists(p) {
		t.Fatal("expected directory to be created")
	}

	// Idempotent — calling again should succeed.
	if err := EnsureDirExists(p); err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
}

func Test_DirIsEmpty(t *testing.T) {
	dir := t.TempDir()

	empty, err := DirIsEmpty(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !empty {
		t.Fatal("expected empty directory")
	}

	// Add a file.
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	empty, err = DirIsEmpty(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if empty {
		t.Fatal("expected non-empty directory")
	}
}

func Test_RemoveFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "file")

	if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := RemoveFile(p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if PathExists(p) {
		t.Fatal("expected file to be removed")
	}

	// Removing nonexistent file should not error.
	if err := RemoveFile(p); err != nil {
		t.Fatalf("expected no error for nonexistent file, got: %v", err)
	}
}

func Test_RemoveDirSync(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "file"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := RemoveDirSync(sub); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if DirExists(sub) {
		t.Fatal("expected directory to be removed")
	}
}

func Test_FilesIdentical(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a")
	b := filepath.Join(dir, "b")

	// Identical content.
	data := []byte("same content")
	if err := os.WriteFile(a, data, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, data, 0644); err != nil {
		t.Fatal(err)
	}
	if !FilesIdentical(a, b) {
		t.Fatal("expected files to be identical")
	}

	// Different content.
	if err := os.WriteFile(b, []byte("different"), 0644); err != nil {
		t.Fatal(err)
	}
	if FilesIdentical(a, b) {
		t.Fatal("expected files to differ")
	}
}
