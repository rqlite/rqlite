package fsutil

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func Test_CopyDir_SourceDoesNotExist(t *testing.T) {
	dir := t.TempDir()
	if err := CopyDir(filepath.Join(dir, "nope"), filepath.Join(dir, "dst")); err == nil {
		t.Fatal("expected error when source does not exist")
	}
}

func Test_CopyDir_SourceNotADirectory(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "file")
	mustWriteFile(t, src, "hello", 0644)

	err := CopyDir(src, filepath.Join(dir, "dst"))
	if err == nil {
		t.Fatal("expected error when source is not a directory")
	}
	if exp, got := "source is not a directory", err.Error(); exp != got {
		t.Fatalf("wrong error, exp %q, got %q", exp, got)
	}
}

func Test_CopyDir_DestinationExists(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	mustMkdir(t, src)
	mustMkdir(t, dst)

	err := CopyDir(src, dst)
	if err == nil {
		t.Fatal("expected error when destination already exists")
	}
	if exp, got := "destination already exists", err.Error(); exp != got {
		t.Fatalf("wrong error, exp %q, got %q", exp, got)
	}

	// A file at the destination path should also be rejected.
	dstFile := filepath.Join(dir, "dst-file")
	mustWriteFile(t, dstFile, "x", 0644)
	if err := CopyDir(src, dstFile); err == nil {
		t.Fatal("expected error when destination file already exists")
	}
}

func Test_CopyDir_Empty(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	mustMkdir(t, src)

	if err := CopyDir(src, dst); err != nil {
		t.Fatalf("failed to copy empty directory: %s", err)
	}
	if !DirExists(dst) {
		t.Fatal("destination directory was not created")
	}
	empty, err := DirIsEmpty(dst)
	if err != nil {
		t.Fatalf("failed to check if destination is empty: %s", err)
	}
	if !empty {
		t.Fatal("expected destination directory to be empty")
	}
}

func Test_CopyDir_Recursive(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	mustMkdir(t, src)
	mustMkdir(t, filepath.Join(src, "sub"))
	mustMkdir(t, filepath.Join(src, "sub", "subsub"))
	mustMkdir(t, filepath.Join(src, "empty"))
	mustWriteFile(t, filepath.Join(src, "a.txt"), "aaa", 0644)
	mustWriteFile(t, filepath.Join(src, "sub", "b.txt"), "bbb", 0644)
	mustWriteFile(t, filepath.Join(src, "sub", "subsub", "c.txt"), "ccc", 0644)

	if err := CopyDir(src, dst); err != nil {
		t.Fatalf("failed to copy directory: %s", err)
	}

	for _, f := range []string{"a.txt", filepath.Join("sub", "b.txt"), filepath.Join("sub", "subsub", "c.txt")} {
		srcPath := filepath.Join(src, f)
		dstPath := filepath.Join(dst, f)
		if !FileExists(dstPath) {
			t.Fatalf("file %s was not copied", f)
		}
		if !FilesIdentical(srcPath, dstPath) {
			t.Fatalf("contents of %s do not match", f)
		}
	}

	if !DirExists(filepath.Join(dst, "empty")) {
		t.Fatal("empty subdirectory was not copied")
	}
}

func Test_CopyDir_PreservesFileModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not meaningfully preserved on Windows")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	mustMkdir(t, src)
	mustWriteFile(t, filepath.Join(src, "ro.txt"), "read-only", 0444)
	mustWriteFile(t, filepath.Join(src, "exec.sh"), "#!/bin/sh\n", 0755)

	if err := CopyDir(src, dst); err != nil {
		t.Fatalf("failed to copy directory: %s", err)
	}

	for name, exp := range map[string]os.FileMode{"ro.txt": 0444, "exec.sh": 0755} {
		info, err := os.Stat(filepath.Join(dst, name))
		if err != nil {
			t.Fatalf("failed to stat copied file %s: %s", name, err)
		}
		if got := info.Mode().Perm(); got != exp {
			t.Fatalf("wrong mode for %s, exp %v, got %v", name, exp, got)
		}
	}
}

func Test_CopyDir_SkipsSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation typically requires elevated privileges on Windows")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	mustMkdir(t, src)
	mustWriteFile(t, filepath.Join(src, "real.txt"), "real", 0644)
	if err := os.Symlink(filepath.Join(src, "real.txt"), filepath.Join(src, "link.txt")); err != nil {
		t.Fatalf("failed to create symlink: %s", err)
	}

	if err := CopyDir(src, dst); err != nil {
		t.Fatalf("failed to copy directory: %s", err)
	}

	if !FileExists(filepath.Join(dst, "real.txt")) {
		t.Fatal("regular file was not copied")
	}
	if PathExists(filepath.Join(dst, "link.txt")) {
		t.Fatal("symlink should have been skipped")
	}
}

func Test_CopyDir_OverwritesNothingInSource(t *testing.T) {
	// Copying should leave the source tree untouched.
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	mustMkdir(t, src)
	mustWriteFile(t, filepath.Join(src, "a.txt"), "aaa", 0644)

	if err := CopyDir(src, dst); err != nil {
		t.Fatalf("failed to copy directory: %s", err)
	}
	if err := os.WriteFile(filepath.Join(dst, "a.txt"), []byte("changed"), 0644); err != nil {
		t.Fatalf("failed to modify copied file: %s", err)
	}

	b, err := os.ReadFile(filepath.Join(src, "a.txt"))
	if err != nil {
		t.Fatalf("failed to read source file: %s", err)
	}
	if exp, got := "aaa", string(b); exp != got {
		t.Fatalf("source file was modified, exp %q, got %q", exp, got)
	}
}

func Test_copyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	mustWriteFile(t, src, "hello, world", 0644)

	if err := copyFile(src, dst); err != nil {
		t.Fatalf("failed to copy file: %s", err)
	}
	if !FilesIdentical(src, dst) {
		t.Fatal("copied file does not match source")
	}

	// Copying over an existing file should replace its contents.
	mustWriteFile(t, src, "new", 0644)
	if err := copyFile(src, dst); err != nil {
		t.Fatalf("failed to overwrite file: %s", err)
	}
	if !FilesIdentical(src, dst) {
		t.Fatal("overwritten file does not match source")
	}

	if err := copyFile(filepath.Join(dir, "nope"), dst); err == nil {
		t.Fatal("expected error when source does not exist")
	}
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create directory %s: %s", path, err)
	}
}

func mustWriteFile(t *testing.T, path, contents string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), mode); err != nil {
		t.Fatalf("failed to write file %s: %s", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("failed to chmod file %s: %s", path, err)
	}
}

func Test_CopyDir_NoTempDirLeftBehind(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	mustMkdir(t, src)
	mustMkdir(t, filepath.Join(src, "sub"))
	mustWriteFile(t, filepath.Join(src, "sub", "a.txt"), "aaa", 0644)

	if err := CopyDir(src, dst); err != nil {
		t.Fatalf("failed to copy directory: %s", err)
	}
	if PathExists(dst + tmpSuffix) {
		t.Fatal("temporary directory was left behind after a successful copy")
	}

	// Nothing but the destination should have appeared alongside the source.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read parent directory: %s", err)
	}
	if len(entries) != 2 {
		t.Fatalf("wrong number of entries in parent directory, exp 2, got %d", len(entries))
	}
}

func Test_CopyDir_RemovesStaleTempDir(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	mustMkdir(t, src)
	mustWriteFile(t, filepath.Join(src, "a.txt"), "aaa", 0644)

	// Simulate the remains of an earlier copy which was interrupted before it
	// could be renamed into place.
	mustMkdir(t, dst+tmpSuffix)
	mustWriteFile(t, filepath.Join(dst+tmpSuffix, "stale.txt"), "stale", 0644)

	if err := CopyDir(src, dst); err != nil {
		t.Fatalf("failed to copy directory over stale temporary directory: %s", err)
	}
	if PathExists(filepath.Join(dst, "stale.txt")) {
		t.Fatal("stale temporary file was carried into the destination")
	}
	if !FilesIdentical(filepath.Join(src, "a.txt"), filepath.Join(dst, "a.txt")) {
		t.Fatal("contents of a.txt do not match")
	}
	if PathExists(dst + tmpSuffix) {
		t.Fatal("temporary directory was left behind after a successful copy")
	}
}

func Test_CopyDir_FailureLeavesNothingBehind(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory permissions do not block reads on Windows")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	mustMkdir(t, src)
	mustWriteFile(t, filepath.Join(src, "a.txt"), "aaa", 0644)
	// An unreadable subdirectory makes the copy fail part-way through, after
	// a.txt has already been staged.
	blocked := filepath.Join(src, "blocked")
	mustMkdir(t, blocked)
	if err := os.Chmod(blocked, 0000); err != nil {
		t.Fatalf("failed to chmod directory: %s", err)
	}
	defer os.Chmod(blocked, 0755)

	if err := CopyDir(src, dst); err == nil {
		t.Fatal("expected copy of an unreadable tree to fail")
	}

	// The copy failed, so the destination must not exist at all -- not even
	// partially -- and no temporary directory may be left behind.
	if PathExists(dst) {
		t.Fatal("destination exists after a failed copy")
	}
	if PathExists(dst + tmpSuffix) {
		t.Fatal("temporary directory was left behind after a failed copy")
	}
}
