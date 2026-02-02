package plan

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExecutor_Rename(t *testing.T) {
	e := NewExecutor()
	tmpDir := t.TempDir()

	src := filepath.Join(tmpDir, "src")
	dst := filepath.Join(tmpDir, "dst")

	// Create source file
	if err := os.WriteFile(src, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Test Rename
	if err := e.Rename(src, dst); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Verify rename
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("source file still exists")
	}
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		t.Fatalf("destination file does not exist")
	}

	// Test Idempotency: Rename again (src missing, dst exists) -> should succeed
	if err := e.Rename(src, dst); err != nil {
		t.Fatalf("Rename idempotency failed: %v", err)
	}

	// Test Idempotency: Rename non-existent file to existing destination -> should succeed
	nonExist := filepath.Join(tmpDir, "nonexist")
	if err := e.Rename(nonExist, dst); err != nil {
		t.Fatalf("Rename non-existent file to existing destination should succeed: %v", err)
	}

	// Now test renaming non-existent to another non-existent
	if err := e.Rename(nonExist, filepath.Join(tmpDir, "other")); err == nil {
		t.Fatalf("Rename non-existent file to non-existent destination should fail")
	}
}

func TestExecutor_Remove(t *testing.T) {
	e := NewExecutor()
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "file")

	// Create file
	if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Test Remove
	if err := e.Remove(path); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify removal
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file still exists")
	}

	// Test Idempotency: Remove again -> should succeed
	if err := e.Remove(path); err != nil {
		t.Fatalf("Remove idempotency failed: %v", err)
	}
}

func TestExecutor_RemoveAll(t *testing.T) {
	e := NewExecutor()
	tmpDir := t.TempDir()
	dir := filepath.Join(tmpDir, "dir")

	// Create directory and file
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Test RemoveAll
	if err := e.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Verify removal
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("directory still exists")
	}

	// Test Idempotency: RemoveAll again -> should succeed
	if err := e.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll idempotency failed: %v", err)
	}
}
