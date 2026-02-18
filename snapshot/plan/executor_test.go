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

func TestExecutor_Checkpoint(t *testing.T) {
	t.Run("no WALs", func(t *testing.T) {
		tmpDir := t.TempDir()
		dstDB := filepath.Join(tmpDir, "main.db")
		mustCopyFile("testdata/main.db", dstDB)

		e := NewExecutor()
		n, err := e.Checkpoint(dstDB, nil)
		if err != nil {
			t.Fatalf("Checkpoint with no WALs failed: %v", err)
		}
		if n != 0 {
			t.Fatalf("Expected 0 checkpointed WALs, got %d", n)
		}
	})

	t.Run("single WAL", func(t *testing.T) {
		tmpDir := t.TempDir()

		dstDB := filepath.Join(tmpDir, "main.db")
		mustCopyFile("testdata/main.db", dstDB)
		wal := filepath.Join(tmpDir, "wal-00")
		mustCopyFile("testdata/wal-00", wal)

		e := NewExecutor()
		n, err := e.Checkpoint(dstDB, []string{wal})
		if err != nil {
			t.Fatalf("Checkpoint with single WAL failed: %v", err)
		}
		if n != 1 {
			t.Fatalf("Expected 1 checkpointed WAL, got %d", n)
		}
	})

	t.Run("single WAL DB missing", func(t *testing.T) {
		tmpDir := t.TempDir()

		wal := filepath.Join(tmpDir, "wal-00")
		mustCopyFile("testdata/wal-00", wal)

		e := NewExecutor()
		_, err := e.Checkpoint("non-existing", []string{wal})
		if err == nil {
			t.Fatalf("Checkpoint with single WAL and missing DB should fail")
		}
	})

	t.Run("multiple WAL", func(t *testing.T) {
		tmpDir := t.TempDir()

		dstDB := filepath.Join(tmpDir, "main.db")
		mustCopyFile("testdata/main.db", dstDB)
		wal0 := filepath.Join(tmpDir, "wal-00")
		mustCopyFile("testdata/wal-00", wal0)
		wal1 := filepath.Join(tmpDir, "wal-01")
		mustCopyFile("testdata/wal-01", wal1)

		e := NewExecutor()
		n, err := e.Checkpoint(dstDB, []string{wal0, wal1})
		if err != nil {
			t.Fatalf("Checkpoint with multiple WALs failed: %v", err)
		}
		if n != 2 {
			t.Fatalf("Expected 2 checkpointed WALs, got %d", n)
		}
	})

	// Test checkpointing when one WAL has already been checkpointed but the plan
	// still includes it. This could happen if we crash after checkpointing one WAL
	// but before we checkpointe the second.
	t.Run("multiple WAL one checkpointed", func(t *testing.T) {
		tmpDir := t.TempDir()

		dstDB := filepath.Join(tmpDir, "main.db")
		mustCopyFile("testdata/main.db", dstDB)
		wal0 := filepath.Join(tmpDir, "wal-00")
		mustCopyFile("testdata/wal-00", wal0)
		wal1 := filepath.Join(tmpDir, "wal-01")
		mustCopyFile("testdata/wal-01", wal1)

		e := NewExecutor()

		n, err := e.Checkpoint(dstDB, []string{wal0})
		if err != nil {
			t.Fatalf("Checkpoint with single WAL failed: %v", err)
		}
		if n != 1 {
			t.Fatalf("Expected 1 checkpointed WAL, got %d", n)
		}

		// wal0 has been checkpointed and removed.

		n, err = e.Checkpoint(dstDB, []string{wal0, wal1})
		if err != nil {
			t.Fatalf("Checkpoint with one WAL previously checkpointed %v", err)
		}
		if n != 1 {
			t.Fatalf("Expected 1 checkpointed WAL, got %d", n)
		}
	})
}

func TestExecutor_WriteMeta(t *testing.T) {
	e := NewExecutor()
	tmpDir := t.TempDir()
	dir := filepath.Join(tmpDir, "snap")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	data := []byte(`{"id":"test-snap","index":100,"term":2}`)

	// Write meta.
	if err := e.WriteMeta(dir, data); err != nil {
		t.Fatalf("WriteMeta failed: %v", err)
	}

	// Verify file was written.
	got, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatalf("failed to read meta.json: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("meta.json content mismatch: got %s, want %s", got, data)
	}

	// Test idempotency: write again with different data.
	data2 := []byte(`{"id":"test-snap","index":200,"term":3}`)
	if err := e.WriteMeta(dir, data2); err != nil {
		t.Fatalf("WriteMeta overwrite failed: %v", err)
	}
	got, _ = os.ReadFile(filepath.Join(dir, "meta.json"))
	if string(got) != string(data2) {
		t.Fatalf("meta.json not overwritten: got %s, want %s", got, data2)
	}

	// Test idempotency: dir doesn't exist (already renamed by a later plan step).
	if err := e.WriteMeta(filepath.Join(tmpDir, "nonexistent"), data); err != nil {
		t.Fatalf("WriteMeta should succeed when dir doesn't exist: %v", err)
	}
}

func TestExecutor_MkdirAll(t *testing.T) {
	e := NewExecutor()
	tmpDir := t.TempDir()
	dir := filepath.Join(tmpDir, "a", "b", "c")

	// Create nested directory.
	if err := e.MkdirAll(dir); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Verify directory exists.
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("directory does not exist: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected a directory")
	}

	// Test idempotency: MkdirAll again -> should succeed.
	if err := e.MkdirAll(dir); err != nil {
		t.Fatalf("MkdirAll idempotency failed: %v", err)
	}
}

func TestExecutor_CopyFile(t *testing.T) {
	e := NewExecutor()
	tmpDir := t.TempDir()

	src := filepath.Join(tmpDir, "src")
	dst := filepath.Join(tmpDir, "dst")
	content := []byte("hello world")

	// Create source file with specific permissions.
	if err := os.WriteFile(src, content, 0600); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Copy file.
	if err := e.CopyFile(src, dst); err != nil {
		t.Fatalf("CopyFile failed: %v", err)
	}

	// Verify copy.
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("failed to read destination file: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("content mismatch: got %q, want %q", got, content)
	}

	// Verify permissions are preserved.
	srcInfo, _ := os.Stat(src)
	dstInfo, _ := os.Stat(dst)
	if srcInfo.Mode().Perm() != dstInfo.Mode().Perm() {
		t.Fatalf("permissions mismatch: src %v, dst %v", srcInfo.Mode().Perm(), dstInfo.Mode().Perm())
	}

	// Test idempotency: remove src, CopyFile again -> should succeed.
	os.Remove(src)
	if err := e.CopyFile(src, dst); err != nil {
		t.Fatalf("CopyFile idempotency failed: %v", err)
	}

	// Verify dst still has the content.
	got, err = os.ReadFile(dst)
	if err != nil {
		t.Fatalf("failed to read destination after idempotent call: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("content changed after idempotent call: got %q, want %q", got, content)
	}

	// Test error: both src and dst missing.
	os.Remove(dst)
	if err := e.CopyFile(src, dst); err == nil {
		t.Fatalf("CopyFile should fail when both src and dst are missing")
	}
}

func mustCopyFile(src, dst string) {
	input, err := os.ReadFile(src)
	if err != nil {
		panic("failed to read file")
	}
	if err := os.WriteFile(dst, input, 0644); err != nil {
		panic("failed to write file")
	}
}
