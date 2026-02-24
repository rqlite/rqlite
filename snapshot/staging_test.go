package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v10/internal/rsum"
)

func Test_StagingDir_CreateWALAndValidate(t *testing.T) {
	dir := t.TempDir()
	sd := NewStagingDir(dir)

	if sd.Path() != dir {
		t.Fatalf("expected path %s, got %s", dir, sd.Path())
	}

	// Copy a real WAL file through the writer so it produces a valid SQLite WAL.
	srcData := mustReadFile(t, "testdata/db-and-wals/wal-01")

	w, walPath, err := sd.CreateWAL()
	if err != nil {
		t.Fatalf("CreateWAL: %v", err)
	}
	if _, err := w.Write(srcData); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// WAL file and CRC sidecar should exist.
	if !fileExists(walPath) {
		t.Fatalf("WAL file does not exist: %v", walPath)
	}
	if !fileExists(walPath + crcSuffix) {
		t.Fatalf("CRC file does not exist: %v", walPath+crcSuffix)
	}

	// Validate should pass.
	if err := sd.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func Test_StagingDir_WALFiles(t *testing.T) {
	dir := t.TempDir()
	sd := NewStagingDir(dir)

	// Empty directory.
	files, err := sd.WALFiles()
	if err != nil {
		t.Fatalf("WALFiles on empty dir: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected 0 files, got %d", len(files))
	}

	// Create two WAL files.
	for _, src := range []string{"testdata/db-and-wals/wal-00", "testdata/db-and-wals/wal-01"} {
		srcData := mustReadFile(t, src)
		w, _, err := sd.CreateWAL()
		if err != nil {
			t.Fatalf("CreateWAL: %v", err)
		}
		if _, err := w.Write(srcData); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	files, err = sd.WALFiles()
	if err != nil {
		t.Fatalf("WALFiles: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}
}

func Test_StagingDir_ValidateFailsCRCMismatch(t *testing.T) {
	dir := t.TempDir()
	sd := NewStagingDir(dir)

	// Write a valid WAL file.
	srcData := mustReadFile(t, "testdata/db-and-wals/wal-01")
	w, walPath, err := sd.CreateWAL()
	if err != nil {
		t.Fatalf("CreateWAL: %v", err)
	}
	if _, err := w.Write(srcData); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt the CRC file.
	if err := rsum.WriteCRC32SumFile(walPath+crcSuffix, 0xdeadbeef, rsum.Sync); err != nil {
		t.Fatalf("failed to write bad CRC: %v", err)
	}

	if err := sd.Validate(); err == nil {
		t.Fatal("expected Validate to fail on CRC mismatch")
	}
}

func Test_StagingDir_ValidateFailsMissingCRC(t *testing.T) {
	dir := t.TempDir()
	sd := NewStagingDir(dir)

	// Copy a WAL file manually without a CRC sidecar.
	walPath := filepath.Join(dir, "00000000000000000001.wal")
	srcData := mustReadFile(t, "testdata/db-and-wals/wal-01")
	if err := os.WriteFile(walPath, srcData, 0644); err != nil {
		t.Fatalf("failed to write WAL file: %v", err)
	}

	if err := sd.Validate(); err == nil {
		t.Fatal("expected Validate to fail on missing CRC file")
	}
}

func Test_StagingDir_MoveWALFilesTo(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	sd := NewStagingDir(srcDir)

	// Create two WAL files.
	var walPaths []string
	for _, src := range []string{"testdata/db-and-wals/wal-00", "testdata/db-and-wals/wal-01"} {
		srcData := mustReadFile(t, src)
		w, walPath, err := sd.CreateWAL()
		if err != nil {
			t.Fatalf("CreateWAL: %v", err)
		}
		walPaths = append(walPaths, walPath)
		if _, err := w.Write(srcData); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Moving to a non-existent directory should fail.
	if err := sd.MoveWALFilesTo(filepath.Join(dstDir, "nonexistent")); err == nil {
		t.Fatal("expected MoveWALFilesTo to fail with non-existent destination")
	}

	// Moving to a path that exists, but is not a directory, should fail.
	nonDirPath := filepath.Join(t.TempDir(), "not-a-dir")
	mustTouchFile(t, nonDirPath)
	if err := sd.MoveWALFilesTo(nonDirPath); err == nil {
		t.Fatal("expected MoveWALFilesTo to fail with non-directory destination")
	}

	// Now, move the files.
	if err := sd.MoveWALFilesTo(dstDir); err != nil {
		t.Fatalf("MoveWALFilesTo: %v", err)
	}

	// Source directory should have no WAL files left.
	remaining, err := sd.WALFiles()
	if err != nil {
		t.Fatalf("WALFiles after move: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected 0 remaining files, got %d", len(remaining))
	}

	// Destination should have both WAL files and their CRC sidecars.
	for _, wp := range walPaths {
		name := filepath.Base(wp)
		dstWAL := filepath.Join(dstDir, name)
		if !fileExists(dstWAL) {
			t.Fatalf("moved WAL file %s does not exist", name)
		}
		if !fileExists(dstWAL + crcSuffix) {
			t.Fatalf("moved CRC file for %s does not exist", name)
		}
	}
}

func Test_StagingDir_ValidateMultipleWALs(t *testing.T) {
	dir := t.TempDir()
	sd := NewStagingDir(dir)

	// Create three valid WAL files.
	for _, src := range []string{
		"testdata/db-and-wals/wal-00",
		"testdata/db-and-wals/wal-01",
		"testdata/db-and-wals/wal-02",
	} {
		srcData := mustReadFile(t, src)
		w, _, err := sd.CreateWAL()
		if err != nil {
			t.Fatalf("CreateWAL: %v", err)
		}
		if _, err := w.Write(srcData); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	files, err := sd.WALFiles()
	if err != nil {
		t.Fatalf("WALFiles: %v", err)
	}
	if exp, got := 3, len(files); exp != got {
		t.Fatalf("expected %d WAL files, got %d", exp, got)
	} else if err := sd.Validate(); err != nil {
		t.Fatalf("Validate with %d valid WALs: %v", exp, err)
	}
}

func Test_WALWriter_CancelActive(t *testing.T) {
	dir := t.TempDir()
	sd := NewStagingDir(dir)

	w, walPath, err := sd.CreateWAL()
	if err != nil {
		t.Fatalf("CreateWAL: %v", err)
	}

	// Write some data but don't Close.
	if _, err := w.Write([]byte("partial data")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Cancel should remove the partial WAL file.
	w.Cancel()

	if _, err := os.Stat(walPath); !os.IsNotExist(err) {
		t.Fatalf("expected WAL file to be removed after Cancel, got err: %v", err)
	}

	// Staging directory should have no WAL files.
	files, err := sd.WALFiles()
	if err != nil {
		t.Fatalf("WALFiles: %v", err)
	}
	if exp, got := 0, len(files); exp != got {
		t.Fatalf("expected %d WAL files after Cancel, got %d", exp, got)
	}
}

func Test_WALWriter_CancelAfterClose(t *testing.T) {
	dir := t.TempDir()
	sd := NewStagingDir(dir)

	srcData, err := os.ReadFile("testdata/db-and-wals/wal-01")
	if err != nil {
		t.Fatalf("failed to read source WAL: %v", err)
	}

	w, walPath, err := sd.CreateWAL()
	if err != nil {
		t.Fatalf("CreateWAL: %v", err)
	}
	if _, err := w.Write(srcData); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Cancel after Close should be a no-op — files must survive.
	w.Cancel()

	if !fileExists(walPath) {
		t.Fatalf("WAL file should still exist after Cancel on closed writer")
	}
	if !fileExists(walPath + crcSuffix) {
		t.Fatalf("CRC file should still exist after Cancel on closed writer")
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	srcData, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read source: %v", err)
	}
	return srcData
}

func mustTouchFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
}
