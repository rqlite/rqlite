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

	// WAL file and CRC sidecar should exist.
	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("WAL file does not exist: %v", err)
	}
	if _, err := os.Stat(walPath + crcSuffix); err != nil {
		t.Fatalf("CRC file does not exist: %v", err)
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
		srcData, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("failed to read source: %v", err)
		}
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
	srcData, err := os.ReadFile("testdata/db-and-wals/wal-01")
	if err != nil {
		t.Fatalf("failed to read source WAL: %v", err)
	}
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
		srcData, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("failed to read source: %v", err)
		}
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
		if _, err := os.Stat(dstWAL); err != nil {
			t.Fatalf("moved WAL file %s does not exist: %v", name, err)
		}
		if _, err := os.Stat(dstWAL + crcSuffix); err != nil {
			t.Fatalf("moved CRC file for %s does not exist: %v", name, err)
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
		srcData, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("failed to read source: %v", err)
		}
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
	if len(files) != 3 {
		t.Fatalf("expected 3 WAL files, got %d", len(files))
	}

	if err := sd.Validate(); err != nil {
		t.Fatalf("Validate with 3 valid WALs: %v", err)
	}
}
