package snapshot

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v10/internal/rsum"
	"github.com/rqlite/rqlite/v10/snapshot/sidecar"
)

// createTestFile creates a data file with the given content and a matching
// CRC32 sidecar file. It returns a ChecksummedFile ready for use with
// CRCChecker.
func createTestFile(t *testing.T, dir, name, content string) *ChecksummedFile {
	t.Helper()

	dataPath := filepath.Join(dir, name)
	if err := os.WriteFile(dataPath, []byte(content), 0644); err != nil {
		t.Fatalf("writing data file: %v", err)
	}

	sum, err := rsum.CRC32(dataPath)
	if err != nil {
		t.Fatalf("computing CRC32: %v", err)
	}

	crcPath := dataPath + crcSuffix
	if err := sidecar.WriteFile(crcPath, sum, rsum.NoSync); err != nil {
		t.Fatalf("writing CRC sidecar: %v", err)
	}

	return &ChecksummedFile{Path: dataPath, CRC32: sum}
}

// createCorruptedTestFile creates a data file and a CRC32 sidecar that does
// NOT match the file content.
func createCorruptedTestFile(t *testing.T, dir, name, content string) *ChecksummedFile {
	t.Helper()

	dataPath := filepath.Join(dir, name)
	if err := os.WriteFile(dataPath, []byte(content), 0644); err != nil {
		t.Fatalf("writing data file: %v", err)
	}

	// Write a bogus CRC that won't match.
	badCRC := uint32(0xDEADBEEF)
	crcPath := dataPath + crcSuffix
	if err := sidecar.WriteFile(crcPath, badCRC, rsum.NoSync); err != nil {
		t.Fatalf("writing CRC sidecar: %v", err)
	}

	return &ChecksummedFile{Path: dataPath, CRC32: badCRC}
}

func Test_CRCChecker_NoFiles(t *testing.T) {
	checker := NewCRCChecker()
	if err := <-checker.Check(); err != nil {
		t.Fatalf("expected nil error for empty checker, got: %v", err)
	}
}

func Test_CRCChecker_OneFilePass(t *testing.T) {
	dir := t.TempDir()
	hf := createTestFile(t, dir, "data.db", "hello world")

	checker := NewCRCChecker()
	checker.Add(hf)
	if err := <-checker.Check(); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func Test_CRCChecker_TwoFilesPass(t *testing.T) {
	dir := t.TempDir()
	hf1 := createTestFile(t, dir, "data.db", "hello world")
	hf2 := createTestFile(t, dir, "data.wal", "wal contents")

	checker := NewCRCChecker()
	checker.Add(hf1)
	checker.Add(hf2)
	if err := <-checker.Check(); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func Test_CRCChecker_OneFileFail(t *testing.T) {
	dir := t.TempDir()
	hf := createCorruptedTestFile(t, dir, "data.db", "hello world")

	checker := NewCRCChecker()
	checker.Add(hf)
	err := <-checker.Check()
	if err == nil {
		t.Fatal("expected error for corrupted file, got nil")
	}
}

func Test_CRCChecker_TwoFilesBothFail(t *testing.T) {
	dir := t.TempDir()
	hf1 := createCorruptedTestFile(t, dir, "data.db", "hello world")
	hf2 := createCorruptedTestFile(t, dir, "data.wal", "wal contents")

	checker := NewCRCChecker()
	checker.Add(hf1)
	checker.Add(hf2)
	err := <-checker.Check()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func Test_CRCChecker_OnePassOneFail(t *testing.T) {
	dir := t.TempDir()
	hf1 := createTestFile(t, dir, "good.db", "valid data")
	hf2 := createCorruptedTestFile(t, dir, "bad.db", "corrupt data")

	checker := NewCRCChecker()
	checker.Add(hf1)
	checker.Add(hf2)
	err := <-checker.Check()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func Test_CRCChecker_MissingFile(t *testing.T) {
	checker := NewCRCChecker()
	checker.Add(&ChecksummedFile{
		Path:  "/nonexistent/file.db",
		CRC32: 0x12345678,
	})
	err := <-checker.Check()
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func Test_CRCChecker_ManyFilesAllPass(t *testing.T) {
	dir := t.TempDir()
	checker := NewCRCChecker()
	for i := 0; i < 20; i++ {
		name := filepath.Base(t.TempDir()) + ".wal"
		hf := createTestFile(t, dir, name, strings.Repeat("data", i+1))
		checker.Add(hf)
	}
	if err := <-checker.Check(); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func Test_CRCChecker_ManyFilesOneFails(t *testing.T) {
	dir := t.TempDir()
	checker := NewCRCChecker()
	for i := 0; i < 19; i++ {
		name := filepath.Base(t.TempDir()) + ".wal"
		hf := createTestFile(t, dir, name, strings.Repeat("data", i+1))
		checker.Add(hf)
	}
	hf := createCorruptedTestFile(t, dir, "corrupt.wal", "bad data")
	checker.Add(hf)

	err := <-checker.Check()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
