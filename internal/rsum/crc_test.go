package rsum

import (
	"bytes"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func Test_CRC32Success(t *testing.T) {
	testContent := "test content"
	testFile := mustWriteTempFile(t, []byte(testContent))

	checksum, err := CRC32(testFile)
	if err != nil {
		t.Fatalf("CRC32 calculation failed: %v", err)
	}

	// Manually calculate the expected checksum
	castagnoliTable := crc32.MakeTable(crc32.Castagnoli)
	h := crc32.New(castagnoliTable)
	h.Write([]byte(testContent))
	expectedChecksum := h.Sum32()

	// Compare the returned checksum with the expected one
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %v, got %v", expectedChecksum, checksum)
	}
}

func Test_CRC32FileNotFound(t *testing.T) {
	_, err := CRC32("nonexistentfile.txt")
	if err == nil {
		t.Error("Expected an error for non-existent file, got none")
	}
}

func Test_CRC32EmptyFile(t *testing.T) {
	testFile := mustWriteTempFile(t, []byte(""))

	checksum, err := CRC32(testFile)
	if err != nil {
		t.Fatalf("CRC32 calculation failed: %v", err)
	}

	// The CRC32 checksum for an empty input should be 0.
	expectedChecksum := crc32.NewIEEE().Sum32()
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %v for empty file, got %v", expectedChecksum, checksum)
	}
}

func Test_CRC32Writer(t *testing.T) {
	testContent := []byte("test content")

	var buf bytes.Buffer
	cw := NewCRC32Writer(&buf)

	n, err := cw.Write(testContent)
	if err != nil {
		t.Fatalf("CRC32Writer write failed: %v", err)
	}
	if n != len(testContent) {
		t.Fatalf("expected to write %d bytes, wrote %d", len(testContent), n)
	}

	// Underlying writer should have received the data.
	if !bytes.Equal(buf.Bytes(), testContent) {
		t.Fatalf("underlying writer got %q, want %q", buf.Bytes(), testContent)
	}

	// Checksum should match a direct computation.
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	h.Write(testContent)
	if got, want := cw.Sum32(), h.Sum32(); got != want {
		t.Errorf("checksum = %d, want %d", got, want)
	}
}

func Test_CRC32WriterMultipleWrites(t *testing.T) {
	parts := [][]byte{[]byte("hello "), []byte("world")}

	var buf bytes.Buffer
	cw := NewCRC32Writer(&buf)
	for _, p := range parts {
		if _, err := cw.Write(p); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}

	// Checksum should be the same as hashing the concatenated data.
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	for _, p := range parts {
		h.Write(p)
	}
	if got, want := cw.Sum32(), h.Sum32(); got != want {
		t.Errorf("checksum = %d, want %d", got, want)
	}
}

func Test_CRC32WriterEmpty(t *testing.T) {
	var buf bytes.Buffer
	cw := NewCRC32Writer(&buf)

	if got, want := cw.Sum32(), uint32(0); got != want {
		t.Errorf("empty checksum = %d, want %d", got, want)
	}
}

func Test_WriteCRC32SumFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checksum")
	var sum uint32 = 0x1a2b3c4d

	if err := WriteCRC32SumFile(path, sum); err != nil {
		t.Fatalf("WriteCRC32SumFile failed: %v", err)
	}

	// File should contain the hex string.
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading checksum file: %v", err)
	}
	if got, want := string(b), "1a2b3c4d"; got != want {
		t.Errorf("file contents = %q, want %q", got, want)
	}

	// Round-trip through ReadCRC32SumFile.
	got, err := ReadCRC32SumFile(path)
	if err != nil {
		t.Fatalf("ReadCRC32SumFile failed: %v", err)
	}
	if got != sum {
		t.Errorf("ReadCRC32SumFile = %08x, want %08x", got, sum)
	}
}

func Test_WriteCRC32SumFileZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checksum")

	if err := WriteCRC32SumFile(path, 0); err != nil {
		t.Fatalf("WriteCRC32SumFile failed: %v", err)
	}

	got, err := ReadCRC32SumFile(path)
	if err != nil {
		t.Fatalf("ReadCRC32SumFile failed: %v", err)
	}
	if got != 0 {
		t.Errorf("ReadCRC32SumFile = %08x, want 00000000", got)
	}
}

func Test_ReadCRC32SumFileNotFound(t *testing.T) {
	_, err := ReadCRC32SumFile(filepath.Join(t.TempDir(), "nonexistent"))
	if err == nil {
		t.Error("expected error for non-existent file, got none")
	}
}

func Test_ReadCRC32SumFileInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checksum")
	if err := os.WriteFile(path, []byte("not-a-checksum"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := ReadCRC32SumFile(path)
	if err == nil {
		t.Error("expected error for invalid checksum file, got none")
	}
}

// mustWriteTempFile writes the given bytes to a temporary file, and returns the
// path to the file. If there is an error, it panics. The file will be automatically
// deleted when the test ends.
func mustWriteTempFile(t *testing.T, b []byte) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "rqlite-test")
	if err != nil {
		panic("failed to create temp file")
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		panic("failed to write to temp file")
	}
	return f.Name()
}
