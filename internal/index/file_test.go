package index

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func Test_ReadOnNewFileFails(t *testing.T) {
	idx := newTempIndexFile(t)
	defer idx.Close()

	// File exists but is empty; ReadValue should error.
	if _, err := idx.ReadValue(); err != ErrEmptyFile {
		t.Fatalf("ReadValue on empty file, got wrong error: %v", err)
	}
}

func Test_WriteThenRead_RoundTrip(t *testing.T) {
	idx := newTempIndexFile(t)
	defer idx.Close()

	vals := []uint64{0, 1, 42, 1<<63 - 1, ^uint64(0)}
	for _, v := range vals {
		if err := idx.WriteValue(v); err != nil {
			t.Fatalf("WriteValue(%d): %v", v, err)
		}
		got, err := idx.ReadValue()
		if err != nil {
			t.Fatalf("ReadValue after WriteValue(%d): %v", v, err)
		}
		if got != v {
			t.Fatalf("round-trip mismatch: wrote %d, read %d", v, got)
		}
	}
}

func Test_Overwrite(t *testing.T) {
	idx := newTempIndexFile(t)
	defer idx.Close()

	if err := idx.WriteValue(111); err != nil {
		t.Fatalf("WriteValue: %v", err)
	}
	if err := idx.WriteValue(222); err != nil {
		t.Fatalf("WriteValue: %v", err)
	}
	got, err := idx.ReadValue()
	if err != nil {
		t.Fatalf("ReadValue: %v", err)
	}
	if got != 222 {
		t.Fatalf("overwrite failed: got %d, want %d", got, 222)
	}
	// Ensure file is at least 16 bytes; extra bytes (if any) should not affect read.
	fi, err := os.Stat(idx.Path())
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() < 16 {
		t.Fatalf("file too small: got %d, want >= 16", fi.Size())
	}
}

func Test_CorruptCRC(t *testing.T) {
	idx := newTempIndexFile(t)
	defer idx.Close()

	if err := idx.WriteValue(1234567890); err != nil {
		t.Fatalf("WriteValue: %v", err)
	}

	// Flip one byte in the value field to corrupt the CRC.
	var buf [1]byte
	// Read original byte at offset 7 (within the 8-byte value starting at 4).
	if _, err := idx.fd.ReadAt(buf[:], 4+7); err != nil {
		t.Fatalf("ReadAt to fetch original byte: %v", err)
	}
	buf[0] ^= 0x01
	if _, err := idx.fd.WriteAt(buf[:], 4+7); err != nil {
		t.Fatalf("WriteAt to corrupt: %v", err)
	}

	if _, err := idx.ReadValue(); err == nil {
		t.Fatalf("ReadValue after CRC corruption: want error, got nil")
	}
}

// Ensure IndexFile implements expected error semantics for ErrBadChecksum on corruption.
func Test_CorruptionReturnsError(t *testing.T) {
	idx := newTempIndexFile(t)
	defer idx.Close()

	if err := idx.WriteValue(5); err != nil {
		t.Fatalf("WriteValue: %v", err)
	}
	// Corrupt CRC field directly.
	var zero [4]byte
	if _, err := idx.fd.WriteAt(zero[:], 12); err != nil {
		t.Fatalf("WriteAt zero CRC: %v", err)
	}

	_, err := idx.ReadValue()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if err != ErrBadChecksum {
		t.Fatalf("expected ErrBadChecksum, got %T: %v", err, err)
	}
}

func Test_WrongMagic(t *testing.T) {
	idx := newTempIndexFile(t)
	defer idx.Close()

	if err := idx.WriteValue(7); err != nil {
		t.Fatalf("WriteValue: %v", err)
	}

	// Overwrite magic with an incorrect constant.
	var badMagic [4]byte
	binary.LittleEndian.PutUint32(badMagic[:], 0xDEADBEEF)
	if _, err := idx.fd.WriteAt(badMagic[:], 0); err != nil {
		t.Fatalf("WriteAt bad magic: %v", err)
	}

	if _, err := idx.ReadValue(); err == nil {
		t.Fatalf("ReadValue with wrong magic: want error, got nil")
	}
}

func Test_ShortFile(t *testing.T) {
	// Create a file that is too short to hold a full record.
	dir := t.TempDir()
	path := filepath.Join(dir, "applied.idx")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatalf("seed short file: %v", err)
	}
	idx := &IndexFile{fd: f}
	defer f.Close()

	if _, err := idx.ReadValue(); err == nil {
		t.Fatalf("ReadValue on short file: want error, got nil")
	}
}

func Test_ReadErrorPropagation(t *testing.T) {
	// Close the file descriptor and ensure ReadValue propagates an error.
	idx := newTempIndexFile(t)
	_ = idx.fd.Close()
	if _, err := idx.ReadValue(); err == nil {
		t.Fatalf("ReadValue on closed fd: want error, got nil")
	}
}

func Test_WriteErrorPropagation(t *testing.T) {
	// Close the file descriptor and ensure WriteValue propagates an error.
	idx := newTempIndexFile(t)
	_ = idx.fd.Close()
	if err := idx.WriteValue(1); err == nil {
		t.Fatalf("WriteValue on closed fd: want error, got nil")
	}
}

func Test_NonExistentPath_OpenFails(t *testing.T) {
	// Verify NewIndexFile fails if parent dir does not exist.
	dir := t.TempDir()
	path := filepath.Join(dir, "no_such_dir", "applied.idx")
	if _, err := NewIndexFile(path); err == nil {
		t.Fatalf("NewIndexFile on non-existent parent: want error, got nil")
	}
}

// helper to make a new IndexFile in a temp directory
func newTempIndexFile(t *testing.T) *IndexFile {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "applied.idx")
	idxf, err := NewIndexFile(path)
	if err != nil {
		t.Fatalf("NewIndexFile: %v", err)
	}
	return idxf
}
