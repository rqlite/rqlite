package index

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func Test_ReadOnNewFileFails(t *testing.T) {
	_, idx, cleanup := newTempIndexFile(t)
	defer cleanup()

	// File exists but is empty; ReadValue should error.
	if _, err := idx.ReadValue(); err == nil {
		t.Fatalf("ReadValue on empty file: want error, got nil")
	}
}

func Test_WriteThenRead_RoundTrip(t *testing.T) {
	_, idx, cleanup := newTempIndexFile(t)
	defer cleanup()

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
	path, idx, cleanup := newTempIndexFile(t)
	defer cleanup()

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
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() < 16 {
		t.Fatalf("file too small: got %d, want >= 16", fi.Size())
	}
}

func Test_CorruptCRC(t *testing.T) {
	_, idx, cleanup := newTempIndexFile(t)
	defer cleanup()

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

func Test_WrongMagic(t *testing.T) {
	_, idx, cleanup := newTempIndexFile(t)
	defer cleanup()

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

func Test_ManualRecordConstruction_MatchesImplementation(t *testing.T) {
	// Construct a valid record manually to ensure the layout is as expected.
	dir := t.TempDir()
	path := filepath.Join(dir, "applied.idx")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	const magic = 0x01415152
	const v uint64 = 0x0123456789ABCDEF

	var rec [16]byte
	binary.LittleEndian.PutUint32(rec[0:4], magic)
	binary.LittleEndian.PutUint64(rec[4:12], v)
	binary.LittleEndian.PutUint32(rec[12:16], crc32.ChecksumIEEE(rec[:12]))

	if _, err := f.WriteAt(rec[:], 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	idx := &IndexFile{fd: f}
	got, err := idx.ReadValue()
	if err != nil {
		t.Fatalf("ReadValue: %v", err)
	}
	if got != v {
		t.Fatalf("manual record read mismatch: got 0x%x, want 0x%x", got, v)
	}
}

func Test_ReadErrorPropagation(t *testing.T) {
	// Close the file descriptor and ensure ReadValue propagates an error.
	_, idx, _ := newTempIndexFile(t)
	_ = idx.fd.Close()
	if _, err := idx.ReadValue(); err == nil {
		t.Fatalf("ReadValue on closed fd: want error, got nil")
	}
}

func Test_WriteErrorPropagation(t *testing.T) {
	// Close the file descriptor and ensure WriteValue propagates an error.
	_, idx, _ := newTempIndexFile(t)
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

func Test_ConcurrentWriteRead_NoOffsetRaces(t *testing.T) {
	// Smoke test that positional I/O avoids offset interference.
	_, idx, cleanup := newTempIndexFile(t)
	defer cleanup()

	const iterations = 1000
	done := make(chan struct{})

	// Writer
	go func() {
		for i := 0; i < iterations; i++ {
			_ = idx.WriteValue(uint64(i))
		}
		close(done)
	}()

	// Reader loop: ensure any successfully read value is within range and nondecreasing
	// relative to a local snapshot. We accept read errors because the file could be
	// momentarily corrupted during a write without fsync guarantees.
	var last uint64
	for {
		select {
		case <-done:
			// One last read after writer completion should succeed.
			got, err := idx.ReadValue()
			if err == nil && got < last {
				t.Fatalf("final read decreased: got %d < last %d", got, last)
			}
			return
		default:
			got, err := idx.ReadValue()
			if err == nil {
				if got < last {
					t.Fatalf("read decreased: got %d < last %d", got, last)
				}
				last = got
			}
		}
	}
}

// Ensure IndexFile implements expected error semantics for io.ErrUnexpectedEOF on corruption.
func Test_CorruptionReturnsUnexpectedEOF(t *testing.T) {
	_, idx, cleanup := newTempIndexFile(t)
	defer cleanup()

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
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %T: %v", err, err)
	}
}

// helper to make a new IndexFile in a temp directory
func newTempIndexFile(t *testing.T) (path string, idx *IndexFile, cleanup func()) {
	t.Helper()
	dir := t.TempDir()
	path = filepath.Join(dir, "applied.idx")
	idxf, err := NewIndexFile(path)
	if err != nil {
		t.Fatalf("NewIndexFile: %v", err)
	}
	cleanup = func() { _ = idxf.fd.Close() }
	return path, idxf, cleanup
}
