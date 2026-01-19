package proto

import (
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/rqlite/rqlite/v9/internal/rsum"
)

func Test_NewHeaderFromFile(t *testing.T) {
	content := []byte("Hello, World!")
	tmpFile := mustWriteToFile(t, content)

	// Test without CRC32
	header, err := NewHeaderFromFile(tmpFile, false)
	if err != nil {
		t.Fatalf("NewHeaderFromFile failed: %v", err)
	}
	if header.SizeBytes != uint64(len(content)) {
		t.Errorf("Expected SizeBytes %d, got %d", len(content), header.SizeBytes)
	}
	if header.Crc32 != 0 {
		t.Errorf("Expected Crc32 0, got %d", header.Crc32)
	}

	// Test with CRC32
	header, err = NewHeaderFromFile(tmpFile, true)
	if err != nil {
		t.Fatalf("NewHeaderFromFile failed: %v", err)
	}
	expectedCRC, err := rsum.CRC32(tmpFile)
	if err != nil {
		t.Fatalf("rsum.CRC32 failed: %v", err)
	}
	if header.Crc32 != expectedCRC {
		t.Errorf("Expected Crc32 %d, got %d", expectedCRC, header.Crc32)
	}
}

func Test_NewHeaderFromFile_Fail(t *testing.T) {
	_, err := NewHeaderFromFile("non-existent", false)
	if err == nil {
		t.Fatalf("Expected error for non-existent file, got nil")
	}
}

func Test_NewSnapshotHeader(t *testing.T) {
	if _, err := NewSnapshotHeader(""); err == nil {
		t.Fatalf("Expected error when both dbPath and walPaths are empty, got nil")
	}

	tmpDBFile := mustWriteToFile(t, []byte("DB Content"))
	tmpWALFile1 := mustWriteToFile(t, []byte("WAL Content 1"))
	tmpWALFile2 := mustWriteToFile(t, []byte("WAL Content 2"))

	if _, err := NewSnapshotHeader(tmpDBFile); err != nil {
		t.Fatalf("NewSnapshotHeader failed with single DB file: %v", err)
	}
	if _, err := NewSnapshotHeader(tmpDBFile, tmpWALFile1); err != nil {
		t.Fatalf("NewSnapshotHeader failed with DB and single WAL file: %v", err)
	}
	if _, err := NewSnapshotHeader(tmpDBFile, tmpWALFile1, tmpWALFile2); err != nil {
		t.Fatalf("NewSnapshotHeader failed with DB and multiple WAL files: %v", err)
	}
	if _, err := NewSnapshotHeader("", tmpWALFile1); err != nil {
		t.Fatalf("NewSnapshotHeader failed with single WAL file: %v", err)
	}

	if _, err := NewSnapshotHeader("", tmpWALFile1, tmpWALFile2); err == nil {
		t.Fatalf("Expected error when dbPath is empty and multiple WAL files are provided, got nil")
	}
}

func Test_SnapshotStreamer_EndToEndSize(t *testing.T) {
	tmpDBFile := mustWriteToFile(t, []byte("DB Content"))
	tmpWALFile := mustWriteToFile(t, []byte("WAL Content"))

	streamer, err := NewSnapshotStreamer(tmpDBFile, tmpWALFile)
	if err != nil {
		t.Fatalf("NewSnapshotStreamer failed: %v", err)
	}
	if err := streamer.Open(); err != nil {
		t.Fatalf("SnapshotStreamer Open failed: %v", err)
	}

	expectedReadLen, err := streamer.Len()
	if err != nil {
		t.Fatalf("Header TotalSize failed: %v", err)
	}
	totalRead := 0

	// Read the first four bytes for header size. Do this by creating
	// a buffer, reading into it, and decoding at a big endian uint32.
	buf := make([]byte, HeaderSizeLen)
	n, err := streamer.Read(buf)
	if err != nil || n != HeaderSizeLen {
		t.Fatalf("Failed to read header size: %v", err)
	}
	totalRead += n

	buf = make([]byte, binary.BigEndian.Uint32(buf))
	n, err = streamer.Read(buf)
	if err != nil || n != len(buf) {
		t.Fatalf("Failed to read header: %v", err)
	}
	totalRead += n
	header, err := UnmarshalSnapshotHeader(buf)
	if err != nil {
		t.Fatalf("Failed to unmarshal header: %v", err)
	}

	// Read the database data into a buf and compare contents
	dbBuf := make([]byte, header.DbHeader.SizeBytes)
	n, err = streamer.Read(dbBuf)
	if err != nil || uint64(n) != header.DbHeader.SizeBytes {
		t.Fatalf("Failed to read DB data: %v", err)
	}
	totalRead += n
	if string(dbBuf) != "DB Content" {
		t.Fatalf("DB data does not match expected content")
	}

	// Read the WAL data into a buf and compare contents
	walBuf := make([]byte, header.WalHeaders[0].SizeBytes)
	n, err = streamer.Read(walBuf)
	if err != nil || uint64(n) != header.WalHeaders[0].SizeBytes {
		t.Fatalf("Failed to read WAL data: %v", err)
	}
	totalRead += n
	if string(walBuf) != "WAL Content" {
		t.Fatalf("WAL data does not match expected content")
	}

	// Ensure we have reached EOF
	var eofBuf [1]byte
	if _, err := streamer.Read(eofBuf[:]); err != io.EOF {
		t.Fatalf("Expected EOF, got: %v", err)
	}

	if int64(totalRead) != expectedReadLen {
		t.Fatalf("Total read size %d does not match expected length %d", totalRead, expectedReadLen)
	}

	// Confirm Close works
	if err := streamer.Close(); err != nil {
		t.Fatalf("SnapshotStreamer Close failed: %v", err)
	}
}

func mustWriteToFile(t *testing.T, data []byte) string {
	tmpFile, err := os.CreateTemp(t.TempDir(), "testfile")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	if _, err := tmpFile.Write(data); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()
	return tmpFile.Name()
}
