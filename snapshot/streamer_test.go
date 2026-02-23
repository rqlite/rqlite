package snapshot

import (
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/rqlite/rqlite/v10/internal/rsum"
)

func Test_NewHeaderFromFile(t *testing.T) {
	content := []byte("Hello, World!")
	tmpFile := mustWriteToTempFile(t, content)

	t.Run("without CRC32", func(t *testing.T) {
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
	})

	t.Run("with CRC32", func(t *testing.T) {
		header, err := NewHeaderFromFile(tmpFile, true)
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
	})
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

	tmpDBFile := mustWriteToTempFile(t, []byte("DB Content"))
	tmpWALFile1 := mustWriteToTempFile(t, []byte("WAL Content 1"))
	tmpWALFile2 := mustWriteToTempFile(t, []byte("WAL Content 2"))

	// DB only → FullSnapshot
	hdr, err := NewSnapshotHeader(tmpDBFile)
	if err != nil {
		t.Fatalf("NewSnapshotHeader failed with single DB file: %v", err)
	}
	if hdr.GetFull() == nil {
		t.Fatalf("Expected Full payload for DB-only header")
	}

	// DB + single WAL → FullSnapshot
	hdr, err = NewSnapshotHeader(tmpDBFile, tmpWALFile1)
	if err != nil {
		t.Fatalf("NewSnapshotHeader failed with DB and single WAL file: %v", err)
	}
	if hdr.GetFull() == nil {
		t.Fatalf("Expected Full payload for DB+WAL header")
	}

	// DB + multiple WALs → FullSnapshot
	hdr, err = NewSnapshotHeader(tmpDBFile, tmpWALFile1, tmpWALFile2)
	if err != nil {
		t.Fatalf("NewSnapshotHeader failed with DB and multiple WAL files: %v", err)
	}
	if full := hdr.GetFull(); full == nil {
		t.Fatalf("Expected Full payload for DB+WALs header")
	} else if len(full.WalHeaders) != 2 {
		t.Fatalf("Expected 2 WAL headers, got %d", len(full.WalHeaders))
	}

	// Single WAL → IncrementalSnapshot
	hdr, err = NewSnapshotHeader("", tmpWALFile1)
	if err != nil {
		t.Fatalf("NewSnapshotHeader failed with single WAL file: %v", err)
	}
	if hdr.GetIncremental() == nil {
		t.Fatalf("Expected Incremental payload for WAL-only header")
	}

	// Multiple WALs without DB → error
	if _, err := NewSnapshotHeader("", tmpWALFile1, tmpWALFile2); err == nil {
		t.Fatalf("Expected error when dbPath is empty and multiple WAL files are provided, got nil")
	}
}

func Test_SnapshotStreamer_EndToEndSize(t *testing.T) {
	cases := []struct {
		name     string
		dbData   string
		walDatas []string
	}{
		{
			name:   "DBOnly",
			dbData: "DB Content",
		},
		{
			name:     "WALOnly",
			walDatas: []string{"WAL Content"},
		},
		{
			name:     "DBWAL",
			dbData:   "DB Content",
			walDatas: []string{"WAL Content"},
		},
		{
			name:     "DBWALs",
			dbData:   "DB Content",
			walDatas: []string{"WAL Content", "More WAL Content"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runSnapshotStreamerEndToEnd(t, tc.dbData, tc.walDatas)
		})
	}
}

func runSnapshotStreamerEndToEnd(t *testing.T, dbData string, walDatas []string) {
	t.Helper()

	var dbPath string
	if dbData != "" {
		dbPath = mustWriteToTempFile(t, []byte(dbData))
	}

	walPaths := make([]string, 0, len(walDatas))
	for _, wd := range walDatas {
		walPaths = append(walPaths, mustWriteToTempFile(t, []byte(wd)))
	}

	streamer, err := NewSnapshotStreamer(dbPath, walPaths...)
	if err != nil {
		t.Fatalf("NewSnapshotStreamer failed: %v", err)
	}
	if err := streamer.Open(); err != nil {
		t.Fatalf("SnapshotStreamer Open failed: %v", err)
	}
	defer func() {
		if err := streamer.Close(); err != nil {
			t.Fatalf("SnapshotStreamer Close failed: %v", err)
		}
	}()

	expectedReadLen, err := streamer.Len()
	if err != nil {
		t.Fatalf("SnapshotStreamer Len failed: %v", err)
	}

	totalRead := 0

	// Read and decode header size.
	sizeBuf := make([]byte, HeaderSizeLen)
	n, err := io.ReadFull(streamer, sizeBuf)
	if err != nil {
		t.Fatalf("Failed to read header size: %v", err)
	}
	totalRead += n

	hdrLen := int(binary.BigEndian.Uint32(sizeBuf))
	hdrBuf := make([]byte, hdrLen)
	n, err = io.ReadFull(streamer, hdrBuf)
	if err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}
	totalRead += n

	header, err := UnmarshalSnapshotHeader(hdrBuf)
	if err != nil {
		t.Fatalf("Failed to unmarshal header: %v", err)
	}

	// Read payloads based on header type.
	if full := header.GetFull(); full != nil {
		if dbData == "" {
			t.Fatalf("Got Full payload but expected no DB data")
		}

		dbBuf := make([]byte, full.DbHeader.SizeBytes)
		n, err = io.ReadFull(streamer, dbBuf)
		if err != nil {
			t.Fatalf("Failed to read DB data: %v", err)
		}
		totalRead += n

		if string(dbBuf) != dbData {
			t.Fatalf("DB data does not match expected content")
		}

		if len(full.WalHeaders) != len(walDatas) {
			t.Fatalf("Expected %d WAL headers, got %d", len(walDatas), len(full.WalHeaders))
		}
		for i, expected := range walDatas {
			walBuf := make([]byte, full.WalHeaders[i].SizeBytes)
			n, err = io.ReadFull(streamer, walBuf)
			if err != nil {
				t.Fatalf("Failed to read WAL data %d: %v", i, err)
			}
			totalRead += n

			if string(walBuf) != expected {
				t.Fatalf("WAL data %d does not match expected content", i)
			}
		}
	} else if inc := header.GetIncremental(); inc != nil {
		if dbData != "" {
			t.Fatalf("Got Incremental payload but expected DB data")
		}
		if len(walDatas) != 1 {
			t.Fatalf("Expected exactly 1 WAL for Incremental, got %d", len(walDatas))
		}

		walBuf := make([]byte, inc.WalHeader.SizeBytes)
		n, err = io.ReadFull(streamer, walBuf)
		if err != nil {
			t.Fatalf("Failed to read WAL data: %v", err)
		}
		totalRead += n

		if string(walBuf) != walDatas[0] {
			t.Fatalf("WAL data does not match expected content")
		}
	} else {
		t.Fatalf("Unexpected payload type in header")
	}

	// Ensure EOF.
	var eofBuf [1]byte
	if _, err := streamer.Read(eofBuf[:]); err != io.EOF {
		t.Fatalf("Expected EOF, got: %v", err)
	}

	if int64(totalRead) != expectedReadLen {
		t.Fatalf("Total read size %d does not match expected length %d", totalRead, expectedReadLen)
	}
}

func Test_SnapshotPathStreamer(t *testing.T) {
	walDir := t.TempDir()
	streamer, err := NewSnapshotPathStreamer(walDir)
	if err != nil {
		t.Fatalf("NewSnapshotPathStreamer failed: %v", err)
	}

	// Read and decode header size.
	sizeBuf := make([]byte, HeaderSizeLen)
	_, err = io.ReadFull(streamer, sizeBuf)
	if err != nil {
		t.Fatalf("Failed to read header size: %v", err)
	}

	hdrLen := int(binary.BigEndian.Uint32(sizeBuf))
	hdrBuf := make([]byte, hdrLen)
	_, err = io.ReadFull(streamer, hdrBuf)
	if err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}

	pb, err := UnmarshalSnapshotHeader(hdrBuf)
	if err != nil {
		t.Fatalf("Failed to unmarshal header from SnapshotPathStreamer: %v", err)
	}

	inc := pb.GetIncrementalFile()
	if inc == nil {
		t.Fatalf("Expected IncrementalFile payload, got nil")
	}

	if inc.WalDirPath != walDir {
		t.Fatalf("Expected WalDirPath %q, got %q", walDir, inc.WalDirPath)
	}

	// Check that there is no more data to read after the header.
	var eofBuf [1]byte
	if _, err := streamer.Read(eofBuf[:]); err != io.EOF {
		t.Fatalf("Expected EOF after reading header, got: %v", err)
	}

	if err := streamer.Close(); err != nil {
		t.Fatalf("Failed to close SnapshotPathStreamer: %v", err)
	}
}

func mustWriteToTempFile(t *testing.T, data []byte) string {
	t.Helper()

	tmpFile, err := os.CreateTemp(t.TempDir(), "testfile")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	if _, err := tmpFile.Write(data); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}
	return tmpFile.Name()
}
