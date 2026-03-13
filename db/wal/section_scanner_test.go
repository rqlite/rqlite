package wal

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func Test_SectionScanner_AllFrames(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	r := bytes.NewReader(b)
	s, err := NewSectionScanner(r, WALHeaderSize, int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}

	if s.Empty() {
		t.Fatal("expected non-empty scanner")
	}

	// Verify the scanner yields the same frames as FullScanner.
	fs, err := NewFullScanner(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; ; i++ {
		expF, expErr := fs.Next()
		gotF, gotErr := s.Next()
		if expErr == io.EOF && gotErr == io.EOF {
			break
		}
		if expErr != nil {
			t.Fatalf("full scanner error on frame %d: %v", i, expErr)
		}
		if gotErr != nil {
			t.Fatalf("section scanner error on frame %d: %v", i, gotErr)
		}
		if expF.Pgno != gotF.Pgno {
			t.Fatalf("frame %d: pgno mismatch: %d != %d", i, expF.Pgno, gotF.Pgno)
		}
		if expF.Commit != gotF.Commit {
			t.Fatalf("frame %d: commit mismatch: %d != %d", i, expF.Commit, gotF.Commit)
		}
		if !bytes.Equal(expF.Data, gotF.Data) {
			t.Fatalf("frame %d: data mismatch", i)
		}
	}
}

func Test_SectionScanner_PartialRange(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// The test WAL has 3 frames with page size 4096.
	// Frame size = WALFrameHeaderSize(24) + 4096 = 4120
	// Frame 1: offset 32
	// Frame 2: offset 4152
	// Frame 3: offset 8272
	// End:     offset 12392
	const frameSize = WALFrameHeaderSize + 4096

	// Read only frames 2-3.
	start := int64(WALHeaderSize + frameSize) // skip frame 1
	end := int64(len(b))

	s, err := NewSectionScanner(bytes.NewReader(b), start, end)
	if err != nil {
		t.Fatal(err)
	}

	if s.Empty() {
		t.Fatal("expected non-empty scanner")
	}

	// Get all frames from FullScanner for reference.
	fs, err := NewFullScanner(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	var allFrames []*Frame
	for {
		f, err := fs.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		allFrames = append(allFrames, f)
	}
	if len(allFrames) != 3 {
		t.Fatalf("expected 3 frames in test WAL, got %d", len(allFrames))
	}

	// SectionScanner should yield frames 2 and 3 (indices 1, 2).
	for i := 1; i < 3; i++ {
		f, err := s.Next()
		if err != nil {
			t.Fatalf("error reading frame %d: %v", i, err)
		}
		if f.Pgno != allFrames[i].Pgno {
			t.Fatalf("frame %d: pgno mismatch: %d != %d", i, f.Pgno, allFrames[i].Pgno)
		}
		if f.Commit != allFrames[i].Commit {
			t.Fatalf("frame %d: commit mismatch: %d != %d", i, f.Commit, allFrames[i].Commit)
		}
		if !bytes.Equal(f.Data, allFrames[i].Data) {
			t.Fatalf("frame %d: data mismatch", i)
		}
	}

	_, err = s.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func Test_SectionScanner_NotAWAL(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"too short", []byte("hello")},
		{"wrong magic", make([]byte, WALHeaderSize)},
		{"random garbage", bytes.Repeat([]byte{0xDE, 0xAD}, WALHeaderSize)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSectionScanner(bytes.NewReader(tc.data), WALHeaderSize, WALHeaderSize)
			if err == nil {
				t.Fatal("expected error for invalid WAL data")
			}
		})
	}
}

func Test_SectionScanner_BadOffsets(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(b)

	// Frame size for this WAL is 24 + 4096 = 4120.
	tests := []struct {
		name  string
		start int64
		end   int64
	}{
		{"start after end", 8272, 4152},
		{"start misaligned", WALHeaderSize + 1, int64(len(b))},
		{"end misaligned", WALHeaderSize, int64(len(b)) - 1},
		{"both misaligned", 100, 200},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSectionScanner(r, tc.start, tc.end)
			if err == nil {
				t.Fatal("expected error for bad offsets")
			}
		})
	}
}

func Test_SectionScanner_Empty(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewSectionScanner(bytes.NewReader(b), WALHeaderSize, WALHeaderSize)
	if err != nil {
		t.Fatal(err)
	}

	if !s.Empty() {
		t.Fatal("expected empty scanner")
	}

	_, err = s.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func Test_SectionScanner_WriterRoundTrip(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// Write frames 2-3 through SectionScanner -> Writer -> buffer.
	const frameSize = WALFrameHeaderSize + 4096
	start := int64(WALHeaderSize + frameSize)
	end := int64(len(b))

	s, err := NewSectionScanner(bytes.NewReader(b), start, end)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := NewWriter(s)
	if err != nil {
		t.Fatal(err)
	}
	n, err := w.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// Output should be a valid WAL: header + 2 frames.
	expectedSize := int64(WALHeaderSize + 2*frameSize)
	if n != expectedSize {
		t.Fatalf("expected %d bytes written, got %d", expectedSize, n)
	}

	// Read the output back with FullScanner to verify checksums are valid.
	fs, err := NewFullScanner(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for {
		_, err := fs.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("FullScanner failed to verify output frame %d: %v", count, err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 frames in output, got %d", count)
	}
}

func Test_SectionScanner_WriterRoundTrip_SQLite(t *testing.T) {
	// Create a real SQLite database with WAL data.
	srcDir := t.TempDir()
	srcDSN := fmt.Sprintf("file:%s", srcDir+"/src.db?_journal_mode=WAL&_synchronous=OFF")
	srcDB := srcDir + "/src.db"
	srcWAL := srcDir + "/src.db-wal"

	srcConn, err := sql.Open("sqlite3", srcDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer srcConn.Close()
	mustExec(srcConn, "PRAGMA wal_autocheckpoint=0")
	mustExec(srcConn, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	// Insert rows to generate WAL frames.
	for i := 0; i < 100; i++ {
		mustExec(srcConn, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	// Checkpoint to flush pages to the main database file, but keep the WAL.
	mustExec(srcConn, "PRAGMA wal_checkpoint(FULL)")

	// Copy the main database file (which now has the checkpointed data).
	destDir := t.TempDir()
	destDB := destDir + "/dest.db"
	destWAL := destDir + "/dest.db-wal"
	mustCopyFile(destDB, srcDB)

	// Write the entire WAL via SectionScanner+Writer to the dest WAL.
	walBytes, err := os.ReadFile(srcWAL)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewSectionScanner(bytes.NewReader(walBytes), WALHeaderSize, int64(len(walBytes)))
	if err != nil {
		t.Fatal(err)
	}

	destF, err := os.Create(destWAL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteTo(destF); err != nil {
		t.Fatal(err)
	}
	destF.Close()

	// Open the dest database and verify data is present.
	destDSN := fmt.Sprintf("file:%s", destDB)
	destConn, err := sql.Open("sqlite3", destDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer destConn.Close()

	var count int
	if err := destConn.QueryRow("SELECT COUNT(*) FROM foo").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("expected 100 rows, got %d", count)
	}
}
