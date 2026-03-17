package wal

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func Test_CompactingSectionScanner_AllFrames(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// The test WAL has 3 frames:
	//   Frame 1: pgno=1, commit=0
	//   Frame 2: pgno=2, commit=2
	//   Frame 3: pgno=2, commit=2
	// After compaction: pgno=1 (from frame 1), pgno=2 (from frame 3).
	s, err := NewCompactingSectionScanner(bytes.NewReader(b), WALHeaderSize, int64(len(b)), false)
	if err != nil {
		t.Fatal(err)
	}

	if s.Empty() {
		t.Fatal("expected non-empty scanner")
	}

	// Get all frames from FullScanner for data reference.
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

	// Compacted output: 2 frames.
	f1, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if f1.Pgno != 1 {
		t.Fatalf("expected pgno=1, got %d", f1.Pgno)
	}
	if !bytes.Equal(f1.Data, allFrames[0].Data) {
		t.Fatal("pgno=1 data mismatch")
	}

	f2, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if f2.Pgno != 2 {
		t.Fatalf("expected pgno=2, got %d", f2.Pgno)
	}
	// pgno=2 should have data from frame 3 (the latest version).
	if !bytes.Equal(f2.Data, allFrames[2].Data) {
		t.Fatal("pgno=2 data should match frame 3 (latest)")
	}

	_, err = s.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func Test_CompactingSectionScanner_PartialRange(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// The test WAL has 3 frames with page size 4096.
	// Frame size = WALFrameHeaderSize(24) + 4096 = 4120
	// Frame 1: offset 32,   pgno=1, commit=0
	// Frame 2: offset 4152, pgno=2, commit=2
	// Frame 3: offset 8272, pgno=2, commit=2
	// End:     offset 12392
	//
	// Frames 2-3 both have pgno=2. After compaction: 1 frame (pgno=2
	// from frame 3, the latest).
	const frameSize = WALFrameHeaderSize + 4096

	start := int64(WALHeaderSize + frameSize) // skip frame 1
	end := int64(len(b))

	s, err := NewCompactingSectionScanner(bytes.NewReader(b), start, end, false)
	if err != nil {
		t.Fatal(err)
	}

	if s.Empty() {
		t.Fatal("expected non-empty scanner")
	}

	// Get frame 3 data from FullScanner for reference.
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

	// Compacted output: 1 frame (pgno=2 from frame 3).
	f, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if f.Pgno != 2 {
		t.Fatalf("expected pgno=2, got %d", f.Pgno)
	}
	if !bytes.Equal(f.Data, allFrames[2].Data) {
		t.Fatal("pgno=2 data should match frame 3 (latest)")
	}

	_, err = s.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func Test_CompactingSectionScanner_NotAWAL(t *testing.T) {
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
			_, err := NewCompactingSectionScanner(bytes.NewReader(tc.data), WALHeaderSize, WALHeaderSize, false)
			if err == nil {
				t.Fatal("expected error for invalid WAL data")
			}
		})
	}
}

func Test_CompactingSectionScanner_BadOffsets(t *testing.T) {
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
			_, err := NewCompactingSectionScanner(r, tc.start, tc.end, false)
			if err == nil {
				t.Fatal("expected error for bad offsets")
			}
		})
	}
}

func Test_CompactingSectionScanner_FullScanRequiresHeaderStart(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	const frameSize = WALFrameHeaderSize + 4096
	start := int64(WALHeaderSize + frameSize)
	end := int64(len(b))

	// fullScan=true with start != WALHeaderSize must fail.
	_, err = NewCompactingSectionScanner(bytes.NewReader(b), start, end, true)
	if err == nil {
		t.Fatal("expected error for fullScan with non-header start offset")
	}

	// fullScan=true with start == WALHeaderSize must succeed.
	_, err = NewCompactingSectionScanner(bytes.NewReader(b), WALHeaderSize, end, true)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_CompactingSectionScanner_Empty(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingSectionScanner(bytes.NewReader(b), WALHeaderSize, WALHeaderSize, false)
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

func Test_CompactingSectionScanner_WriterRoundTrip(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// Write frames 2-3 through CompactingSectionScanner -> Writer -> buffer.
	const frameSize = WALFrameHeaderSize + 4096
	start := int64(WALHeaderSize + frameSize)
	end := int64(len(b))

	s, err := NewCompactingSectionScanner(bytes.NewReader(b), start, end, false)
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

	// Frames 2-3 are both pgno=2. After compaction: 1 frame.
	// Output should be a valid WAL: header + 1 frame.
	expectedSize := int64(WALHeaderSize + 1*frameSize)
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
	if count != 1 {
		t.Fatalf("expected 1 frame in output, got %d", count)
	}
}

func Test_CompactingSectionScanner_WriterRoundTrip_SQLite(t *testing.T) {
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

	// Write the entire WAL via CompactingSectionScanner+Writer to the dest WAL.
	walBytes, err := os.ReadFile(srcWAL)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingSectionScanner(bytes.NewReader(walBytes), WALHeaderSize, int64(len(walBytes)), false)
	if err != nil {
		t.Fatal(err)
	}

	destF, err := os.Create(destWAL)
	if err != nil {
		t.Fatal(err)
	}
	defer destF.Close()
	w, err := NewWriter(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteTo(destF); err != nil {
		t.Fatal(err)
	}

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

func Test_CompactingSectionScanner_Scan_FullScan(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingSectionScanner(bytes.NewReader(b), WALHeaderSize, int64(len(b)), true)
	if err != nil {
		t.Fatal(err)
	}

	for i, expF := range []struct {
		pgno        uint32
		commit      uint32
		dataLowIdx  int
		dataHighIdx int
	}{
		{1, 0, 56, 4152},
		//{2, 2, 4176, 8272}, skipped by the Compactor.
		{2, 2, 8296, 12392},
	} {
		f, err := s.Next()
		if err != nil {
			t.Fatal(err)
		}
		if f.Pgno != expF.pgno {
			t.Fatalf("expected pgno %d, got %d", expF.pgno, f.Pgno)
		}
		if f.Commit != expF.commit {
			t.Fatalf("expected commit %d, got %d", expF.commit, f.Commit)
		}
		if len(f.Data) != 4096 {
			t.Fatalf("expected data length 4096, got %d", len(f.Data))
		}
		if !bytes.Equal(f.Data, b[expF.dataLowIdx:expF.dataHighIdx]) {
			t.Fatalf("page data mismatch on test %d", i)
		}
	}

	_, err = s.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func Test_CompactingSectionScanner_Scan_Commit0(t *testing.T) {
	b, err := os.ReadFile("testdata/compacting-scanner/commit-0/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingSectionScanner(bytes.NewReader(b), WALHeaderSize, int64(len(b)), false)
	if err != nil {
		t.Fatal(err)
	}

	for _, expF := range []struct {
		pgno   uint32
		commit uint32
	}{
		// {1,0}, skipped by the Compactor.
		// {2,2}, skipped by the Compactor.
		{1, 0},
		{2, 0},
		{3, 0},
		{4, 0},
		{5, 0},
		// {6,6}, skipped by the Compactor.
		{6, 6},
	} {
		f, err := s.Next()
		if err != nil {
			t.Fatal(err)
		}
		if f.Pgno != expF.pgno {
			t.Fatalf("expected pgno %d, got %d", expF.pgno, f.Pgno)
		}
		if f.Commit != expF.commit {
			t.Fatalf("expected commit %d, got %d", expF.commit, f.Commit)
		}
	}

	_, err = s.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func Test_CompactingSectionScanner_Bytes(t *testing.T) {
	conn, path := mustCreateWAL(t, 128*1024)
	defer conn.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingSectionScanner(bytes.NewReader(b), WALHeaderSize, int64(len(b)), false)
	if err != nil {
		t.Fatal(err)
	}

	var ramWriter bytes.Buffer
	w, err := NewWriter(s)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.WriteTo(&ramWriter)
	if err != nil {
		t.Fatal(err)
	}

	buf, err := s.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, ramWriter.Bytes()) {
		t.Fatal("bytes mismatch")
	}
}

func Test_CompactingSectionScanner_Bytes_FullCycle(t *testing.T) {
	// First, make a copy of the test data.
	tmpDir := t.TempDir()
	if err := os.Remove(tmpDir); err != nil {
		t.Fatalf("failed to remove tmp dir: %s", err)
	}
	err := copyDir("testdata/compacting-scanner/full-cycle", tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Compact the WAL file.
	walPath := filepath.Join(tmpDir, "fc.db-wal")
	walFD, err := os.OpenFile(walPath, os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer walFD.Close()

	fi, err := walFD.Stat()
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewCompactingSectionScanner(walFD, WALHeaderSize, fi.Size(), false)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := s.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	// Remove the old WAL file.
	if err := walFD.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(walPath); err != nil {
		t.Fatal(err)
	}

	// Write the compacted WAL file back to disk, effectively replacing the old WAL file.
	if err := os.WriteFile(walPath, buf, 0666); err != nil {
		t.Fatal(err)
	}

	// Now, open the database and check the number of rows.
	dbPath := filepath.Join(tmpDir, "fc.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %s", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query db: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		var c int
		err = rows.Scan(&c)
		if err != nil {
			t.Fatalf("failed to scan row: %s", err)
		}
		if c != 1900 {
			t.Fatalf("expected 1900 rows, got %d", c)
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("failed to iterate rows: %s", err)
	}
}

func Test_CompactingSectionScanner_Writer_FullCycle(t *testing.T) {
	// First, make a copy of the test data.
	tmpDir := t.TempDir()
	if err := os.Remove(tmpDir); err != nil {
		t.Fatalf("failed to remove tmp dir: %s", err)
	}
	err := copyDir("testdata/compacting-scanner/full-cycle", tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Compact the WAL file.
	walPath := filepath.Join(tmpDir, "fc.db-wal")
	walFD, err := os.OpenFile(walPath, os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer walFD.Close()

	fi, err := walFD.Stat()
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewCompactingSectionScanner(walFD, WALHeaderSize, fi.Size(), false)
	if err != nil {
		t.Fatal(err)
	}

	// Create WAL writer.
	walWriter, err := NewWriter(s)
	if err != nil {
		t.Fatal(err)
	}

	tmpWALPath := filepath.Join(tmpDir, "fc.db-wal-tmp")
	tmpWALFD, err := os.Create(tmpWALPath)
	if err != nil {
		t.Fatal(err)
	}

	// Write the compacted WAL file back to disk, effectively replacing the old WAL file.
	func() {
		defer tmpWALFD.Close()
		_, err = walWriter.WriteTo(tmpWALFD)
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Remove the old WAL file.
	if err := walFD.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(walPath); err != nil {
		t.Fatal(err)
	}

	// Put the compacted WAL file in place.
	if err := os.Rename(tmpWALPath, walPath); err != nil {
		t.Fatal(err)
	}

	// Now, open the database and check the number of rows.
	dbPath := filepath.Join(tmpDir, "fc.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %s", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query db: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		var c int
		err = rows.Scan(&c)
		if err != nil {
			t.Fatalf("failed to scan row: %s", err)
		}
		if c != 1900 {
			t.Fatalf("expected 1900 rows, got %d", c)
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("failed to iterate rows: %s", err)
	}
}
