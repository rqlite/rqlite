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

func Test_CompactingFrameScanner_AllFrames(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// The test WAL has 3 frames:
	//   Frame 0: pgno=1, commit=0
	//   Frame 1: pgno=2, commit=2
	//   Frame 2: pgno=2, commit=2
	// After compaction: pgno=1 (from frame 0), pgno=2 (from frame 2).
	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 0, 3, false)
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

func Test_CompactingFrameScanner_PartialRange(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// The test WAL has 3 frames with page size 4096.
	// Frame 0: pgno=1, commit=0
	// Frame 1: pgno=2, commit=2
	// Frame 2: pgno=2, commit=2
	//
	// Frames 1-2 both have pgno=2. After compaction: 1 frame (pgno=2
	// from frame 2, the latest).
	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 1, 3, false)
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

func Test_CompactingFrameScanner_NotAWAL(t *testing.T) {
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
			_, err := NewCompactingFrameScanner(bytes.NewReader(tc.data), 0, 0, false)
			if err == nil {
				t.Fatal("expected error for invalid WAL data")
			}
		})
	}
}

func Test_CompactingFrameScanner_BadFrameIndices(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(b)

	tests := []struct {
		name       string
		startFrame int64
		endFrame   int64
	}{
		{"start after end", 2, 1},
		{"negative start", -1, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewCompactingFrameScanner(r, tc.startFrame, tc.endFrame, false)
			if err == nil {
				t.Fatal("expected error for bad frame indices")
			}
		})
	}
}

func Test_CompactingFrameScanner_FullScanRequiresFrameZero(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// fullScan=true with startFrame != 0 must fail.
	_, err = NewCompactingFrameScanner(bytes.NewReader(b), 1, 3, true)
	if err == nil {
		t.Fatal("expected error for fullScan with non-zero startFrame")
	}

	// fullScan=true with startFrame == 0 must succeed.
	_, err = NewCompactingFrameScanner(bytes.NewReader(b), 0, 3, true)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_CompactingFrameScanner_Empty(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 0, 0, false)
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

func Test_CompactingFrameScanner_OpenTransaction(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// The test WAL has 3 frames with page size 4096:
	//   Frame 0: pgno=1, commit=0
	//   Frame 1: pgno=2, commit=2
	//   Frame 2: pgno=2, commit=2
	// A range containing only frame 0 (commit=0) is an open transaction.
	_, err = NewCompactingFrameScanner(bytes.NewReader(b), 0, 1, false)
	if err != ErrOpenTransaction {
		t.Fatalf("expected ErrOpenTransaction, got %v", err)
	}
}

func Test_CompactingFrameScanner_Bytes_PartialSection(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// Scan frames 1-2 only (both pgno=2, compacts to 1 frame).
	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 1, 3, false)
	if err != nil {
		t.Fatal(err)
	}

	// Bytes() output should match Writer output for the same section.
	var writerBuf bytes.Buffer
	w, err := NewWriter(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteTo(&writerBuf); err != nil {
		t.Fatal(err)
	}

	bytesBuf, err := s.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytesBuf, writerBuf.Bytes()) {
		t.Fatal("Bytes() output does not match Writer output for partial section")
	}

	// Verify the output is a valid WAL by reading it back.
	fs, err := NewFullScanner(bytes.NewReader(bytesBuf))
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
			t.Fatalf("FullScanner failed on frame %d: %v", count, err)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("expected 1 frame in output, got %d", count)
	}
}

func Test_CompactingFrameScanner_WriterRoundTrip(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// Write frames 1-2 through CompactingFrameScanner -> Writer -> buffer.
	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 1, 3, false)
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

	// Frames 1-2 are both pgno=2. After compaction: 1 frame.
	// Output should be a valid WAL: header + 1 frame.
	const frameSize = WALFrameHeaderSize + 4096
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

func Test_CompactingFrameScanner_WriterRoundTrip_SQLite(t *testing.T) {
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
	for i := range 100 {
		mustExec(srcConn, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	// Copy the main database file, which hasn't been checkpointed into.
	destDir := t.TempDir()
	destDB := destDir + "/dest.db"
	mustCopyFile(destDB, srcDB)

	// Write the entire WAL via CompactingFrameScanner+Writer to the dest WAL.
	walBytes, err := os.ReadFile(srcWAL)
	if err != nil {
		t.Fatal(err)
	}

	nFrames := walFrameCount(walBytes)
	s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), 0, nFrames, false)
	if err != nil {
		t.Fatal(err)
	}

	// Now write the compacted WAL to disk, and have that checkpointed into the database copy. This
	// checks that the compacted WAL checkpointed into the same main database results in the same
	// data being present in the database.
	destWAL := destDir + "/dest.db-wal"
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

func Test_CompactingFrameScanner_Scan_FullScan(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 0, 3, true)
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

func Test_CompactingFrameScanner_Scan_Commit0(t *testing.T) {
	b, err := os.ReadFile("testdata/compacting-scanner/commit-0/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 0, walFrameCount(b), false)
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

func Test_CompactingFrameScanner_Bytes(t *testing.T) {
	conn, path := mustCreateWAL(t, 128*1024)
	defer conn.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 0, walFrameCount(b), false)
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

func Test_CompactingFrameScanner_Bytes_FullCycle(t *testing.T) {
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
	s, err := NewCompactingFrameScanner(walFD, 0, walFrameCountFromFile(walFD, fi.Size()), false)
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

func Test_CompactingFrameScanner_Writer_FullCycle(t *testing.T) {
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
	s, err := NewCompactingFrameScanner(walFD, 0, walFrameCountFromFile(walFD, fi.Size()), false)
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
