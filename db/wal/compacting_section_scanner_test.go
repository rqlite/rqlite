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
	"github.com/rqlite/rqlite/v10/internal/fsutil"
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
	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 0, false)
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
		// Copy Data: the scanner reuses its page buffer across Next calls.
		allFrames = append(allFrames, &Frame{
			Pgno:   f.Pgno,
			Commit: f.Commit,
			Data:   append([]byte(nil), f.Data...),
		})
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
	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 1, false)
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
		// Copy Data: the scanner reuses its page buffer across Next calls.
		allFrames = append(allFrames, &Frame{
			Pgno:   f.Pgno,
			Commit: f.Commit,
			Data:   append([]byte(nil), f.Data...),
		})
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
			_, err := NewCompactingFrameScanner(bytes.NewReader(tc.data), 0, false)
			if err == nil {
				t.Fatal("expected error for invalid WAL data")
			}
		})
	}
}

func Test_CompactingFrameScanner_NegativeStartFrame(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	_, err = NewCompactingFrameScanner(bytes.NewReader(b), -1, false)
	if err == nil {
		t.Fatal("expected error for negative startFrame")
	}
}

func Test_CompactingFrameScanner_FullScanRequiresFrameZero(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// fullScan=true with startFrame != 0 must fail.
	_, err = NewCompactingFrameScanner(bytes.NewReader(b), 1, true)
	if err == nil {
		t.Fatal("expected error for fullScan with non-zero startFrame")
	}

	// fullScan=true with startFrame == 0 must succeed.
	_, err = NewCompactingFrameScanner(bytes.NewReader(b), 0, true)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_CompactingFrameScanner_Empty(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// Start past all frames in the WAL (which has 3 frames).
	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 100, false)
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

// Test_CompactingFrameScanner_TrailingUncommittedFrames tests that frames after
// the last commit frame, as left by a rolled-back or interrupted transaction,
// are dropped whether or not checksums are verified.
func Test_CompactingFrameScanner_TrailingUncommittedFrames(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// The test WAL has 3 frames with page size 4096:
	//   Frame 0: pgno=1, commit=0
	//   Frame 1: pgno=2, commit=2
	//   Frame 2: pgno=2, commit=2
	// Truncate to just the header + frame 0, so the WAL ends with a frame
	// that no commit frame follows.
	const frameSize = WALFrameHeaderSize + 4096
	truncated := b[:WALHeaderSize+frameSize]

	for _, fullScan := range []bool{true, false} {
		s, err := NewCompactingFrameScanner(bytes.NewReader(truncated), 0, fullScan)
		if err != nil {
			t.Fatalf("fullScan=%v: expected trailing uncommitted frame to be dropped, got %v", fullScan, err)
		}
		if !s.Empty() {
			t.Fatalf("fullScan=%v: expected empty scanner", fullScan)
		}
	}
}

// Test_CompactingFrameScanner_TrailingUncommittedFrames_SQLite tests a WAL,
// written by SQLite, in which committed transactions are followed by many
// frames of a transaction which is still open. Scanning it must give the same
// compacted WAL as scanning a copy cut off after the last commit frame, when
// starting at frame 0, part-way through, and at the first uncommitted frame.
func Test_CompactingFrameScanner_TrailingUncommittedFrames_SQLite(t *testing.T) {
	dir := t.TempDir()
	walPath := dir + "/test.db-wal"
	conn, err := sql.Open("sqlite3", fmt.Sprintf("file:%s", dir+"/test.db?_journal_mode=WAL&_synchronous=OFF"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetMaxOpenConns(1) // The transaction must stay on one connection.
	mustExec(conn, "PRAGMA wal_autocheckpoint=0")
	mustExec(conn, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	for i := range 10 {
		mustExec(conn, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}
	committedSz := mustFileSize(walPath)

	// Open a transaction and insert enough rows that SQLite writes its frames
	// to the WAL before it commits. The transaction is left open.
	mustExec(conn, "PRAGMA cache_size=5")
	mustExec(conn, "BEGIN")
	for i := range 1000 {
		mustExec(conn, fmt.Sprintf("INSERT INTO foo (name) VALUES ('%0500d')", i))
	}
	b, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	committed := b[:committedSz]

	const frameSize = WALFrameHeaderSize + 4096
	nCommitted := (committedSz - WALHeaderSize) / frameSize
	if nCommitted < 2 {
		t.Fatalf("expected at least 2 committed frames, got %d", nCommitted)
	}
	if nUncommitted := (int64(len(b)) - committedSz) / frameSize; nUncommitted < 2 {
		t.Fatalf("expected at least 2 uncommitted frames, got %d", nUncommitted)
	}

	mustScan := func(data []byte, start int64, fullScan bool) *CompactingFrameScanner {
		t.Helper()
		s, err := NewCompactingFrameScanner(bytes.NewReader(data), start, fullScan)
		if err != nil {
			t.Fatalf("start=%d, fullScan=%v: failed to create scanner: %s", start, fullScan, err)
		}
		return s
	}
	mustBytes := func(s *CompactingFrameScanner) []byte {
		t.Helper()
		data, err := s.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	for _, tt := range []struct {
		start    int64
		fullScan bool
	}{
		{0, true},
		{0, false},
		{nCommitted / 2, false},
	} {
		s := mustScan(b, tt.start, tt.fullScan)
		if s.Empty() {
			t.Fatalf("start=%d, fullScan=%v: expected non-empty scanner", tt.start, tt.fullScan)
		}
		if !bytes.Equal(mustBytes(s), mustBytes(mustScan(committed, tt.start, tt.fullScan))) {
			t.Fatalf("start=%d, fullScan=%v: compacted WAL carries frames after the last commit frame",
				tt.start, tt.fullScan)
		}
	}

	// Starting at the first uncommitted frame there is nothing to carry.
	if !mustScan(b, nCommitted, false).Empty() {
		t.Fatal("expected empty scanner when starting at the first uncommitted frame")
	}
}

func Test_CompactingFrameScanner_Bytes_PartialSection(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	// Scan frames 1-2 only (both pgno=2, compacts to 1 frame).
	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 1, false)
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
	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 1, false)
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
	mustExec(srcConn, "CREATE TABLE bar (id INTEGER PRIMARY KEY, name TEXT)")

	// Insert rows to generate WAL frames.
	testCount := 1000
	for i := range testCount {
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
	t.Logf("source WAL is %d bytes in size", mustFileSize(srcWAL))

	s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), 0, false)
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
	t.Logf("compacted WAL is %d bytes in size", mustFileSize(destWAL))

	// Open the dest database and verify data is present.
	destDSN := fmt.Sprintf("file:%s", destDB)
	destConn, err := sql.Open("sqlite3", destDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer destConn.Close()

	// Check that inserted record counts are correct for both tables.
	var count int
	if err := destConn.QueryRow("SELECT COUNT(*) FROM foo").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != testCount {
		t.Fatalf("expected %d rows, got %d", testCount, count)
	}
	if err := destConn.QueryRow("SELECT COUNT(*) FROM bar").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows, got %d", count)
	}
}

func Test_CompactingFrameScanner_Scan_FullScan(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 0, true)
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

	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 0, false)
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

	s, err := NewCompactingFrameScanner(bytes.NewReader(b), 0, false)
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
	if err := fsutil.Remove(tmpDir); err != nil {
		t.Fatalf("failed to remove tmp dir: %s", err)
	}
	err := fsutil.CopyDir("testdata/compacting-scanner/full-cycle", tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer fsutil.RemoveAll(tmpDir)

	// Compact the WAL file.
	walPath := filepath.Join(tmpDir, "fc.db-wal")
	walFD, err := os.OpenFile(walPath, os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer walFD.Close()

	s, err := NewCompactingFrameScanner(walFD, 0, false)
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
	if err := fsutil.Remove(walPath); err != nil {
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
	if err := fsutil.Remove(tmpDir); err != nil {
		t.Fatalf("failed to remove tmp dir: %s", err)
	}
	err := fsutil.CopyDir("testdata/compacting-scanner/full-cycle", tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer fsutil.RemoveAll(tmpDir)

	// Compact the WAL file.
	walPath := filepath.Join(tmpDir, "fc.db-wal")
	walFD, err := os.OpenFile(walPath, os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer walFD.Close()

	s, err := NewCompactingFrameScanner(walFD, 0, false)
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
	if err := fsutil.Remove(walPath); err != nil {
		t.Fatal(err)
	}

	// Put the compacted WAL file in place.
	if err := fsutil.Rename(tmpWALPath, walPath); err != nil {
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

func mustFileSize(path string) int64 {
	stat, err := os.Stat(path)
	if err != nil {
		panic("failed to stat file")
	}
	return stat.Size()
}
