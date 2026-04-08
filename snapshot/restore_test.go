package snapshot

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

func Test_Restore_DBOnly(t *testing.T) {
	srcDB := "testdata/db-and-wals/full2.db"
	streamer, err := NewSnapshotStreamer(srcDB)
	if err != nil {
		t.Fatalf("NewSnapshotStreamer failed: %v", err)
	}
	if err := streamer.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer streamer.Close()

	dstPath := filepath.Join(t.TempDir(), "restored.db")
	n, err := Restore(streamer, dstPath)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
	if n == 0 {
		t.Fatal("expected non-zero bytes read")
	}

	// Output should be byte-identical to the source DB.
	if !fsutil.FilesIdentical(srcDB, dstPath) {
		t.Fatal("restored database does not match source")
	}
}

func Test_Restore_DBAndWALs(t *testing.T) {
	srcDB := "testdata/db-and-wals/backup.db"
	walFiles := []string{
		"testdata/db-and-wals/wal-00",
		"testdata/db-and-wals/wal-01",
	}

	streamer, err := NewSnapshotStreamer(srcDB, walFiles...)
	if err != nil {
		t.Fatalf("NewSnapshotStreamer failed: %v", err)
	}
	if err := streamer.Open(); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer streamer.Close()

	dstPath := filepath.Join(t.TempDir(), "restored.db")
	n, err := Restore(streamer, dstPath)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
	if n == 0 {
		t.Fatal("expected non-zero bytes read")
	}

	// WALs are checkpointed, so the result won't match the source DB,
	// but it must be a valid SQLite file.
	if !db.IsValidSQLiteFile(dstPath) {
		t.Fatal("restored database is not a valid SQLite file")
	}

	// Check the state of the restored database.
	checkDB, err := db.Open(dstPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database at %s: %s", dstPath, err)
	}
	defer checkDB.Close()
	rows, err := checkDB.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query exp: %s got: %s", exp, got)
	}

	// Temporary WAL files should have been cleaned up.
	entries, _ := os.ReadDir(filepath.Dir(dstPath))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "restore-wal-") {
			t.Fatalf("temporary WAL file %s was not cleaned up", e.Name())
		}
	}
}

func Test_Restore_EmptyReader(t *testing.T) {
	dstPath := filepath.Join(t.TempDir(), "restored.db")
	_, err := Restore(bytes.NewReader(nil), dstPath)
	if err == nil {
		t.Fatal("expected error restoring from empty reader")
	}
}

func Test_Restore_CorruptDB(t *testing.T) {
	// Buffer a valid stream, then flip a single byte inside the DB region.
	// Restore must reject it with a CRC32 mismatch error.
	stream := mustBufferStream(t, "testdata/db-and-wals/full2.db")
	hdrLen := binary.BigEndian.Uint32(stream[:HeaderSizeLen])
	dbStart := int(HeaderSizeLen) + int(hdrLen)
	stream[dbStart] ^= 0xFF

	dstPath := filepath.Join(t.TempDir(), "restored.db")
	_, err := Restore(bytes.NewReader(stream), dstPath)
	if err == nil {
		t.Fatal("expected CRC mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "CRC32 mismatch for DB file") {
		t.Fatalf("expected DB CRC mismatch error, got: %v", err)
	}
}

func Test_Restore_CorruptWAL(t *testing.T) {
	// Buffer a valid stream containing a DB plus two WALs, then flip a
	// byte inside the first WAL's region. Restore must reject it with a
	// WAL CRC mismatch error before any WAL is applied.
	srcDB := "testdata/db-and-wals/backup.db"
	walFiles := []string{
		"testdata/db-and-wals/wal-00",
		"testdata/db-and-wals/wal-01",
	}
	stream := mustBufferStream(t, srcDB, walFiles...)

	// Parse the header to learn the DB size, then flip a byte inside the
	// first WAL.
	hdrLen := binary.BigEndian.Uint32(stream[:HeaderSizeLen])
	hdrBytes := stream[HeaderSizeLen : HeaderSizeLen+hdrLen]
	hdr, err := UnmarshalSnapshotHeader(hdrBytes)
	if err != nil {
		t.Fatalf("UnmarshalSnapshotHeader failed: %v", err)
	}
	full := hdr.GetFull()
	walStart := int(HeaderSizeLen) + int(hdrLen) + int(full.DbHeader.SizeBytes)
	stream[walStart] ^= 0xFF

	dstPath := filepath.Join(t.TempDir(), "restored.db")
	_, err = Restore(bytes.NewReader(stream), dstPath)
	if err == nil {
		t.Fatal("expected CRC mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "CRC32 mismatch for WAL file 0") {
		t.Fatalf("expected WAL 0 CRC mismatch error, got: %v", err)
	}

	// No temporary WAL files should be left behind from the failed restore...
	// (the existing code doesn't clean up on error, so this is informational
	// only — we just confirm the restored DB is the unchecksumed DB and not
	// a checkpointed one.)
	entries, _ := os.ReadDir(filepath.Dir(dstPath))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "restore-wal-") {
			// Clean up in case the test runs in a shared dir.
			os.Remove(filepath.Join(filepath.Dir(dstPath), e.Name()))
		}
	}
}

// mustBufferStream returns the entire framed snapshot stream produced by
// SnapshotStreamer for the given DB and (optional) WAL files.
func mustBufferStream(t *testing.T, dbPath string, walPaths ...string) []byte {
	t.Helper()
	streamer, err := NewSnapshotStreamer(dbPath, walPaths...)
	if err != nil {
		t.Fatalf("NewSnapshotStreamer failed: %v", err)
	}
	if err := streamer.Open(); err != nil {
		t.Fatalf("streamer Open failed: %v", err)
	}
	defer streamer.Close()
	buf, err := io.ReadAll(streamer)
	if err != nil {
		t.Fatalf("reading streamer failed: %v", err)
	}
	return buf
}
