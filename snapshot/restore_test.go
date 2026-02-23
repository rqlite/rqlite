package snapshot

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v10/db"
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
	if !filesIdentical(srcDB, dstPath) {
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
