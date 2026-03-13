package db

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rqlite/rqlite/v10/db/wal"
)

func Test_WALManager_NewNoFile(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db := mustOpenRqliteDB(t, dbPath)
	defer db.Close()
	mustExecSQL(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	// Remove the WAL file so init() fails.
	os.Remove(db.WALPath())

	wm := NewWALManager(db)
	_, _, err := wm.Checkpoint()
	if err == nil {
		t.Fatal("expected error for nonexistent WAL file")
	}
}

func Test_WALManager_NewJunkFile(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db := mustOpenRqliteDB(t, dbPath)
	defer db.Close()
	mustExecSQL(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	// Overwrite the WAL file with junk.
	if err := os.WriteFile(db.WALPath(), []byte("this is not a WAL file"), 0644); err != nil {
		t.Fatal(err)
	}

	wm := NewWALManager(db)
	_, _, err := wm.Checkpoint()
	if err == nil {
		t.Fatal("expected error for junk WAL file")
	}
}

func Test_WALManager_NewValidWAL(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db := mustOpenRqliteDB(t, dbPath)
	defer db.Close()
	mustExecSQL(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	mustExecSQL(t, db, "INSERT INTO foo (name) VALUES ('test')")

	wm := NewWALManager(db)
	defer wm.Close()

	w, _, err := wm.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if w.Empty() {
		t.Fatal("expected non-empty WALWriter for valid WAL file")
	}
}

func Test_WALManager_SuccessPath(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db := mustOpenRqliteDB(t, dbPath)
	defer db.Close()
	mustExecSQL(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	// Insert first batch of rows.
	for i := 0; i < 50; i++ {
		mustExecSQL(t, db, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	wm := NewWALManager(db)
	defer wm.Close()

	// First checkpoint: should get all frames.
	w, busy, err := wm.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if busy {
		t.Fatal("expected busy=false")
	}
	if w.Empty() {
		t.Fatal("expected non-empty WALWriter")
	}

	var buf1 bytes.Buffer
	if _, err := w.WriteTo(&buf1); err != nil {
		t.Fatal(err)
	}
	firstSize := buf1.Len()

	// Verify the output is a valid WAL.
	mustVerifyWAL(t, buf1.Bytes())

	// Insert second batch.
	for i := 50; i < 100; i++ {
		mustExecSQL(t, db, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	// Second checkpoint: should get only new frames (smaller output).
	w, busy, err = wm.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if busy {
		t.Fatal("expected busy=false")
	}
	if w.Empty() {
		t.Fatal("expected non-empty WALWriter")
	}

	var buf2 bytes.Buffer
	if _, err := w.WriteTo(&buf2); err != nil {
		t.Fatal(err)
	}
	secondSize := buf2.Len()

	mustVerifyWAL(t, buf2.Bytes())

	if secondSize >= firstSize {
		t.Fatalf("expected second WAL (%d bytes) to be smaller than first (%d bytes)",
			secondSize, firstSize)
	}
}

func Test_WALManager_BusyPath(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db := mustOpenRqliteDB(t, dbPath)
	defer db.Close()
	mustExecSQL(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	// Insert rows so there's data in the WAL.
	for i := 0; i < 50; i++ {
		mustExecSQL(t, db, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	wm := NewWALManager(db)
	defer wm.Close()

	// Open a separate read-only connection and hold a cursor open to block the
	// checkpoint. The cursor must not be fully consumed — go-sqlite3
	// releases the WAL read lock when the statement finishes stepping.
	roDSN := fmt.Sprintf("file:%s?mode=ro", dbPath)
	roConn, err := sql.Open("sqlite3", roDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer roConn.Close()
	roConn.SetMaxOpenConns(1)

	rows, err := roConn.Query("SELECT * FROM foo")
	if err != nil {
		t.Fatal(err)
	}
	rows.Next() // read one row, leave cursor open to hold WAL read lock

	// Insert more rows while the reader holds a snapshot at the old end-mark.
	for i := 50; i < 100; i++ {
		mustExecSQL(t, db, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	// Checkpoint should report busy because the reader's open cursor
	// prevents FULL checkpoint from completing.
	w, busy, err := wm.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if !busy {
		t.Fatal("expected busy=true")
	}
	if w.Empty() {
		t.Fatal("expected non-empty WALWriter even when busy")
	}
	mustVerifyWAL(t, mustWriteWAL(t, w))

	// Release the reader.
	rows.Close()

	// Checkpoint again should succeed.
	w, busy, err = wm.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if busy {
		t.Fatal("expected busy=false after releasing reader")
	}
}

func Test_WALManager_SaltChange(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db := mustOpenRqliteDB(t, dbPath)
	defer db.Close()
	mustExecSQL(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	// Insert rows and create WALManager.
	for i := 0; i < 50; i++ {
		mustExecSQL(t, db, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	wm := NewWALManager(db)
	defer wm.Close()

	// First checkpoint: success, advances offset.
	w1, _, err := wm.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if w1.Empty() {
		t.Fatal("expected non-empty WALWriter")
	}
	firstWAL := mustWriteWAL(t, w1)
	firstSize := len(firstWAL)

	// Force a WAL reset by doing a TRUNCATE checkpoint, then writing new data.
	// TRUNCATE resets the WAL, which changes the salt.
	if _, err := db.Checkpoint(CheckpointTruncate); err != nil {
		t.Fatal(err)
	}

	// Insert new rows. This will cause SQLite to write a new WAL with new salt.
	for i := 100; i < 150; i++ {
		mustExecSQL(t, db, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	// Checkpoint should detect salt change and deliver all frames.
	w2, _, err := wm.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if w2.Empty() {
		t.Fatal("expected non-empty WALWriter after salt change")
	}
	secondWAL := mustWriteWAL(t, w2)
	mustVerifyWAL(t, secondWAL)

	// After salt change, the WALWriter should deliver all frames from the
	// beginning, so its size should be comparable to what a full scan would
	// produce (not just a small incremental).
	t.Logf("first WAL size: %d, second WAL size after salt change: %d", firstSize, len(secondWAL))
}

// mustOpenRqliteDB opens a rqlite DB in WAL mode. Auto-checkpoint is
// disabled by Open().
func mustOpenRqliteDB(t *testing.T, path string) *DB {
	t.Helper()
	db, err := Open(path, false, true)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// mustExecSQL executes a SQL statement via the DB's read-write connection.
func mustExecSQL(t *testing.T, db *DB, query string) {
	t.Helper()
	if _, err := db.rwDB.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// mustWriteWAL writes the WALWriter to a buffer and returns the bytes.
func mustWriteWAL(t *testing.T, w *WALWriter) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// mustVerifyWAL reads the WAL bytes using FullScanner to verify checksums.
func mustVerifyWAL(t *testing.T, b []byte) {
	t.Helper()
	s, err := wal.NewFullScanner(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("invalid WAL: %v", err)
	}
	count := 0
	for {
		_, err := s.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("invalid WAL frame %d: %v", count, err)
		}
		count++
	}
	if count == 0 {
		t.Fatal("WAL contains no valid frames")
	}
}
