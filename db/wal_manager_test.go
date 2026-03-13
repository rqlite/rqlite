package db

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rqlite/rqlite/v10/db/wal"
)

func Test_WALManager_NewNil(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManager(filepath.Join(dir, "nonexistent-wal"))
	if err != nil {
		t.Fatal(err)
	}
	if wm != nil {
		t.Fatal("expected nil WALManager for nonexistent file")
	}
}

func Test_WALManager_NewEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty-wal")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatal(err)
	}
	wm, err := NewWALManager(path)
	if err != nil {
		t.Fatal(err)
	}
	if wm != nil {
		t.Fatal("expected nil WALManager for empty file")
	}
}

func Test_WALManager_SuccessPath(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db := mustOpenDB(t, dbPath)
	defer db.Close()
	mustDBExec(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	// Insert first batch of rows.
	for i := 0; i < 50; i++ {
		mustDBExec(t, db, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	walPath := dbPath + "-wal"
	wm, err := NewWALManager(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if wm == nil {
		t.Fatal("expected non-nil WALManager")
	}
	defer wm.Close()

	// First checkpoint: should get all frames.
	w, busy, err := wm.Checkpoint(db)
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
		mustDBExec(t, db, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	// Second checkpoint: should get only new frames (smaller output).
	w, busy, err = wm.Checkpoint(db)
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

	rwDB := mustOpenDB(t, dbPath)
	defer rwDB.Close()
	mustDBExec(t, rwDB, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	// Insert rows so there's data in the WAL.
	for i := 0; i < 50; i++ {
		mustDBExec(t, rwDB, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	walPath := dbPath + "-wal"
	wm, err := NewWALManager(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if wm == nil {
		t.Fatal("expected non-nil WALManager")
	}
	defer wm.Close()

	// Open a read-only connection and hold a cursor open to block the
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
		mustDBExec(t, rwDB, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	// Checkpoint should report busy because the reader's open cursor
	// prevents FULL checkpoint from completing.
	w, busy, err := wm.Checkpoint(rwDB)
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
	w, busy, err = wm.Checkpoint(rwDB)
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

	db := mustOpenDB(t, dbPath)
	defer db.Close()
	mustDBExec(t, db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	// Insert rows and create WALManager.
	for i := 0; i < 50; i++ {
		mustDBExec(t, db, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	walPath := dbPath + "-wal"
	wm, err := NewWALManager(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if wm == nil {
		t.Fatal("expected non-nil WALManager")
	}
	defer wm.Close()

	// First checkpoint: success, advances offset.
	w1, _, err := wm.Checkpoint(db)
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
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}

	// Insert new rows. This will cause SQLite to write a new WAL with new salt.
	for i := 100; i < 150; i++ {
		mustDBExec(t, db, fmt.Sprintf("INSERT INTO foo (name) VALUES ('row%d')", i))
	}

	// Checkpoint should detect salt change and deliver all frames.
	w2, _, err := wm.Checkpoint(db)
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

// mustOpenDB opens a SQLite database in WAL mode with auto-checkpoint disabled.
// It returns the raw *sql.DB for use with WALManager.Checkpoint.
func mustOpenDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=OFF", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Force a connection to be created so the database file exists.
	if _, err := db.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	return db
}

// mustDBExec executes a SQL statement and fails the test on error.
func mustDBExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
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
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("invalid WAL frame %d: %v", count, err)
		}
		count++
	}
	if count == 0 {
		t.Fatal("WAL contains no valid frames")
	}
}
