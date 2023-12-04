package wal

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"testing"

	_ "github.com/rqlite/go-sqlite3"
	"github.com/rqlite/rqlite/random"
)

func Test_Writer_FullScanner(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewFullScanner(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	// Simply reading every frame and writing it back to a buffer should
	// result in the same bytes as the original WAL file.
	var buf bytes.Buffer
	w, err := NewWriter(s)
	if err != nil {
		t.Fatal(err)
	}
	n, err := w.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(b)) {
		t.Fatalf("expected to write %d bytes, wrote %d", len(b), n)
	}

	if !bytes.Equal(b, buf.Bytes()) {
		t.Fatal("writer did not write the same bytes as the reader")
	}
}

func Test_Writer_FullScanner_LargeWAL(t *testing.T) {
	conn, path := mustCreateWAL(t, 128*1024)
	defer conn.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("WAL size:", len(b))

	s, err := NewFullScanner(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	// Simply reading every frame and writing it back to a buffer should
	// result in the same bytes as the original WAL file.
	var buf bytes.Buffer
	w, err := NewWriter(s)
	if err != nil {
		t.Fatal(err)
	}
	n, err := w.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(b)) {
		t.Fatalf("expected to write %d bytes, wrote %d", len(b), n)
	}

	if !bytes.Equal(b, buf.Bytes()) {
		t.Fatal("writer did not write the same bytes as the reader")
	}
}

// Test_Writer_CompactingScanner tests The CompactingScanner by continuously
// using it to copy a WAL file, and using that WAL file to apply changes to a
// second database. The second database should be identical to the first in
// terms of SQL queries.
func Test_Writer_CompactingScanner(t *testing.T) {
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

	destDir := t.TempDir()
	destDSN := fmt.Sprintf("file:%s", destDir+"/dest.db")
	destDB := destDir + "/dest.db"
	destWAL := destDir + "/dest.db-wal"

	mustExec(srcConn, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")

	// Copy the src database to the dest database to seed the process.
	mustCopyFile(destDB, srcDB)

	writeRows := func(db *sql.DB, n int) {
		// Write 1000 random rows to the src database, this data will appear in the WAL.
		for i := 0; i < 1000; i++ {
			mustExec(srcConn, fmt.Sprintf(`INSERT INTO foo (name) VALUES ('%s')`, random.String()))
		}
		mustExec(srcConn, "PRAGMA wal_checkpoint(FULL)")
	}

	copyAndCompactWAL := func(sWAL, dWAL string) {
		// Copy the src WAL to the dest WAL using the CompactingScanner.
		srcF, err := os.Open(sWAL)
		if err != nil {
			t.Fatal(err)
		}
		defer srcF.Close()
		destF, err := os.Create(dWAL)
		if err != nil {
			t.Fatal(err)
		}
		defer destF.Close()
		s, err := NewCompactingScanner(srcF)
		if err != nil {
			t.Fatal(err)
		}
		w, err := NewWriter(s)
		if err != nil {
			t.Fatal(err)
		}
		_, err = w.WriteTo(destF)
		if err != nil {
			t.Fatal(err)
		}
	}

	checkRowCount := func(dsn string, n int) {
		db, err := sql.Open("sqlite3", destDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		rows, err := db.Query("SELECT COUNT(*) FROM foo")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var count int
		for rows.Next() {
			if err := rows.Scan(&count); err != nil {
				t.Fatal(err)
			}
		}
		if count != n {
			t.Fatalf("expected %d rows, got %d", n, count)
		}
	}

	writeRows(srcConn, 1000)
	copyAndCompactWAL(srcWAL, destWAL)
	checkRowCount(srcDSN, 1000)
	checkRowCount(destDSN, 1000)

	writeRows(srcConn, 1000)
	copyAndCompactWAL(srcWAL, destWAL)
	checkRowCount(srcDSN, 2000)

	checkRowCount(destDSN, 2000)

	writeRows(srcConn, 1000)
	copyAndCompactWAL(srcWAL, destWAL)
	checkRowCount(srcDSN, 3000)
	checkRowCount(destDSN, 3000)
}

func mustExec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		panic(err)
	}
}

func mustCreateWAL(t *testing.T, size int) (*sql.DB, string) {
	dir := t.TempDir()
	rwDSN := fmt.Sprintf("file:%s", dir+"/test.db")
	rwDB, err := sql.Open("sqlite3", rwDSN)
	if err != nil {
		panic(err)
	}

	mustExec(rwDB, "PRAGMA journal_mode=WAL")
	mustExec(rwDB, "PRAGMA wal_autocheckpoint=0")
	mustExec(rwDB, "PRAGMA synchronous=OFF")
	mustExec(rwDB, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	for {
		for i := 0; i < 10; i++ {
			mustExec(rwDB, "INSERT INTO test (name) VALUES ('name')")
		}
		// break if dir+test.db-wal is bigger than size
		if fi, err := os.Stat(dir + "/test.db-wal"); err != nil {
			continue
		} else {
			if fi.Size() >= int64(size) {
				break
			}
		}
	}
	return rwDB, dir + "/test.db-wal"
}

func mustCopyFile(dst, src string) {
	srcF, err := os.Open(src)
	if err != nil {
		panic(err)
	}
	defer srcF.Close()

	dstF, err := os.Create(dst)
	if err != nil {
		panic(err)
	}
	defer dstF.Close()

	_, err = io.Copy(dstF, srcF)
	if err != nil {
		panic(err)
	}
}
