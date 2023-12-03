package wal

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/rqlite/go-sqlite3"
)

func Test_NewWriter_FullScanner(t *testing.T) {
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

func Test_NewWriter_FullScanner_LargeWAL(t *testing.T) {
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

func mustCreateWAL(t *testing.T, size int) (*sql.DB, string) {
	dir := t.TempDir()
	rwDSN := fmt.Sprintf("file:%s", dir+"/test.db")
	rwDB, err := sql.Open("sqlite3", rwDSN)
	if err != nil {
		panic(err)
	}

	mustExec := func(query string) {
		if _, err := rwDB.Exec(query); err != nil {
			panic(err)
		}
	}

	mustExec("PRAGMA journal_mode=WAL")
	mustExec("PRAGMA wal_autocheckpoint=0")
	mustExec("PRAGMA synchronous=OFF")
	mustExec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	for {
		for i := 0; i < 10; i++ {
			mustExec("INSERT INTO test (name) VALUES ('name')")
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
