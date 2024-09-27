package wal

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/rqlite/go-sqlite3"
)

func Test_CompactingScanner_Scan_Empty(t *testing.T) {
	_, err := NewCompactingScanner(bytes.NewReader([]byte{}), true)
	if err != io.EOF {
		t.Fatal(err)
	}
}

func Test_CompactingScanner_Scan(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingScanner(bytes.NewReader(b), true)
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

func Test_CompactingScanner_Scan_Commit0(t *testing.T) {
	b, err := os.ReadFile("testdata/compacting-scanner/commit-0/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingScanner(bytes.NewReader(b), false)
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

func Test_CompactingScanner_Bytes(t *testing.T) {
	conn, path := mustCreateWAL(t, 128*1024)
	defer conn.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingScanner(bytes.NewReader(b), false)
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

func Test_CompactingScanner_Bytes_FullCycle(t *testing.T) {
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
	s, err := NewCompactingScanner(walFD, false)
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

/* MIT License
 *
 * Copyright (c) 2017 Roland Singer [roland.singer@desertbit.com]
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

// copyFile copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file. The file mode will be copied from the source and
// the copied data is synced/flushed to stable storage.
func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		if e := out.Close(); e != nil {
			err = e
		}
	}()

	_, err = io.Copy(out, in)
	if err != nil {
		return
	}

	err = out.Sync()
	if err != nil {
		return
	}

	si, err := os.Stat(src)
	if err != nil {
		return
	}
	err = os.Chmod(dst, si.Mode())
	if err != nil {
		return
	}

	return
}

// copyDir recursively copies a directory tree, attempting to preserve permissions.
// Source directory must exist, destination directory must *not* exist.
// Symlinks are ignored and skipped.
func copyDir(src string, dst string) (err error) {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !si.IsDir() {
		return fmt.Errorf("source is not a directory")
	}

	_, err = os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	if err == nil {
		return fmt.Errorf("destination already exists")
	}

	err = os.MkdirAll(dst, si.Mode())
	if err != nil {
		return
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			err = copyDir(srcPath, dstPath)
			if err != nil {
				return
			}
		} else {
			// Skip symlinks.
			if entry.Type()&fs.ModeSymlink != 0 {
				continue
			}

			err = copyFile(srcPath, dstPath)
			if err != nil {
				return
			}
		}
	}

	return
}
