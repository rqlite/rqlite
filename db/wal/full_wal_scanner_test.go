package wal

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func Test_FullScanner_Scan(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewFullWALScanner(bytes.NewReader(b))
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
		{2, 2, 4176, 8272},
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
