package wal

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func Test_CompactingScanner_Scan(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewCompactingScanner(bytes.NewReader(b), false)
	if err != nil {
		t.Fatal(err)
	}

	// The test WAL has 3 frames, 2 unique pages after compaction.
	n := 0
	for {
		_, err := s.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 2 {
		t.Fatalf("expected 2 compacted frames, got %d", n)
	}
}

func Test_CompactingScanner_Scan_Empty(t *testing.T) {
	_, err := NewCompactingScanner(bytes.NewReader([]byte{}), true)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}
