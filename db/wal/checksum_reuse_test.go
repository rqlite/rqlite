package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"
)

// noChecksumIterator strips pre-derived checksums from frames, forcing Writer
// to checksum the page data itself.
type noChecksumIterator struct {
	WALIterator
}

func (n noChecksumIterator) Next() (*Frame, error) {
	f, err := n.WALIterator.Next()
	if f != nil {
		f.HasChecksum = false
	}
	return f, err
}

// compactToBytes compacts the given WAL and serializes it. When derive is
// false, any pre-derived checksums are discarded so the Writer has to
// checksum each page itself.
func compactToBytes(t *testing.T, b []byte, startFrame int64, fullScan, derive bool) []byte {
	t.Helper()

	s, err := NewCompactingFrameScanner(bytes.NewReader(b), startFrame, fullScan)
	if err != nil {
		t.Fatalf("NewCompactingFrameScanner: %v", err)
	}
	var it WALIterator = s
	if !derive {
		it = noChecksumIterator{s}
	}
	w, err := NewWriter(it)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return buf.Bytes()
}

// Test_ChecksumMatrixPow checks that A^n is the linear part of the WAL
// checksum over n 8-byte blocks, for exponents that include 0, the sizes of
// real page checksum passes, and a couple of others. The expected values come
// from refChecksum, the test-local implementation of the checksum.
func Test_ChecksumMatrixPow(t *testing.T) {
	for _, n := range []uint64{0, 1, 2, 3, 8, 39, 40, 64, 65, 128, 1024, 8193} {
		b := make([]byte, n*8)
		for i := range b {
			b[i] = byte(i*7 + 3)
		}
		s0, s1 := uint32(0x12345678), uint32(0x9abcdef0)

		c := refChecksum(binary.LittleEndian, s0, s1, b)
		z := refChecksum(binary.LittleEndian, 0, 0, b)

		// The checksum is affine, so the linear part applied to the initial
		// state equals the checksum delta caused by that state.
		m := checksumMatrixPow(n).apply(s0, s1)
		if m[0] != c[0]-z[0] || m[1] != c[1]-z[1] {
			t.Fatalf("n=%d: matrix apply = (%d, %d), checksum delta = (%d, %d)",
				n, m[0], m[1], c[0]-z[0], c[1]-z[1])
		}
	}
}

// Test_CompactingFrameScanner_DerivedChecksums checks that checksums derived
// from the source WAL are byte-for-byte identical to checksums recomputed from
// the page data.
func Test_CompactingFrameScanner_DerivedChecksums(t *testing.T) {
	b, err := os.ReadFile("testdata/wal-reader/ok/wal")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		start    int64
		fullScan bool
	}{
		{"all", 0, false},
		{"all-fullscan", 0, true},
		{"resumed", 1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			derived := compactToBytes(t, b, tc.start, tc.fullScan, true)
			recomputed := compactToBytes(t, b, tc.start, tc.fullScan, false)
			if !bytes.Equal(derived, recomputed) {
				t.Fatal("checksums derived from source WAL differ from recomputed checksums")
			}

			// Bytes() must use the same derived checksums.
			s, err := NewCompactingFrameScanner(bytes.NewReader(b), tc.start, tc.fullScan)
			if err != nil {
				t.Fatal(err)
			}
			bb, err := s.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(bb, derived) {
				t.Fatal("Bytes() output differs from Writer output")
			}

			// The output must be a valid WAL with a consistent checksum chain.
			fs, err := NewFullScanner(bytes.NewReader(derived))
			if err != nil {
				t.Fatal(err)
			}
			for {
				if _, err := fs.Next(); err == io.EOF {
					break
				} else if err != nil {
					t.Fatalf("output WAL failed checksum verification: %v", err)
				}
			}
		})
	}
}

// Test_CompactingFrameScanner_DedupSize checks the compaction invariant that a
// WAL of 32 + N*(pageSize+24) bytes becomes 32 + U*(pageSize+24) bytes, where U
// is the number of unique pages, and that derived checksums match the
// recomputed ones on a real SQLite WAL.
func Test_CompactingFrameScanner_DedupSize(t *testing.T) {
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
	hdr, err := s.Header()
	if err != nil {
		t.Fatal(err)
	}
	frameSize := WALFrameHeaderSize + int(hdr.PageSize)
	if (len(b)-WALHeaderSize)%frameSize != 0 {
		t.Fatalf("source WAL has trailing partial frame")
	}
	inFrames := (len(b) - WALHeaderSize) / frameSize

	derived := compactToBytes(t, b, 0, false, true)
	if (len(derived)-WALHeaderSize)%frameSize != 0 {
		t.Fatalf("compacted WAL has trailing partial frame")
	}
	outFrames := (len(derived) - WALHeaderSize) / frameSize

	if outFrames >= inFrames {
		t.Fatalf("expected deduplication: %d input frames, %d output frames", inFrames, outFrames)
	}
	if want := WALHeaderSize + outFrames*frameSize; len(derived) != want {
		t.Fatalf("compacted WAL is %d bytes, want %d", len(derived), want)
	}

	recomputed := compactToBytes(t, b, 0, false, false)
	if !bytes.Equal(derived, recomputed) {
		t.Fatal("checksums derived from source WAL differ from recomputed checksums")
	}
}
