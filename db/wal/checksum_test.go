package wal

// Tests for reusing source checksums during WAL compaction. Expected output is
// built by naming the frames a scan should keep, so the tests do not
// reimplement the compaction rule. The WAL builder and the checksum are written
// from the format definition, so nothing here shares code with the
// implementation under test.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// testSalt is the salt written into every fixture WAL.
var testSalt = [2]uint32{0x11223344, 0x55667788}

// naiveChecksum is the SQLite WAL checksum: each 8-byte block holds two 32-bit
// words and advances the state with two additions.
func naiveChecksum(bo binary.ByteOrder, s0, s1 uint32, b []byte) [2]uint32 {
	for i := 0; i < len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return [2]uint32{s0, s1}
}

// testFrame is one frame of a fixture WAL.
type testFrame struct {
	pgno   uint32
	commit uint32
	page   []byte
}

// mkFrames builds frames from (pgno, commit) pairs. Each page body encodes its
// own index, so keeping the wrong frame changes the output bytes.
func mkFrames(pageSize uint32, specs ...[2]uint32) []testFrame {
	frames := make([]testFrame, len(specs))
	for i, s := range specs {
		page := bytes.Repeat([]byte{byte(i + 1)}, int(pageSize))
		binary.BigEndian.PutUint32(page, uint32(i))
		frames[i] = testFrame{pgno: s[0], commit: s[1], page: page}
	}
	return frames
}

// pick returns the frames at the given indexes, in order.
func pick(frames []testFrame, idx []int) []testFrame {
	out := make([]testFrame, len(idx))
	for i, j := range idx {
		out[i] = frames[j]
	}
	return out
}

// buildWAL serialises frames as a WAL, checksumming from the format definition.
func buildWAL(bigEndian bool, pageSize uint32, frames []testFrame) []byte {
	bo := binary.ByteOrder(binary.LittleEndian)
	magic := uint32(0x377f0682)
	if bigEndian {
		bo, magic = binary.BigEndian, 0x377f0683
	}

	b := make([]byte, WALHeaderSize)
	binary.BigEndian.PutUint32(b[0:], magic)
	binary.BigEndian.PutUint32(b[4:], WALSupportedVersion)
	binary.BigEndian.PutUint32(b[8:], pageSize)
	binary.BigEndian.PutUint32(b[12:], 1)
	binary.BigEndian.PutUint32(b[16:], testSalt[0])
	binary.BigEndian.PutUint32(b[20:], testSalt[1])
	cs := naiveChecksum(bo, 0, 0, b[:24])
	binary.BigEndian.PutUint32(b[24:], cs[0])
	binary.BigEndian.PutUint32(b[28:], cs[1])

	for _, f := range frames {
		hdr := make([]byte, WALFrameHeaderSize)
		binary.BigEndian.PutUint32(hdr[0:], f.pgno)
		binary.BigEndian.PutUint32(hdr[4:], f.commit)
		binary.BigEndian.PutUint32(hdr[8:], testSalt[0])
		binary.BigEndian.PutUint32(hdr[12:], testSalt[1])
		cs = naiveChecksum(bo, cs[0], cs[1], hdr[:8])
		cs = naiveChecksum(bo, cs[0], cs[1], f.page)
		binary.BigEndian.PutUint32(hdr[16:], cs[0])
		binary.BigEndian.PutUint32(hdr[20:], cs[1])
		b = append(append(b, hdr...), f.page...)
	}
	return b
}

// checkWAL verifies that the header and every frame carry the checksum the
// naive computation produces, and returns the frame count.
func checkWAL(t *testing.T, b []byte) int {
	t.Helper()
	if len(b) < WALHeaderSize {
		t.Fatalf("WAL is %d bytes, shorter than the header", len(b))
	}
	bo := binary.ByteOrder(binary.LittleEndian)
	if binary.BigEndian.Uint32(b[0:4]) == 0x377f0683 {
		bo = binary.BigEndian
	}
	pageSize := int(binary.BigEndian.Uint32(b[8:12]))

	cs := naiveChecksum(bo, 0, 0, b[:24])
	if got := ([2]uint32{binary.BigEndian.Uint32(b[24:]), binary.BigEndian.Uint32(b[28:])}); got != cs {
		t.Fatalf("header checksum = %v, want %v", got, cs)
	}

	frameSize := WALFrameHeaderSize + pageSize
	n := 0
	for off := WALHeaderSize; off+frameSize <= len(b); off += frameSize {
		hdr := b[off : off+WALFrameHeaderSize]
		cs = naiveChecksum(bo, cs[0], cs[1], hdr[:8])
		cs = naiveChecksum(bo, cs[0], cs[1], b[off+WALFrameHeaderSize:off+frameSize])
		if got := ([2]uint32{binary.BigEndian.Uint32(hdr[16:]), binary.BigEndian.Uint32(hdr[20:])}); got != cs {
			t.Fatalf("frame %d checksum = %v, want %v", n, got, cs)
		}
		n++
	}
	return n
}

// compact runs a compaction and returns the serialised result.
func compact(t *testing.T, walBytes []byte, startFrame int64, fullScan bool) []byte {
	t.Helper()
	s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), startFrame, fullScan)
	if err != nil {
		t.Fatalf("NewCompactingFrameScanner: %v", err)
	}
	w, err := NewWriter(s)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return buf.Bytes()
}

// Test_ChecksumMatrixMatchesNaiveChecksum checks that the matrix is the linear
// part of the checksum, i.e. that applying it to the incoming state equals the
// delta the checksum itself produces.
func Test_ChecksumMatrixMatchesNaiveChecksum(t *testing.T) {
	const s0, s1 = uint32(0x12345678), uint32(0x9abcdef0)
	for _, n := range []uint64{0, 1, 2, 7, 64, 65, 512, 8193} {
		b := make([]byte, n*8)
		for i := range b {
			b[i] = byte(i*7 + 3)
		}
		want := naiveChecksum(binary.LittleEndian, s0, s1, b)
		zero := naiveChecksum(binary.LittleEndian, 0, 0, b)
		if got := checksumMatrixPow(n).apply(s0, s1); got != [2]uint32{want[0] - zero[0], want[1] - zero[1]} {
			t.Fatalf("n=%d: matrix gives %v, checksum delta is %v", n, got, [2]uint32{want[0] - zero[0], want[1] - zero[1]})
		}
	}
}

// Test_Compaction checks the compacted bytes against a WAL built from the frames
// the scan should keep, and re-verifies the checksum chain of the result.
func Test_Compaction(t *testing.T) {
	// Two pages written twice, so later frames supersede earlier ones.
	dedup := [][2]uint32{{1, 1}, {2, 2}, {1, 3}, {3, 4}, {2, 5}}
	// A two-frame transaction, a single-frame transaction, then another
	// two-frame transaction: frames 0-1 commit together, 3-4 commit together.
	txs := [][2]uint32{{1, 0}, {2, 2}, {2, 3}, {1, 0}, {1, 5}}

	tests := []struct {
		name     string
		specs    [][2]uint32
		start    int64
		keep     []int
		fullScan bool
	}{
		{"dedup", dedup, 0, []int{2, 3, 4}, false},
		{"dedup/full-scan", dedup, 0, []int{2, 3, 4}, true},
		{"dedup/from frame 1", dedup, 1, []int{2, 3, 4}, false},
		{"dedup/from mid-WAL", dedup, 3, []int{3, 4}, false},
		{"dedup/from last frame", dedup, 4, []int{4}, false},
		{"txs", txs, 0, []int{2, 4}, false},
		{"txs/start on non-committing frame", txs, 3, []int{4}, false},
		{"txs/start past the transaction start", txs, 4, []int{4}, false},
	}

	for _, pageSize := range []uint32{512, 4096, 65536} {
		for _, be := range []bool{false, true} {
			for _, tc := range tests {
				name := fmt.Sprintf("%s/pageSize=%d/bigEndian=%t", tc.name, pageSize, be)
				t.Run(name, func(t *testing.T) {
					frames := mkFrames(pageSize, tc.specs...)
					src := buildWAL(be, pageSize, frames)
					want := buildWAL(be, pageSize, pick(frames, tc.keep))

					got := compact(t, src, tc.start, tc.fullScan)
					if !bytes.Equal(got, want) {
						t.Fatalf("compacted WAL is %d bytes, want %d", len(got), len(want))
					}
					if n := checkWAL(t, got); n != len(tc.keep) {
						t.Fatalf("output has %d frames, want %d", n, len(tc.keep))
					}

					// Size invariant: 32 + U*(pageSize+24), where U is the
					// number of frames kept.
					if size := WALHeaderSize + len(tc.keep)*(WALFrameHeaderSize+int(pageSize)); len(got) != size {
						t.Fatalf("output is %d bytes, want %d", len(got), size)
					}

					// Compacting the result changes nothing.
					if again := compact(t, got, 0, false); !bytes.Equal(again, got) {
						t.Fatal("compacting a compacted WAL changed it")
					}
				})
			}
		}
	}
}

// Test_BytesKeepsPositionAndData checks that Bytes() serialises the whole WAL
// whatever position the scan has reached, and that it leaves the position and
// the frame data already handed out untouched.
func Test_BytesKeepsPositionAndData(t *testing.T) {
	frames := mkFrames(512, [2]uint32{1, 1}, [2]uint32{2, 2}, [2]uint32{3, 3})
	s, err := NewCompactingFrameScanner(bytes.NewReader(buildWAL(false, 512, frames)), 0, false)
	if err != nil {
		t.Fatalf("NewCompactingFrameScanner: %v", err)
	}

	all, err := s.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	held, err := s.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	want := append([]byte(nil), held.Data...) // Data aliases the page buffer

	again, err := s.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if !bytes.Equal(all, again) {
		t.Fatal("Bytes() output depends on the scan position")
	}
	if !bytes.Equal(held.Data, want) {
		t.Fatal("Bytes() overwrote the page data of a frame handed out by Next")
	}
	if f, err := s.Next(); err != nil || f.Pgno != 2 {
		t.Fatalf("scan position was disturbed: pgno %v, err %v", f, err)
	}
}

// Test_WriteAfterSkippingFrames checks that the checksum chain follows the
// frames actually written: consuming a frame before handing the rest to a
// Writer still yields a WAL that verifies from its first byte.
func Test_WriteAfterSkippingFrames(t *testing.T) {
	frames := mkFrames(512, [2]uint32{1, 1}, [2]uint32{2, 2}, [2]uint32{3, 3}, [2]uint32{4, 4})
	s, err := NewCompactingFrameScanner(bytes.NewReader(buildWAL(false, 512, frames)), 0, false)
	if err != nil {
		t.Fatalf("NewCompactingFrameScanner: %v", err)
	}
	if _, err := s.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}

	w, err := NewWriter(s)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	// The first frame was consumed, so the output starts at page 2.
	want := buildWAL(false, 512, frames[1:])
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatal("output does not match the frames the Writer was handed")
	}
	if n := checkWAL(t, buf.Bytes()); n != 3 {
		t.Fatalf("output has %d frames, want 3", n)
	}
}

// Test_ScanErrors checks where the checksum seed comes from and how failures
// are reported. A seed error is only reported when frames were found: a start
// position past the last frame is a successful empty result, because nothing
// ever uses the seed.
func Test_ScanErrors(t *testing.T) {
	const pageSize, start = 512, 5
	const frameSize = WALFrameHeaderSize + pageSize
	const prevHdr = WALHeaderSize + (start-1)*frameSize

	frames := mkFrames(pageSize, [][2]uint32{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}}...)
	src := buildWAL(false, pageSize, frames)

	tests := []struct {
		name   string
		mutate func(*[]byte)
		fault  func(*faultReader)
		start  int64
		errMsg string // empty means the scan must succeed
		empty  bool
	}{
		{name: "seed seek fails", start: start, fault: func(f *faultReader) { f.seekAt = prevHdr }, errMsg: "seek to preceding frame"},
		{name: "seed read fails", start: start, fault: func(f *faultReader) { f.failReadAt = prevHdr }, errMsg: "read preceding frame header"},
		{name: "seed salt mismatch", start: start, mutate: func(b *[]byte) { binary.BigEndian.PutUint32((*b)[prevHdr+8:], 0xdeadbeef) }, errMsg: "different WAL"},
		{name: "output ignores the preceding page", start: start, mutate: func(b *[]byte) { (*b)[prevHdr+WALFrameHeaderSize] ^= 0xff }},
		{name: "start past the end", start: int64(len(frames) + 1), empty: true},
		{
			name:   "open transaction",
			start:  0,
			mutate: func(b *[]byte) { binary.BigEndian.PutUint32((*b)[WALHeaderSize+(len(frames)-1)*frameSize+4:], 0) },
			errMsg: ErrOpenTransaction.Error(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := append([]byte(nil), src...)
			if tc.mutate != nil {
				tc.mutate(&b)
			}
			fs := &faultReader{rs: bytes.NewReader(b), seekAt: -1, failReadAt: -1, readErrAt: -1}
			if tc.fault != nil {
				tc.fault(fs)
			}

			s, err := NewCompactingFrameScanner(fs, tc.start, false)
			if tc.errMsg == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if s.Empty() != tc.empty {
					t.Fatalf("empty = %t, want %t", s.Empty(), tc.empty)
				}
				// The seed comes from the frame header, so changing the page
				// data of the preceding frame must not change the output.
				if !bytes.Equal(compact(t, b, tc.start, false), compact(t, src, tc.start, false)) {
					t.Fatal("output changed when only the preceding page data changed")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.errMsg) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.errMsg)
			}
		})
	}
}

// Test_WriterNeedsSourceChecksums checks that the Writer advances its checksum
// from the frame's source transition without reading the page, and refuses a
// frame that carries none.
func Test_WriterNeedsSourceChecksums(t *testing.T) {
	page := make([]byte, 512)
	for i := range page {
		page[i] = byte(i)
	}
	write := func(f *Frame) ([]byte, error) {
		var buf bytes.Buffer
		w, err := NewWriter(&oneFrame{f: f})
		if err != nil {
			t.Fatal(err)
		}
		_, err = w.WriteTo(&buf)
		return buf.Bytes(), err
	}

	// A transition no hash of these bytes could produce must be written
	// verbatim, proving the page is not hashed.
	seed := naiveChecksum(binary.LittleEndian, 0, 0, buildWAL(false, 512, nil)[:24])
	f := &Frame{
		Pgno: 3, Commit: 1, Data: page,
		Prev: seed, Source: [2]uint32{0x01020304, 0x05060708},
		HasChecksums: true,
	}
	out, err := write(f)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if got := binary.BigEndian.Uint32(out[WALHeaderSize+16:]); got != f.Source[0] {
		t.Fatalf("checksum = %#x, want %#x", got, f.Source[0])
	}

	f.HasChecksums = false
	if _, err := write(f); err == nil {
		t.Fatal("expected an error for a frame without source checksums")
	}
}

// oneFrame is a WALIterator yielding a single frame.
type oneFrame struct {
	f    *Frame
	done bool
}

func (o *oneFrame) Header() (*WALHeader, error) {
	h := buildWAL(false, uint32(len(o.f.Data)), nil)
	return &WALHeader{
		Magic:     binary.BigEndian.Uint32(h[0:4]),
		Version:   WALSupportedVersion,
		PageSize:  uint32(len(o.f.Data)),
		Seq:       binary.BigEndian.Uint32(h[12:16]),
		Salt1:     testSalt[0],
		Salt2:     testSalt[1],
		Checksum1: binary.BigEndian.Uint32(h[24:28]),
		Checksum2: binary.BigEndian.Uint32(h[28:32]),
	}, nil
}

func (o *oneFrame) Next() (*Frame, error) {
	if o.done {
		return nil, io.EOF
	}
	o.done = true
	return o.f, nil
}

// Test_IOErrors checks that I/O failures in the scan, the iterator, and Bytes()
// are reported rather than producing a silently truncated WAL.
func Test_IOErrors(t *testing.T) {
	walBytes := buildWAL(false, 512, mkFrames(512, [][2]uint32{{1, 1}, {2, 2}, {3, 3}}...))

	// Each case prepares a fault and says whether the scan itself must fail,
	// or whether the failure must appear from Next() or Bytes() instead.
	tests := []struct {
		name   string
		fault  func(*faultReader)
		during string // "next" or "bytes" when the scan must succeed first
	}{
		{"seek to scan start", func(f *faultReader) { f.seekAt = WALHeaderSize }, ""},
		{"read during scan", func(f *faultReader) { f.readErrAt = WALHeaderSize + WALFrameHeaderSize }, ""},
		{"read during iteration", func(f *faultReader) { f.readErrAt = 0 }, "next"},
		{"seek during iteration", func(f *faultReader) { f.seekAt = WALHeaderSize + WALFrameHeaderSize }, "next"},
		{"read during Bytes", func(f *faultReader) { f.readErrAt = 0 }, "bytes"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &faultReader{rs: bytes.NewReader(walBytes), seekAt: -1, failReadAt: -1, readErrAt: -1}
			switch tc.during {
			case "":
				tc.fault(f)
				if _, err := NewCompactingFrameScanner(f, 0, false); err == nil {
					t.Fatal("expected an error from the scan")
				}
			case "next":
				s, err := NewCompactingFrameScanner(f, 0, false)
				if err != nil {
					t.Fatalf("NewCompactingFrameScanner: %v", err)
				}
				if _, err := s.Next(); err != nil {
					t.Fatalf("first Next: %v", err)
				}
				if tc.fault != nil && tc.name == "seek during iteration" {
					f.seekAt = s.frames[1].Offset + WALFrameHeaderSize
				} else {
					tc.fault(f)
				}
				if _, err := s.Next(); err == nil {
					t.Fatal("expected an error from Next")
				}
			case "bytes":
				s, err := NewCompactingFrameScanner(f, 0, false)
				if err != nil {
					t.Fatalf("NewCompactingFrameScanner: %v", err)
				}
				tc.fault(f)
				if _, err := s.Bytes(); err == nil {
					t.Fatal("expected an error from Bytes")
				}
			}
		})
	}
}

// faultReader injects I/O failures so the scanner's error paths can be
// exercised.
type faultReader struct {
	rs         io.ReadSeeker
	seekAt     int64 // SeekStart to exactly this offset fails
	failReadAt int64 // the first Read starting at exactly this offset fails
	readErrAt  int64 // every Read at or beyond this offset fails
}

func (f *faultReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset == f.seekAt {
		return 0, errors.New("injected seek error")
	}
	return f.rs.Seek(offset, whence)
}

func (f *faultReader) Read(p []byte) (int, error) {
	pos, err := f.rs.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if pos == f.failReadAt {
		f.failReadAt = -1
		return 0, errors.New("injected read error")
	}
	if f.readErrAt >= 0 && pos >= f.readErrAt {
		return 0, errors.New("injected read error")
	}
	return f.rs.Read(p)
}
