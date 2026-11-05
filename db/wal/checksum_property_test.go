package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"testing"
)

// synthWAL builds a WAL from scratch, independently of the production code, so
// tests have an oracle for the expected compacted bytes. It supports both
// checksum byte orders and arbitrary page sizes.
//
// Checksums here come from refChecksum (see checksum_oracle_test.go), not from
// the production checksum function, so the fixture itself is independent.
type synthWAL struct {
	magic    uint32
	bo       binary.ByteOrder
	pageSize uint32
	seq      uint32
	salt1    uint32
	salt2    uint32
	buf      []byte
	c1, c2   uint32
}

func newSynthWAL(bigEndian bool, pageSize uint32) *synthWAL {
	w := &synthWAL{
		magic:    0x377f0682,
		bo:       binary.LittleEndian,
		pageSize: pageSize,
		seq:      7,
		salt1:    0x11223344,
		salt2:    0x55667788,
	}
	if bigEndian {
		w.magic = 0x377f0683
		w.bo = binary.BigEndian
	}

	hdr := make([]byte, WALHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:], w.magic)
	binary.BigEndian.PutUint32(hdr[4:], WALSupportedVersion)
	binary.BigEndian.PutUint32(hdr[8:], w.pageSize)
	binary.BigEndian.PutUint32(hdr[12:], w.seq)
	binary.BigEndian.PutUint32(hdr[16:], w.salt1)
	binary.BigEndian.PutUint32(hdr[20:], w.salt2)

	cs := refChecksum(w.bo, 0, 0, hdr[:24])
	w.c1, w.c2 = cs[0], cs[1]
	binary.BigEndian.PutUint32(hdr[24:], w.c1)
	binary.BigEndian.PutUint32(hdr[28:], w.c2)
	w.buf = hdr
	return w
}

func (w *synthWAL) addFrame(pgno, commit uint32, page []byte) {
	if uint32(len(page)) != w.pageSize {
		panic(fmt.Sprintf("page is %d bytes, want %d", len(page), w.pageSize))
	}

	hdr := make([]byte, WALFrameHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:], pgno)
	binary.BigEndian.PutUint32(hdr[4:], commit)
	binary.BigEndian.PutUint32(hdr[8:], w.salt1)
	binary.BigEndian.PutUint32(hdr[12:], w.salt2)

	cs := refChecksum(w.bo, w.c1, w.c2, hdr[:8])
	cs = refChecksum(w.bo, cs[0], cs[1], page)
	w.c1, w.c2 = cs[0], cs[1]
	binary.BigEndian.PutUint32(hdr[16:], w.c1)
	binary.BigEndian.PutUint32(hdr[20:], w.c2)

	w.buf = append(w.buf, hdr...)
	w.buf = append(w.buf, page...)
}

type synthFrame struct {
	pgno   uint32
	commit uint32
	page   []byte
	index  int
}

// makeSynthFrames produces nFrames frames grouped into transactions of 1-5
// frames. Only the last frame of each transaction carries a commit value.
func makeSynthFrames(pageSize uint32, nFrames int, seed int64) []synthFrame {
	rng := rand.New(rand.NewSource(seed))
	frames := make([]synthFrame, 0, nFrames)
	for len(frames) < nFrames {
		txLen := 1 + rng.Intn(5)
		if remain := nFrames - len(frames); txLen > remain {
			txLen = remain
		}
		for i := range txLen {
			page := make([]byte, pageSize)
			rng.Read(page)
			// Make every page distinct, even for the same pgno, so a wrong
			// "latest version" choice cannot pass unnoticed.
			binary.BigEndian.PutUint32(page, uint32(len(frames)))
			f := synthFrame{
				pgno:  uint32(1 + rng.Intn(12)),
				page:  page,
				index: len(frames),
			}
			if i == txLen-1 {
				f.commit = uint32(f.index + 1)
			}
			frames = append(frames, f)
		}
	}
	return frames
}

// expectedCompaction is an independent implementation of the compaction
// contract: keep the last committed version of every page from start onwards,
// in file offset order.
func expectedCompaction(frames []synthFrame, start int) []synthFrame {
	committed := make(map[uint32]synthFrame)
	tx := make(map[uint32]synthFrame)
	for _, f := range frames[start:] {
		tx[f.pgno] = f
		if f.commit == 0 {
			continue
		}
		for pgno, cf := range tx {
			committed[pgno] = cf
		}
		tx = make(map[uint32]synthFrame)
	}

	out := make([]synthFrame, 0, len(committed))
	for _, f := range committed {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].index < out[j].index })
	return out
}

func buildSynthWAL(bigEndian bool, pageSize uint32, frames []synthFrame) []byte {
	w := newSynthWAL(bigEndian, pageSize)
	for _, f := range frames {
		w.addFrame(f.pgno, f.commit, f.page)
	}
	return w.buf
}

func verifyWALChecksums(t *testing.T, b []byte) int {
	t.Helper()
	fs, err := NewFullScanner(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("NewFullScanner: %v", err)
	}
	n := 0
	for {
		if _, err := fs.Next(); err == io.EOF {
			return n
		} else if err != nil {
			t.Fatalf("compacted WAL failed checksum verification at frame %d: %v", n, err)
		}
		n++
	}
}

// Test_CompactingFrameScanner_Synthetic answers "is the derived checksum path
// correct?" for combinations the fixture WALs cannot cover: both checksum byte
// orders, a range of page sizes, duplicated pages spread across transactions,
// and scans resumed at arbitrary frame boundaries (including mid-transaction).
func Test_CompactingFrameScanner_Synthetic(t *testing.T) {
	type cfg struct {
		pageSize uint32
		nFrames  int
	}
	pageSizes := []cfg{
		{512, 800},
		{1024, 500},
		{4096, 200},
		{8192, 100},
		{65536, 20},
	}

	for _, be := range []bool{false, true} {
		for _, c := range pageSizes {
			frames := makeSynthFrames(c.pageSize, c.nFrames, int64(c.pageSize)+endianSeed(be))
			walBytes := buildSynthWAL(be, c.pageSize, frames)
			frameSize := WALFrameHeaderSize + int(c.pageSize)
			if want := WALHeaderSize + len(frames)*frameSize; len(walBytes) != want {
				t.Fatalf("source WAL is %d bytes, want %d", len(walBytes), want)
			}

			starts := []int{0, 1, len(frames) / 3, len(frames) - 1}
			for _, start := range starts {
				name := fmt.Sprintf("pageSize=%d/bigEndian=%t/start=%d", c.pageSize, be, start)
				t.Run(name, func(t *testing.T) {
					derived := compactToBytes(t, walBytes, int64(start), false, true)
					recomputed := compactToBytes(t, walBytes, int64(start), false, false)
					if !bytes.Equal(derived, recomputed) {
						t.Fatal("derived checksums differ from recomputed checksums")
					}

					expectedFrames := expectedCompaction(frames, start)
					expected := buildSynthWAL(be, c.pageSize, expectedFrames)
					if !bytes.Equal(derived, expected) {
						t.Fatalf("compacted WAL differs from independent expectation (%d bytes vs %d)",
							len(derived), len(expected))
					}

					// The invariant this optimization is about: N frames in,
					// U unique pages out.
					wantLen := WALHeaderSize + len(expectedFrames)*frameSize
					if len(derived) != wantLen {
						t.Fatalf("compacted WAL is %d bytes, want %d", len(derived), wantLen)
					}
					if got := verifyWALChecksums(t, derived); got != len(expectedFrames) {
						t.Fatalf("verified %d frames, want %d", got, len(expectedFrames))
					}
				})
			}
		}
	}
}

// endianSeed gives the big-endian variant a different frame layout, so a bug
// that only shows up with one ordering is not masked by identical inputs.
func endianSeed(bigEndian bool) int64 {
	if bigEndian {
		return 12345
	}
	return 0
}

// Test_CompactingFrameScanner_BytesPreservesPosition checks that Bytes()
// serialises the whole WAL regardless of how many frames Next() has already
// returned, and that it leaves the scan position untouched.
func Test_CompactingFrameScanner_BytesPreservesPosition(t *testing.T) {
	frames := makeSynthFrames(512, 30, 77)
	walBytes := buildSynthWAL(false, 512, frames)

	s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), 0, false)
	if err != nil {
		t.Fatalf("NewCompactingFrameScanner: %v", err)
	}
	if len(s.frames) < 3 {
		t.Fatalf("need at least 3 compacted frames, got %d", len(s.frames))
	}

	before, err := s.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}

	// Consume two frames, then serialise again.
	if _, err := s.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if _, err := s.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	mid, err := s.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if !bytes.Equal(before, mid) {
		t.Fatal("Bytes() output depends on the scan position")
	}

	// The position must still be at the third frame.
	f, err := s.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if f.Pgno != s.frames[2].Pgno || f.Commit != s.frames[2].Commit {
		t.Fatalf("after Bytes, Next returned pgno=%d commit=%d, want pgno=%d commit=%d",
			f.Pgno, f.Commit, s.frames[2].Pgno, s.frames[2].Commit)
	}
}

// Test_CompactingFrameScanner_Idempotent checks that compacting an already
// compacted WAL is a no-op, which is what makes repeated checkpoints cheap.
func Test_CompactingFrameScanner_Idempotent(t *testing.T) {
	frames := makeSynthFrames(1024, 300, 99)
	walBytes := buildSynthWAL(false, 1024, frames)

	once := compactToBytes(t, walBytes, 0, false, true)
	twice := compactToBytes(t, once, 0, false, true)
	if !bytes.Equal(once, twice) {
		t.Fatal("compacting a compacted WAL changed it")
	}
}

// Test_CompactingFrameScanner_ResumedScanDerives checks that a resumed scan
// also derives its checksums instead of checksumming page data, using the
// preceding frame header as the seed.
func Test_CompactingFrameScanner_ResumedScanDerives(t *testing.T) {
	frames := makeSynthFrames(512, 120, 4242)
	walBytes := buildSynthWAL(false, 512, frames)

	const start = 7
	want := compactToBytes(t, walBytes, start, false, true)

	s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), start, false)
	if err != nil {
		t.Fatalf("NewCompactingFrameScanner: %v", err)
	}
	if len(s.frames) == 0 {
		t.Fatal("expected surviving frames")
	}
	for i, f := range s.frames {
		if f.out == [2]uint32{} {
			t.Fatalf("frame %d has no derived checksum on a resumed scan", i)
		}
	}

	got, err := s.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("Bytes() output differs from Writer output")
	}
	verifyWALChecksums(t, got)
}

// Test_CompactingFrameScanner_ResumedScanSeedsFromPrecedingFrame checks that
// the seed really comes from the preceding frame header: corrupting that
// frame's checksum field changes the output, while corrupting its page data
// does not.
func Test_CompactingFrameScanner_ResumedScanSeedsFromPrecedingFrame(t *testing.T) {
	// Every page number is distinct, so the first scanned frame is guaranteed
	// to survive and its seed is guaranteed to be used.
	frames := makeBenchFrames(512, 120, 120)
	walBytes := buildSynthWAL(false, 512, frames)

	const start = 9
	const frameSize = WALFrameHeaderSize + 512
	base := compactToBytes(t, walBytes, start, false, true)

	t.Run("checksum field", func(t *testing.T) {
		corrupt := append([]byte(nil), walBytes...)
		prevHdr := WALHeaderSize + (start-1)*frameSize
		binary.BigEndian.PutUint32(corrupt[prevHdr+16:], 0xdeadbeef)
		got := compactToBytes(t, corrupt, start, false, true)
		if bytes.Equal(got, base) {
			t.Fatal("output did not change when the seed checksum changed")
		}
	})

	t.Run("page data", func(t *testing.T) {
		corrupt := append([]byte(nil), walBytes...)
		prevPage := WALHeaderSize + (start-1)*frameSize + WALFrameHeaderSize
		corrupt[prevPage] ^= 0xff
		got := compactToBytes(t, corrupt, start, false, true)
		if !bytes.Equal(got, base) {
			t.Fatal("output changed when only the preceding page data changed")
		}
	})
}

// faultReadSeeker injects I/O errors at specific offsets so the scanner's
// defensive error paths can be exercised.
type faultReadSeeker struct {
	rs            io.ReadSeeker
	seekErrOffset int64 // SeekStart to exactly this offset fails; -1 disables
	readErrOffset int64 // Read at or beyond this offset fails; -1 disables
}

func newFaultReadSeeker(b []byte) *faultReadSeeker {
	return &faultReadSeeker{rs: bytes.NewReader(b), seekErrOffset: -1, readErrOffset: -1}
}

func (f *faultReadSeeker) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset == f.seekErrOffset {
		return 0, errors.New("injected seek error")
	}
	return f.rs.Seek(offset, whence)
}

func (f *faultReadSeeker) Read(p []byte) (int, error) {
	if f.readErrOffset >= 0 {
		if pos, err := f.rs.Seek(0, io.SeekCurrent); err == nil && pos >= f.readErrOffset {
			return 0, errors.New("injected read error")
		}
	}
	return f.rs.Read(p)
}

// Test_CompactingFrameScanner_NextErrorPropagates checks that read and seek
// failures during iteration surface to the caller instead of being silently
// turned into a truncated WAL.
func Test_CompactingFrameScanner_NextErrorPropagates(t *testing.T) {
	frames := makeSynthFrames(512, 20, 3)
	walBytes := buildSynthWAL(false, 512, frames)

	t.Run("read", func(t *testing.T) {
		s, err := NewCompactingFrameScanner(newFaultReadSeeker(walBytes), 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Next(); err != nil {
			t.Fatalf("first Next: %v", err)
		}
		s.readSeeker.(*faultReadSeeker).readErrOffset = 0
		if _, err := s.Next(); err == nil {
			t.Fatal("expected read error from Next")
		}
	})

	t.Run("seek", func(t *testing.T) {
		fs := newFaultReadSeeker(walBytes)
		s, err := NewCompactingFrameScanner(fs, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(s.frames) < 2 {
			t.Fatal("need at least 2 compacted frames")
		}
		fs.seekErrOffset = s.frames[1].Offset + WALFrameHeaderSize
		if _, err := s.Next(); err != nil {
			t.Fatalf("first Next: %v", err)
		}
		if _, err := s.Next(); err == nil {
			t.Fatal("expected seek error from Next")
		}
	})
}

// Test_Writer_UsesProvidedChecksums proves the Writer really consumes the
// derived checksums instead of rehashing page data: a frame whose Data does
// not hash to the supplied checksums is written verbatim.
func Test_Writer_UsesProvidedChecksums(t *testing.T) {
	const pageSize = 512
	src := newSynthWAL(false, pageSize)

	page := make([]byte, pageSize)
	for i := range page {
		page[i] = byte(i)
	}

	writeOne := func(f *Frame) []byte {
		var buf bytes.Buffer
		w, err := NewWriter(&singleFrameIterator{frame: f})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}

	// Deliberately inconsistent checksums: no real hash of these bytes could
	// produce them.
	provided := &Frame{
		Pgno:        3,
		Commit:      1,
		Data:        page,
		Checksum:    [2]uint32{0x01020304, 0x05060708},
		HasChecksum: true,
	}
	out := writeOne(provided)
	if got := binary.BigEndian.Uint32(out[WALHeaderSize+16:]); got != provided.Checksum[0] {
		t.Fatalf("checksum1 = %#x, want %#x", got, provided.Checksum[0])
	}
	if got := binary.BigEndian.Uint32(out[WALHeaderSize+20:]); got != provided.Checksum[1] {
		t.Fatalf("checksum2 = %#x, want %#x", got, provided.Checksum[1])
	}

	// Without the pre-derived checksums the Writer must hash the page data,
	// and the result must differ from the bogus values above.
	provided.HasChecksum = false
	out = writeOne(provided)
	if got := binary.BigEndian.Uint32(out[WALHeaderSize+16:]); got == provided.Checksum[0] {
		t.Fatal("Writer did not recompute the checksum")
	}
	verifyWALChecksums(t, append(src.buf, out[WALHeaderSize:]...))
}

type singleFrameIterator struct {
	frame *Frame
	done  bool
}

func (s *singleFrameIterator) Header() (*WALHeader, error) {
	pageSize := uint32(len(s.frame.Data))
	h := newSynthWAL(false, pageSize)
	return &WALHeader{
		Magic:     h.magic,
		Version:   WALSupportedVersion,
		PageSize:  h.pageSize,
		Seq:       h.seq,
		Salt1:     h.salt1,
		Salt2:     h.salt2,
		Checksum1: h.c1,
		Checksum2: h.c2,
	}, nil
}

func (s *singleFrameIterator) Next() (*Frame, error) {
	if s.done {
		return nil, io.EOF
	}
	s.done = true
	return s.frame, nil
}

// makeBenchFrames produces frames whose page numbers span nPgno distinct
// values, so deduplication keeps a controlled fraction of the input.
func makeBenchFrames(pageSize uint32, n, nPgno int) []synthFrame {
	rng := rand.New(rand.NewSource(7))
	frames := make([]synthFrame, n)
	for i := range frames {
		page := make([]byte, pageSize)
		rng.Read(page)
		frames[i] = synthFrame{
			pgno:   uint32(1 + i%nPgno),
			commit: uint32(i + 1),
			page:   page,
			index:  i,
		}
	}
	return frames
}

func BenchmarkCompactingFrameScanner_WriteTo(b *testing.B) {
	for _, bp := range []struct {
		name  string
		nPgno int
	}{
		{"no-dedup", 2000}, // every frame survives: worst case for re-checksumming
		{"high-dedup", 12}, // few pages survive: checksum work is negligible
	} {
		frames := makeBenchFrames(4096, 2000, bp.nPgno)
		walBytes := buildSynthWAL(false, 4096, frames)
		b.Run(bp.name, func(b *testing.B) {
			benchWriteTo(b, walBytes)
		})
	}
}

func benchWriteTo(b *testing.B, walBytes []byte) {
	for _, bc := range []struct {
		name   string
		derive bool
	}{
		{"derived-checksums", true},
		{"recomputed-checksums", false},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.SetBytes(int64(len(walBytes)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), 0, false)
				if err != nil {
					b.Fatal(err)
				}
				var it WALIterator = s
				if !bc.derive {
					it = noChecksumIterator{s}
				}
				w, err := NewWriter(it)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := w.WriteTo(io.Discard); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
