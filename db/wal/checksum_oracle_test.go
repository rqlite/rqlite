package wal

// This file is a test oracle for the WAL checksum. It implements the SQLite WAL
// format and its running checksum from scratch, without calling into the
// production code, so derived checksums can be compared against an independent
// calculation rather than against another code path in the same package.
//
// Nothing here is used by production code.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"testing"
)

// refChecksum computes the SQLite WAL checksum over b, 8 bytes at a time.
// Written directly from the format description: each 8-byte block holds two
// 32-bit words (x, y) and advances the state as
//
//	s0 += x + s1
//	s1 += y + s0
//
// The byte order used to read the words is that of the WAL header magic.
func refChecksum(bo binary.ByteOrder, s0, s1 uint32, b []byte) [2]uint32 {
	if len(b)%8 != 0 {
		panic(fmt.Sprintf("refChecksum: %d bytes is not a multiple of 8", len(b)))
	}
	for i := 0; i < len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return [2]uint32{s0, s1}
}

// refFrame is one frame parsed out of a WAL file.
type refFrame struct {
	header []byte // the 24-byte frame header, as stored in the file
	page   []byte
	pgno   uint32
	commit uint32
	index  int       // 0-based position in the file
	after  [2]uint32 // cumulative checksum after this frame
}

// refWAL is a parsed WAL file: header, frames, and the checksum sequence.
type refWAL struct {
	bo       binary.ByteOrder
	pageSize int
	salt     [2]uint32
	header   []byte
	seed     [2]uint32 // cumulative checksum after the WAL header
	frames   []refFrame
}

// parseRefWAL parses a WAL file, verifying the header checksum and computing
// the cumulative checksum after every frame.
func parseRefWAL(t *testing.T, b []byte) *refWAL {
	t.Helper()

	if len(b) < WALHeaderSize {
		t.Fatalf("WAL is %d bytes, shorter than the %d-byte header", len(b), WALHeaderSize)
	}
	w := &refWAL{header: append([]byte(nil), b[:WALHeaderSize]...)}

	magic := binary.BigEndian.Uint32(b[0:4])
	switch magic {
	case 0x377f0682:
		w.bo = binary.LittleEndian
	case 0x377f0683:
		w.bo = binary.BigEndian
	default:
		t.Fatalf("unexpected WAL magic %#x", magic)
	}
	w.pageSize = int(binary.BigEndian.Uint32(b[8:12]))
	w.salt = [2]uint32{binary.BigEndian.Uint32(b[16:20]), binary.BigEndian.Uint32(b[20:24])}

	stored := [2]uint32{binary.BigEndian.Uint32(b[24:28]), binary.BigEndian.Uint32(b[28:32])}
	w.seed = refChecksum(w.bo, 0, 0, b[:24])
	if w.seed != stored {
		t.Fatalf("WAL header checksum = %v, stored %v", w.seed, stored)
	}

	frameSize := WALFrameHeaderSize + w.pageSize
	cs := w.seed
	for off := WALHeaderSize; off+frameSize <= len(b); off += frameSize {
		hdr := append([]byte(nil), b[off:off+WALFrameHeaderSize]...)
		page := append([]byte(nil), b[off+WALFrameHeaderSize:off+frameSize]...)

		if got := [2]uint32{binary.BigEndian.Uint32(hdr[8:12]), binary.BigEndian.Uint32(hdr[12:16])}; got != w.salt {
			t.Fatalf("frame %d salt = %v, want %v", len(w.frames), got, w.salt)
		}

		cs = refChecksum(w.bo, cs[0], cs[1], hdr[:8])
		cs = refChecksum(w.bo, cs[0], cs[1], page)

		w.frames = append(w.frames, refFrame{
			header: hdr,
			page:   page,
			pgno:   binary.BigEndian.Uint32(hdr[0:4]),
			commit: binary.BigEndian.Uint32(hdr[4:8]),
			index:  len(w.frames),
			after:  cs,
		})
	}
	return w
}

// refCompact returns the expected compacted WAL for a scan starting at
// startFrame, computed entirely in test code: the bytes of the output file and
// the cumulative checksum after each of its frames.
//
// It follows the compaction contract: keep the last committed version of every
// page seen at or after startFrame, in file order. An open transaction at the
// end of the scan is reported as an error, matching the production behaviour.
func refCompact(t *testing.T, src *refWAL, startFrame int) ([]byte, [][2]uint32, error) {
	t.Helper()

	if startFrame < 0 || startFrame > len(src.frames) {
		t.Fatalf("start frame %d out of range for %d frames", startFrame, len(src.frames))
	}

	committed := make(map[uint32]refFrame)
	tx := make(map[uint32]refFrame)
	openTx := false
	for _, f := range src.frames[startFrame:] {
		tx[f.pgno] = f
		if f.commit == 0 {
			openTx = true
			continue
		}
		openTx = false
		for pgno, cf := range tx {
			committed[pgno] = cf
		}
		tx = make(map[uint32]refFrame)
	}
	if openTx {
		return nil, nil, ErrOpenTransaction
	}

	survivors := make([]refFrame, 0, len(committed))
	for _, f := range committed {
		survivors = append(survivors, f)
	}
	sort.Slice(survivors, func(i, j int) bool { return survivors[i].index < survivors[j].index })

	out := append([]byte(nil), src.header...)
	checksums := make([][2]uint32, 0, len(survivors))
	cs := src.seed
	for _, f := range survivors {
		hdr := make([]byte, WALFrameHeaderSize)
		binary.BigEndian.PutUint32(hdr[0:4], f.pgno)
		binary.BigEndian.PutUint32(hdr[4:8], f.commit)
		binary.BigEndian.PutUint32(hdr[8:12], src.salt[0])
		binary.BigEndian.PutUint32(hdr[12:16], src.salt[1])

		cs = refChecksum(src.bo, cs[0], cs[1], hdr[:8])
		cs = refChecksum(src.bo, cs[0], cs[1], f.page)

		binary.BigEndian.PutUint32(hdr[16:20], cs[0])
		binary.BigEndian.PutUint32(hdr[20:24], cs[1])

		out = append(out, hdr...)
		out = append(out, f.page...)
		checksums = append(checksums, cs)
	}
	return out, checksums, nil
}

// Test_CompactingFrameScanner_ChecksumsAgainstOracle compares, for a matrix of
// WAL shapes and scan start positions, the checksums the scanner derives with
// a full recalculation performed in test code:
//
//   - each frame handed out by Next(), via Frame.Checksum
//   - the final cumulative checksum, i.e. the last frame's checksum
//   - the checksums parsed back out of the serialised compacted WAL
func Test_CompactingFrameScanner_ChecksumsAgainstOracle(t *testing.T) {
	type shape struct {
		name     string
		pageSize uint32
		nFrames  int
		nPgno    int // distinct page numbers; nFrames means "no duplicates"
	}

	shapes := []shape{
		{"no-dedup", 512, 120, 120},
		{"heavy-dedup", 1024, 200, 3},
		{"mixed-dedup", 4096, 120, 40},
		{"single-page", 512, 60, 1},
		{"large-page", 65536, 8, 8},
	}

	for _, be := range []bool{false, true} {
		for _, sh := range shapes {
			frames := makeBenchFrames(sh.pageSize, sh.nFrames, sh.nPgno)
			walBytes := buildSynthWAL(be, sh.pageSize, frames)
			src := parseRefWAL(t, walBytes)

			starts := []int{0, 1, sh.nFrames / 2, sh.nFrames - 1, sh.nFrames}
			for _, start := range starts {
				name := fmt.Sprintf("%s/bigEndian=%t/start=%d", sh.name, be, start)
				t.Run(name, func(t *testing.T) {
					wantBytes, wantChecksums, err := refCompact(t, src, start)
					if err != nil {
						t.Fatalf("oracle: %v", err)
					}

					s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), int64(start), false)
					if err != nil {
						t.Fatalf("NewCompactingFrameScanner: %v", err)
					}
					if got := len(s.frames); got != len(wantChecksums) {
						t.Fatalf("scanner kept %d frames, oracle expects %d", got, len(wantChecksums))
					}

					// Per-frame and final checksum, as handed out by Next().
					var last [2]uint32
					for i, want := range wantChecksums {
						f, err := s.Next()
						if err != nil {
							t.Fatalf("Next frame %d: %v", i, err)
						}
						if !f.HasChecksum {
							t.Fatalf("frame %d has no checksum", i)
						}
						if f.Checksum != want {
							t.Fatalf("frame %d checksum = %v, oracle says %v", i, f.Checksum, want)
						}
						last = f.Checksum
					}
					if len(wantChecksums) > 0 && last != wantChecksums[len(wantChecksums)-1] {
						t.Fatalf("final checksum = %v, oracle says %v", last, wantChecksums[len(wantChecksums)-1])
					}
					if _, err := s.Next(); err != io.EOF {
						t.Fatalf("expected EOF after %d frames, got %v", len(wantChecksums), err)
					}

					// Byte-for-byte output, which carries the same checksums.
					gotBytes, err := s.Bytes()
					if err != nil {
						t.Fatalf("Bytes: %v", err)
					}
					if !bytes.Equal(gotBytes, wantBytes) {
						t.Fatalf("compacted WAL differs from oracle (%d vs %d bytes)", len(gotBytes), len(wantBytes))
					}

					// Re-parsing the output must reproduce the same checksum chain.
					out := parseRefWAL(t, gotBytes)
					if len(out.frames) != len(wantChecksums) {
						t.Fatalf("output has %d frames, want %d", len(out.frames), len(wantChecksums))
					}
					for i := range out.frames {
						if out.frames[i].after != wantChecksums[i] {
							t.Fatalf("output frame %d checksum = %v, want %v",
								i, out.frames[i].after, wantChecksums[i])
						}
					}
					if len(out.frames) > 0 && out.frames[len(out.frames)-1].after != wantChecksums[len(wantChecksums)-1] {
						t.Fatal("final checksum of the output WAL differs from the oracle")
					}
				})
			}
		}
	}
}

// Test_RefChecksumMatchesProduction validates the oracle itself against the
// production checksum function, so a mistake in refChecksum cannot silently
// make the comparison tests meaningless.
func Test_RefChecksumMatchesProduction(t *testing.T) {
	rng := rand.New(rand.NewSource(20261105))
	for _, blocks := range []int{1, 2, 3, 7, 64, 65, 512} {
		b := make([]byte, blocks*8)
		rng.Read(b)
		for _, bo := range []binary.ByteOrder{binary.LittleEndian, binary.BigEndian} {
			w0, w1, err := WALChecksum(bo, 0x13572468, 0x24681357, b)
			if err != nil {
				t.Fatal(err)
			}
			if got := refChecksum(bo, 0x13572468, 0x24681357, b); got != [2]uint32{w0, w1} {
				t.Fatalf("%d blocks: refChecksum = %v, production = (%d, %d)", blocks, got, w0, w1)
			}
		}
	}
}

// Test_CompactingFrameScanner_OpenTransactionChecksums checks that when the
// scan ends inside a transaction, no output is produced and the caller sees
// ErrOpenTransaction rather than a partially checksummed WAL.
func Test_CompactingFrameScanner_OpenTransactionChecksums(t *testing.T) {
	const pageSize = 512
	frames := makeBenchFrames(pageSize, 40, 40)
	// Drop the commit from the last frame so the WAL ends mid-transaction.
	frames[len(frames)-1].commit = 0
	walBytes := buildSynthWAL(false, pageSize, frames)

	src := parseRefWAL(t, walBytes)
	if _, _, err := refCompact(t, src, 0); err != ErrOpenTransaction {
		t.Fatalf("oracle: expected ErrOpenTransaction, got %v", err)
	}

	s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), 0, false)
	if err != ErrOpenTransaction {
		t.Fatalf("expected ErrOpenTransaction, got %v", err)
	}
	if s != nil {
		t.Fatal("expected nil scanner on open transaction")
	}
}

// Test_CompactingFrameScanner_EmptyResultChecksums checks the degenerate case
// where the scan starts past the last frame: no frames, so no checksums, and
// only the WAL header is emitted.
func Test_CompactingFrameScanner_EmptyResultChecksums(t *testing.T) {
	const pageSize = 512
	frames := makeBenchFrames(pageSize, 20, 20)
	walBytes := buildSynthWAL(false, pageSize, frames)
	src := parseRefWAL(t, walBytes)

	start := len(frames)
	wantBytes, wantChecksums, err := refCompact(t, src, start)
	if err != nil {
		t.Fatalf("oracle: %v", err)
	}
	if len(wantChecksums) != 0 {
		t.Fatalf("oracle expects %d checksums, want 0", len(wantChecksums))
	}
	if len(wantBytes) != WALHeaderSize {
		t.Fatalf("oracle output is %d bytes, want %d", len(wantBytes), WALHeaderSize)
	}

	s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), int64(start), false)
	if err != nil {
		t.Fatalf("NewCompactingFrameScanner: %v", err)
	}
	if !s.Empty() {
		t.Fatal("expected empty scanner")
	}
	got, err := s.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	if !bytes.Equal(got, wantBytes) {
		t.Fatal("empty-result output differs from oracle")
	}
}
