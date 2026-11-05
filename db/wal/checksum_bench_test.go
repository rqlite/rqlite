package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"testing"
)

// BenchmarkFrameChecksumUpdate measures the per-frame checksum work that the
// compaction path used to do against the work it does now:
//
//   - rehash-page:    WALChecksum over the frame's page, pageSize/8 iterations
//   - matrix-apply:   apply A^(1+pageSize/8) to the running state, O(1)
//
// The two sub-benchmarks do not write anything, so the numbers isolate the
// arithmetic that the optimisation removes. MB/s is frames-per-second scaled by
// pageSize, i.e. the WAL throughput this step alone could sustain.
func BenchmarkFrameChecksumUpdate(b *testing.B) {
	rng := rand.New(rand.NewSource(1))

	for _, pageSize := range []uint32{512, 1024, 4096, 8192, 65536} {
		page := make([]byte, pageSize)
		rng.Read(page)

		const s0, s1 = uint32(0x12345678), uint32(0x9abcdef0)
		m := checksumMatrixPow(1 + uint64(pageSize)/8)

		b.Run(fmt.Sprintf("pageSize=%d/rehash-page", pageSize), func(b *testing.B) {
			var out [2]uint32
			b.SetBytes(int64(pageSize))
			b.ReportAllocs()
			for b.Loop() {
				c0, c1, err := WALChecksum(binary.LittleEndian, s0, s1, page)
				if err != nil {
					b.Fatal(err)
				}
				out = [2]uint32{c0, c1}
			}
			_ = out
		})

		b.Run(fmt.Sprintf("pageSize=%d/matrix-apply", pageSize), func(b *testing.B) {
			var out [2]uint32
			b.SetBytes(int64(pageSize))
			b.ReportAllocs()
			for b.Loop() {
				out = m.apply(s0, s1)
			}
			_ = out
		})
	}
}

// BenchmarkCompactingFrameScanner_PageSizes compares the full compaction
// pipeline with derived checksums against the same pipeline rehashing every
// page, across page sizes and scan start positions. Each case holds the total
// WAL size near 8 MiB so MB/s is comparable between page sizes.
//
// "fresh" starts at frame zero and takes its seed from the WAL header;
// "resumed" starts mid-WAL and reads the preceding frame header for its seed.
func BenchmarkCompactingFrameScanner_PageSizes(b *testing.B) {
	const targetBytes = 8 << 20

	for _, pageSize := range []uint32{512, 1024, 4096, 8192, 65536} {
		frameSize := int(pageSize) + WALFrameHeaderSize
		nFrames := targetBytes / frameSize
		// Every page distinct, so every frame survives: the worst case for
		// re-checksumming, and the case where the derivation earns the most.
		frames := makeBenchFrames(pageSize, nFrames, nFrames)
		walBytes := buildSynthWAL(false, pageSize, frames)

		for _, sp := range []struct {
			name  string
			start int64
		}{
			{"fresh", 0},
			{"resumed", 1},
		} {
			b.Run(fmt.Sprintf("pageSize=%d/%s", pageSize, sp.name), func(b *testing.B) {
				benchPipeline(b, walBytes, sp.start)
			})
		}
	}
}

// benchPipeline measures one compaction pass for both checksum strategies.
func benchPipeline(b *testing.B, walBytes []byte, start int64) {
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
			for b.Loop() {
				s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), start, false)
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
