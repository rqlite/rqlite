package wal

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

// BenchmarkCompaction measures a full compaction of 8 MiB of WAL with every page
// kept. Compare against the parent commit to see the effect of the change.
func BenchmarkCompaction(b *testing.B) {
	const targetBytes = 8 << 20

	for _, pageSize := range []uint32{512, 4096, 65536} {
		frameSize := int(pageSize) + WALFrameHeaderSize
		n := targetBytes / frameSize

		specs := make([][2]uint32, n)
		for i := range specs {
			specs[i] = [2]uint32{uint32(i + 1), uint32(i + 1)}
		}
		walBytes := buildWAL(false, pageSize, mkFrames(pageSize, specs...))

		b.Run(fmt.Sprintf("pageSize=%d", pageSize), func(b *testing.B) {
			b.SetBytes(int64(len(walBytes)))
			b.ReportAllocs()
			for b.Loop() {
				s, err := NewCompactingFrameScanner(bytes.NewReader(walBytes), 0, false)
				if err != nil {
					b.Fatal(err)
				}
				w, err := NewWriter(s)
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
