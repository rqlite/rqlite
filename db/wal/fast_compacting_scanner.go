package wal

import (
	"expvar"
	"io"
	"time"
)

// NewFastCompactingScanner creates a new CompactingFrameScanner that scans the
// entire WAL file without checksum verification.
func NewFastCompactingScanner(r io.ReadSeeker) (*CompactingFrameScanner, error) {
	startT := time.Now()
	s, err := NewCompactingFrameScanner(r, 0, false)
	if err != nil {
		return nil, err
	}
	stats.Get(compactScanDuration).(*expvar.Int).Set(time.Since(startT).Milliseconds())
	stats.Get(compactLoadPageCount).(*expvar.Int).Set(int64(len(s.frames)))
	return s, nil
}
