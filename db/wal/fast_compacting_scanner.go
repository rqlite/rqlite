package wal

import (
	"expvar"
	"io"
	"time"
)

// NewFastCompactingScanner creates a new CompactingSectionScanner that scans the
// entire WAL file without checksum verification.
func NewFastCompactingScanner(r io.ReadSeeker) (*CompactingSectionScanner, error) {
	end, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	startT := time.Now()
	s, err := NewCompactingSectionScanner(r, WALHeaderSize, end, false)
	if err != nil {
		return nil, err
	}
	stats.Get(compactScanDuration).(*expvar.Int).Set(time.Since(startT).Milliseconds())
	stats.Get(compactLoadPageCount).(*expvar.Int).Set(int64(len(s.frames)))
	return s, nil
}
