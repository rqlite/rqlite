package wal

import (
	"errors"
	"expvar"
	"io"
	"time"
)

var (
	// ErrOpenTransaction is returned when the final frame in the WAL file is not a committing frame.
	ErrOpenTransaction = errors.New("open transaction at end of WAL file")
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
