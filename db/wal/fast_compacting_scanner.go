package wal

import (
	"encoding/binary"
	"expvar"
	"io"
	"time"
)

// NewFastCompactingScanner creates a new CompactingFrameScanner that scans the
// entire WAL file without checksum verification.
func NewFastCompactingScanner(r io.ReadSeeker) (*CompactingFrameScanner, error) {
	fileSize, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// Peek at the header to get page size for frame count calculation.
	hdr := make([]byte, WALHeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	pageSize := int64(binary.BigEndian.Uint32(hdr[8:12]))
	frameSize := int64(WALFrameHeaderSize) + pageSize
	nFrames := (fileSize - WALHeaderSize) / frameSize

	startT := time.Now()
	s, err := NewCompactingFrameScanner(r, 0, nFrames, false)
	if err != nil {
		return nil, err
	}
	stats.Get(compactScanDuration).(*expvar.Int).Set(time.Since(startT).Milliseconds())
	stats.Get(compactLoadPageCount).(*expvar.Int).Set(int64(len(s.frames)))
	return s, nil
}
