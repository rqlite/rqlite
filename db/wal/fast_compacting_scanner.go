package wal

import (
	"io"
)

// NewFastCompactingScanner creates a new CompactingFrameScanner that scans the
// entire WAL file without checksum verification.
func NewFastCompactingScanner(r io.ReadSeeker) (*CompactingFrameScanner, error) {
	return NewCompactingFrameScanner(r, 0, false)
}
