package rsum

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"time"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// SyncState represents whether a file should be synced after writing.
type SyncState bool

var (
	// Sync indicates that the file should be synced after writing.
	Sync SyncState = true

	// NoSync indicates that the file should not be synced after writing.
	NoSync SyncState = false
)

// CRC32 calculates the CRC32 checksum of the file at the given path.
func CRC32(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := crc32.New(castagnoliTable)
	if _, err := io.Copy(h, f); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

// CRC32WithTiming calculates the CRC32 checksum of the file at the given path
// and also returns the duration taken to compute it.
func CRC32WithTiming(path string) (uint32, time.Duration, error) {
	startT := time.Now()
	sum, err := CRC32(path)
	if err != nil {
		return 0, 0, err
	}
	return sum, time.Since(startT), nil
}

// CRC32Writer wraps an io.Writer, computing a running CRC32 (Castagnoli)
// checksum over all data written through it.
type CRC32Writer struct {
	h  hash.Hash32
	mw io.Writer
}

// NewCRC32Writer creates a new CRC32Writer that writes data to w while
// computing a running CRC32 checksum.
func NewCRC32Writer(w io.Writer) *CRC32Writer {
	h := crc32.New(castagnoliTable)
	return &CRC32Writer{
		h:  h,
		mw: io.MultiWriter(w, h),
	}
}

// Write writes p to the underlying writer and updates the CRC32 checksum.
func (c *CRC32Writer) Write(p []byte) (int, error) {
	return c.mw.Write(p)
}

// Sum32 returns the CRC32 checksum of all data written so far.
func (c *CRC32Writer) Sum32() uint32 {
	return c.h.Sum32()
}

// CRC32WriteCloser wraps an io.WriteCloser, computing a running CRC32 (Castagnoli)
// checksum over all data written through it, and also writing the final checksum
// to a separate io.WriteCloser when closed.
type CRC32WriteCloser struct {
	w    io.WriteCloser
	sumW io.WriteCloser
	crcW *CRC32Writer
}

// NewCRC32WriteCloser creates a new CRC32WriteCloser that writes data to w while
// computing a running CRC32 checksum. The underlying writer must also be an
// io.Closer.
func NewCRC32WriteCloser(w io.WriteCloser, sumW io.WriteCloser) *CRC32WriteCloser {
	return &CRC32WriteCloser{
		w:    w,
		sumW: sumW,
		crcW: NewCRC32Writer(w),
	}
}

// Write writes p to the underlying writer and updates the CRC32 checksum.
func (c *CRC32WriteCloser) Write(p []byte) (int, error) {
	return c.crcW.Write(p)
}

// Close closes the underlying writer and the sum writer.
func (c *CRC32WriteCloser) Close() error {
	sum := c.crcW.Sum32()
	if err := WriteCRC32Sum(c.sumW, sum); err != nil {
		return err
	}
	if err := c.sumW.Close(); err != nil {
		return err
	}
	return c.w.Close()
}

// WriteCRC32Sum writes the given CRC32 checksum to w as an 8-character lowercase
// hex string (e.g. "1a2b3c4d").
func WriteCRC32Sum(w io.Writer, sum uint32) error {
	_, err := fmt.Fprintf(w, "%08x", sum)
	return err
}

// ReadCRC32Sum reads a CRC32 checksum from r, expecting it to be an 8-character
// lowercase hex string (e.g. "1a2b3c4d").
func ReadCRC32Sum(r io.Reader) (uint32, error) {
	var sum uint32
	if _, err := fmt.Fscanf(r, "%08x", &sum); err != nil {
		return 0, fmt.Errorf("invalid checksum: %w", err)
	}
	return sum, nil
}

// WriteCRC32SumFile writes the given CRC32 checksum to path as an 8-character
// lowercase hex string (e.g. "1a2b3c4d").
func WriteCRC32SumFile(path string, sum uint32, sync SyncState) error {
	fd, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fd.Close()

	_, err = fmt.Fprintf(fd, "%08x", sum)
	if err != nil {
		return err
	}
	if sync {
		if err := fd.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// ReadCRC32SumFile reads a CRC32 checksum previously written by WriteCRC32SumFile.
func ReadCRC32SumFile(path string) (uint32, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var sum uint32
	if _, err := fmt.Sscanf(string(b), "%08x", &sum); err != nil {
		return 0, fmt.Errorf("invalid checksum file: %w", err)
	}
	return sum, nil
}

// CompareCRC32SumFile calculates the CRC32 checksum of the file at dataPath and
// compares it to the expected checksum read from crcPath. Returns true if they
// match, false if they don't, or an error if there was a problem reading the
// files or calculating the checksum.
func CompareCRC32SumFile(dataPath, crcPath string) (bool, error) {
	expectedSum, err := ReadCRC32SumFile(crcPath)
	if err != nil {
		return false, fmt.Errorf("reading CRC32 sum file: %w", err)
	}
	actualSum, err := CRC32(dataPath)
	if err != nil {
		return false, fmt.Errorf("calculating CRC32 of data file: %w", err)
	}
	return expectedSum == actualSum, nil
}
