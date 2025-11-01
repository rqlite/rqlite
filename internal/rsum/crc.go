package rsum

import (
	"hash/crc32"
	"io"
	"os"
	"time"
)

// CRC32 calculates the CRC32 checksum of the file at the given path.
func CRC32(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := crc32.NewIEEE()
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
