package rsum

import (
	"hash/crc32"
	"io"
	"os"
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
