package rsum

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

// SHA256 calculates the SHA256 checksum of the file at the given path.
func SHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
