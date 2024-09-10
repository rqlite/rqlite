package rsum

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
)

// MD5 calculates the MD5 checksum of the file at the given path.
func MD5(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
