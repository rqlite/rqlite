package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

// SHA256Sum is a SHA256 hash.
type SHA256Sum []byte

// String returns the hex-encoded string representation of the SHA256Sum.
func (s SHA256Sum) String() string {
	return hex.EncodeToString(s)
}

// Equals returns true if the SHA256Sum is equal to the other.
func (s SHA256Sum) Equals(other SHA256Sum) bool {
	return bytes.Equal(s, other)
}

// FileSHA256 returns the SHA256 hash of the file at the given path.
func FileSHA256(filePath string) (SHA256Sum, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return nil, err
	}

	hash := hasher.Sum(nil)
	return SHA256Sum(hash), nil
}
