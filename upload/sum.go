package upload

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

type SHA256Sum []byte

func (s SHA256Sum) String() string {
	return hex.EncodeToString(s)
}

func (s SHA256Sum) Equals(other SHA256Sum) bool {
	return bytes.Equal(s, other)
}

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
