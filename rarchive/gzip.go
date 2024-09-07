package rarchive

import (
	"compress/gzip"
	"io"
	"os"
)

// Gunzip decompresses the given GZIP file and returns the
// path to the decompressed file. It is the caller's responsibility
// to remove the decompressed file when it is no longer needed.
func Gunzip(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gz.Close()

	tmpFd, err := os.CreateTemp("", "gunzip")
	if err != nil {
		return "", err
	}
	defer tmpFd.Close()

	_, err = io.Copy(tmpFd, gz)
	if err != nil {
		return "", err
	}
	return tmpFd.Name(), nil
}
