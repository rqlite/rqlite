package rarchive

import (
	"compress/gzip"
	"io"
	"os"
)

// Gzip compresses the given file and returns the path to the
// compressed file. It is the caller's responsibility to remove
// the compressed file when it is no longer needed.
func Gzip(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()

	tmpFd, err := os.CreateTemp("", "gzip")
	if err != nil {
		return "", err
	}

	gz := gzip.NewWriter(tmpFd)
	_, err = io.Copy(gz, f)
	if err != nil {
		return "", err
	}

	err = gz.Close()
	if err != nil {
		return "", err
	}
	return tmpFd.Name(), nil
}

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
