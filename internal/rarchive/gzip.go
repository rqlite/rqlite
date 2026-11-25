package rarchive

import (
	"compress/gzip"
	"io"
	"os"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// Gzip compresses the given file and returns the path to the
// compressed file, which is created in dir. It is the caller's responsibility to remove
// the compressed file when it is no longer needed.
func Gzip(file, dir string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()

	tmpFd, err := os.CreateTemp(dir, "gzip")
	if err != nil {
		return "", err
	}
	defer tmpFd.Close()

	gz := gzip.NewWriter(tmpFd)
	_, err = io.Copy(gz, f)
	if err != nil {
		fsutil.Remove(tmpFd.Name())
		return "", err
	}

	err = gz.Close()
	if err != nil {
		fsutil.Remove(tmpFd.Name())
		return "", err
	}
	return tmpFd.Name(), nil
}

// Gunzip decompresses the given GZIP file and returns the
// path to the decompressed file, which is created in dir. It is the caller's responsibility
// to remove the decompressed file when it is no longer needed.
func Gunzip(file, dir string) (string, error) {
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

	tmpFd, err := os.CreateTemp(dir, "gunzip")
	if err != nil {
		return "", err
	}
	defer tmpFd.Close()

	_, err = io.Copy(tmpFd, gz)
	if err != nil {
		fsutil.Remove(tmpFd.Name())
		return "", err
	}
	return tmpFd.Name(), nil
}
