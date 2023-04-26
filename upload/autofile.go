package upload

import (
	"os"
)

// AutoDeleteFile is a wrapper around os.File that deletes the file when it is
// closed.
type AutoDeleteFile struct {
	*os.File
}

// Close implements the io.Closer interface
func (f *AutoDeleteFile) Close() error {
	if err := f.File.Close(); err != nil {
		return err
	}
	return os.Remove(f.Name())
}

// NewAutoDeleteFile takes a filename and wraps it in an AutoDeleteFile
func NewAutoDeleteFile(path string) (*AutoDeleteFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &AutoDeleteFile{f}, nil
}
