//go:build !windows

package history

import (
	"io"
	"os"
	"path/filepath"
)

const historyFile = ".rqlite_history"

// Path returns the full path to the history file.
func Path() (string, error) {
	hdir, err := os.UserHomeDir()
	if err != nil {
		return "", nil
	}
	return filepath.Join(hdir, historyFile), nil
}

// Reader returns a reader of the history file.
func Reader() io.ReadCloser {
	p, err := Path()
	if err != nil {
		return nil
	}

	f, err := os.Open(p)
	if err != nil {
		return nil
	}
	return f
}

// Writer returns a writer for the history file.
func Writer() io.WriteCloser {
	p, err := Path()
	if err != nil {
		return nil
	}

	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil
	}
	return f
}

// Delete performs a best-effort removal of the history file.
func Delete() {
	p, err := Path()
	if err != nil {
		return
	}
	os.Remove(p)
}
