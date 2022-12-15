//go:build !windows

package history

import (
	"io"
	"os"
	"path/filepath"
)

const historyFile = ".rqlite_history"

// Reader returns a reader of the history file.
func Reader() io.ReadCloser {
	hdir, err := os.UserHomeDir()
	if err != nil {
		return nil
	}

	f, err := os.Open(filepath.Join(hdir, historyFile))
	if err != nil {
		return nil
	}
	return f
}

// Writer returns a writer for the history file.
func Writer() io.WriteCloser {
	hdir, err := os.UserHomeDir()
	if err != nil {
		return nil
	}

	f, err := os.OpenFile(filepath.Join(hdir, historyFile), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil
	}
	return f
}
