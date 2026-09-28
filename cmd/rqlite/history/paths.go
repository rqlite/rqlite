package history

import (
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

const historyFile = ".rqlite_history"

// Path returns the full path to the history file.
func Path() (string, error) {
	hdir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(hdir, historyFile), nil
}

// Delete deletes the history file.
func Delete() error {
	p, err := Path()
	if err != nil {
		return err
	}
	return fsutil.Remove(p)
}
