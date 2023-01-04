package history

import (
	"os"
	"path/filepath"
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

// Delete performs a best-effort removal of the history file.
func Delete() {
	p, err := Path()
	if err != nil {
		return
	}
	os.Remove(p)
}
