package history

// This file would be for linux, file_linux.go. Also create file_windows.go

import (
	"io"
	"os"
	"path/filepath"
)

func Reader() io.ReadCloser {
	file := os.Getenv("RQLITE_HISTFILE")
	if file == "" {
		file = "rqlite_history"
	}

	hdir, err := os.UserHomeDir()
	if err != nil {
		return nil
	}

	// Call OS-specific function actually
	f, err := os.Open(filepath.Join(hdir, file))
	if err != nil {
		return nil
	}
	return f
}

func Writer() io.WriteCloser {
	file := os.Getenv("RQLITE_HISTFILE")
	if file == "" {
		file = "rqlite_history"
	}

	hdir, err := os.UserHomeDir()
	if err != nil {
		return nil
	}

	// Call OS-specific function actually
	f, err := os.OpenFile(filepath.Join(hdir, file), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil
	}
	return f
}
