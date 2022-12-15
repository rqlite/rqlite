package history

import (
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const historyFile = "rqlite_history"

func setHidden(path string) error {
	filenameW, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}

	err = syscall.SetFileAttributes(filenameW, syscall.FILE_ATTRIBUTE_HIDDEN)
	if err != nil {
		return err
	}

	return nil
}

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

func Writer() io.WriteCloser {
	hdir, err := os.UserHomeDir()
	if err != nil {
		return nil
	}

	path := filepath.Join(hdir, historyFile)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil
	}
	setHidden(path) // best effort
	return f
}
