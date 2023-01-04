package history

import (
	"io"
	"os"
	"syscall"
)

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
	setHidden(p) // best effort
	return f
}
