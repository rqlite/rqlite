//go:build !windows

package history

import (
	"io"
	"os"
)

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
