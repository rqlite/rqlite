package history

import (
	"io"
	"os"
)

type HiddenWriter struct {
	path string
	file *os.File
}

func NewHiddenWriter(path string) *HiddenWriter {
	return &HiddenWriter{
		path: "." + path,
	}
}

func (h *HiddenWriter) Open() error {
	var err error
	h.file, err = os.OpenFile(h.path, os.O_CREATE|os.O_WRONLY, 0644)
	return err
}

func (h *HiddenWriter) Write(p []byte) (n int, err error) {
	return h.file.Write(p)
}

func (h *HiddenWriter) Close() error {
	return h.file.Close()
}

func Reader() io.ReadCloser {
	file := os.Getenv("RQLITE_HISTFILE")
	if file == "" {
		file = "rqlite_history"
	}

	// Call OS-specific function actually
	f, err := os.Open(file)
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

	// Call OS-specific function actually
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil
	}
	return f
}
