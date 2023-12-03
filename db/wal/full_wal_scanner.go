package wal

import (
	"io"
)

// Frame represents a single frame in the WAL file.
type Frame struct {
	Pgno   uint32 // Page number
	Commit uint32 // Commit flag
	Data   []byte // Frame data
}

// WALIterator defines the interface for WAL frame iteration.
type WALIterator interface {
	Next() (*Frame, error)
}

// FullWALScanner implements WALIterator to iterate over all frames in a WAL file.
type FullWALScanner struct {
	reader *Reader
}

// NewFullWALScanner creates a new FullWALScanner with the given io.Reader.
func NewFullWALScanner(r io.Reader) (*FullWALScanner, error) {
	wr := NewReader(r)
	err := wr.ReadHeader()
	if err != nil {
		return nil, err
	}
	return &FullWALScanner{
		reader: wr,
	}, nil
}

// Next reads the next frame from the WAL file.
func (f *FullWALScanner) Next() (*Frame, error) {
	data := make([]byte, f.reader.PageSize())
	pgno, commit, err := f.reader.ReadFrame(data)
	if err != nil {
		return nil, err
	}

	frame := &Frame{
		Pgno:   pgno,
		Commit: commit,
		Data:   data,
	}
	return frame, nil
}
