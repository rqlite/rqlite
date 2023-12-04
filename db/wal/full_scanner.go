package wal

import (
	"io"
)

// FullScanner implements WALIterator to iterate over all frames in a WAL file.
type FullScanner struct {
	reader *Reader
	header *WALHeader
}

// NewFullScanner creates a new FullScanner with the given io.Reader.
func NewFullScanner(r io.Reader) (*FullScanner, error) {
	wr := NewReader(r)
	err := wr.ReadHeader()
	if err != nil {
		return nil, err
	}

	hdr := &WALHeader{
		Magic:     wr.magic,
		Version:   WALSupportedVersion,
		PageSize:  wr.PageSize(),
		Seq:       wr.seq,
		Salt1:     wr.salt1,
		Salt2:     wr.salt2,
		Checksum1: wr.chksum1,
		Checksum2: wr.chksum2,
	}

	return &FullScanner{
		reader: wr,
		header: hdr,
	}, nil
}

// Header returns the header of the WAL file.
func (f *FullScanner) Header() (*WALHeader, error) {
	return f.header, nil
}

// Next reads the next frame from the WAL file.
func (f *FullScanner) Next() (*Frame, error) {
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
