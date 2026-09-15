package wal

import (
	"io"
)

// FullScanner implements WALIterator to iterate over all frames in a WAL file.
type FullScanner struct {
	reader *Reader
	header *WALHeader

	// pageBuf is a scratch buffer reused across Next() calls to avoid a
	// page-sized allocation per frame. The Frame returned by Next aliases
	// this buffer in its Data field; see Next's doc comment.
	pageBuf []byte
}

// NewFullScanner creates a new FullScanner with the given io.Reader.
func NewFullScanner(r io.ReadSeeker) (*FullScanner, error) {
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
		reader:  wr,
		header:  hdr,
		pageBuf: make([]byte, wr.PageSize()),
	}, nil
}

// Header returns the header of the WAL file.
func (f *FullScanner) Header() (*WALHeader, error) {
	return f.header, nil
}

// Next reads the next frame from the WAL file.
//
// The returned Frame's Data slice is owned by the scanner and is overwritten
// on the next call to Next. Callers that retain a Frame across Next calls
// must copy Data first.
func (f *FullScanner) Next() (*Frame, error) {
	pgno, commit, err := f.reader.ReadFrame(f.pageBuf)
	if err != nil {
		return nil, err
	}

	frame := &Frame{
		Pgno:   pgno,
		Commit: commit,
		Data:   f.pageBuf,
	}
	return frame, nil
}
