package wal

import "io"

// Frame points to a single WAL frame in a WAL file.
type Frame struct {
	Pgno   uint32
	Commit uint32
	Offset int64
}

// Compactor is used to compact a WAL file.
type Compactor struct {
	r  io.ReadSeeker
	wr *Reader
}

// NewCompactor returns a new Compactor.
func NewCompactor(r io.ReadSeeker) *Compactor {
	return &Compactor{
		r:  r,
		wr: NewReader(r),
	}
}

// WriteTo compacts the WAL file to the given writer.
func (c *Compactor) WriteTo(w io.Writer) (n int64, err error) {
	frames, err := c.getFrames()
	if err != nil {
		return 0, err
	}
	if len(frames) == 0 {
		return 0, nil
	}

	// Copy the WAL header.
	if _, err := c.r.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if _, err := io.CopyN(w, c.r, WALHeaderSize); err != nil {
		return 0, err
	}

	// Write each new WAL Frame header, and the associated page data.
	for _, f := range frames {
		_ = f
	}
	return 0, nil
}

func (c *Compactor) getFrames() (map[uint32]*Frame, error) {
	if err := c.wr.ReadHeader(); err != io.EOF {
		return nil, nil
	}

	// Read the offset of the last version of each page in the WAL.
	frames := make(map[uint32]*Frame)
	txFrames := make(map[uint32]*Frame)
	buf := make([]byte, c.wr.PageSize())
	for {
		pgno, commit, err := c.wr.ReadFrame(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		frame := &Frame{
			Pgno:   pgno,
			Commit: commit,
			Offset: c.wr.Offset(),
		}

		// Save latest frame information for each page.
		txFrames[pgno] = frame

		// If this is not a committing frame, continue to next frame.
		if commit == 0 {
			continue
		}

		// At the end of each transaction, copy frame information to main map.
		for k, v := range txFrames {
			frames[k] = v
		}
		txFrames = make(map[uint32]*Frame)
	}

	return frames, nil
}
