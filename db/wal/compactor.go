package wal

import (
	"encoding/binary"
	"io"
)

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

	chksum1, chksum2 uint32
}

// NewCompactor returns a new Compactor.
func NewCompactor(r io.ReadSeeker) *Compactor {
	return &Compactor{
		r:  r,
		wr: NewReader(r),
	}
}

// WriteTo writes the compacted WAL file to the given writer.
func (c *Compactor) WriteTo(w io.Writer) (n int64, err error) {
	frames, err := c.getFrames()
	if err != nil {
		return 0, err
	}

	if err := c.writeWALHeader(w); err != nil {
		return 0, err
	}

	// Iterate over the frames and write each one to the new WAL file.
	for _, frame := range frames {
		if err := c.writeFrame(w, frame); err != nil {
			return 0, err
		}
	}

	return n, nil
}

func (c *Compactor) writeWALHeader(w io.Writer) error {
	header := make([]byte, WALHeaderSize)

	c.putUint32(header[0:], SQLITE_WAL_MAGIC)
	c.putUint32(header[4:], SQLITE_WAL_FILE_FORMAT_VERSION)

	// Database page size
	c.putUint32(header[8:], c.wr.PageSize())

	// Checkpoint sequence number
	c.putUint32(header[12:], c.wr.seq)

	// Salt values, reusing the original salt values.
	c.putUint32(header[16:], c.wr.salt1)
	c.putUint32(header[20:], c.wr.salt2)

	// Checksum of header
	c.chksum1, c.chksum2 = WALChecksum(c.wr.bo, 0, 0, header[:24])
	c.putUint32(header[24:], c.chksum1)
	c.putUint32(header[28:], c.chksum2)

	// Write the header to the new WAL file.
	if _, err := w.Write(header); err != nil {
		return err
	}

	return nil
}

func (c *Compactor) writeFrame(w io.Writer, frame *Frame) error {
	// Seek to the frame's offset in the original WAL file.
	if _, err := c.r.Seek(frame.Offset, io.SeekStart); err != nil {
		return err
	}

	// Read the frame header and data.
	header := make([]byte, WALFrameHeaderSize)
	if _, err := io.ReadFull(c.r, header); err != nil {
		return err
	}
	data := make([]byte, c.wr.PageSize())
	if _, err := io.ReadFull(c.r, data); err != nil {
		return err
	}

	// Calculate checksums for the frame header and data.
	c.chksum1, c.chksum2 = WALChecksum(c.wr.bo, c.chksum1, c.chksum2, header[:8]) // For frame header
	c.chksum1, c.chksum2 = WALChecksum(c.wr.bo, c.chksum1, c.chksum2, data)       // For frame data

	// Update checksums in the header with the new values.
	c.putUint32(header[16:], c.chksum1)
	c.putUint32(header[20:], c.chksum2)

	// Write the frame header and data to the new WAL file.
	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}

	return nil
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

// putUint32 writes a uint32 to the given byte slice
func (c *Compactor) putUint32(b []byte, v uint32) {

	binary.BigEndian.PutUint32(b, v)
}
