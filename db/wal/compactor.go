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

	// File format write version (1 byte) and read version (1 byte).
	// Assuming values for SQLite version 3.7.0 or later.
	header[4] = 0x02 // Write version
	header[5] = 0x02 // Read version

	// Database page size (2 bytes).
	c.putUint16(header[6:], uint16(c.wr.PageSize()))

	// Checkpoint sequence number (4 bytes).
	// Incrementing from the original sequence number.
	c.putUint32(header[12:], c.wr.seq+1)

	// Salt values (4 bytes each), reusing the original salt values.
	c.putUint32(header[16:], c.wr.salt1)
	c.putUint32(header[20:], c.wr.salt2)

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

	// Recalculate checksums.
	chksum1, chksum2 := WALChecksum(c.wr.bo, c.wr.salt1, c.wr.salt2, header)
	chksum1, chksum2 = WALChecksum(c.wr.bo, chksum1, chksum2, data)

	// Update checksums in the header.
	c.putUint32(header[16:], chksum1)
	c.putUint32(header[20:], chksum2)

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

// putUint16 writes a uint16 to the given byte slice in the same byte order as
// as the source WAL file.
func (c *Compactor) putUint16(b []byte, v uint16) {
	if c.wr.bo == binary.LittleEndian {
		binary.LittleEndian.PutUint16(b, v)
	} else {
		binary.BigEndian.PutUint16(b, v)
	}
}

// putUint32 writes a uint32 to the given byte slice in the same byte order as
// as the source WAL file.
func (c *Compactor) putUint32(b []byte, v uint32) {
	if c.wr.bo == binary.LittleEndian {
		binary.LittleEndian.PutUint32(b, v)
	} else {
		binary.BigEndian.PutUint32(b, v)
	}
}
