package wal

import (
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"io"
	"sort"
	"time"
)

var (
	// ErrOpenTransaction is returned when the final frame in the WAL file is not a committing frame.
	ErrOpenTransaction = errors.New("open transaction at end of WAL file")
)

type cFrame struct {
	Pgno   uint32
	Commit uint32
	Offset int64
}

type cFrames []*cFrame

func (c cFrames) Len() int           { return len(c) }
func (c cFrames) Less(i, j int) bool { return c[i].Offset < c[j].Offset }
func (c cFrames) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// CompactingScanner implements WALIterator to iterate over frames in a WAL file.
// It also compacts the WAL file, with Next() returning the last valid frame for each
// page in the right order such that they can be written to a new WAL file. This Scanner
// requires that the final frame in the WAL file is a committing frame. It will return an
// error at creation time if this is not the case.
type CompactingScanner struct {
	readSeeker io.ReadSeeker
	walReader  *Reader
	header     *WALHeader
	fullScan   bool

	cIdx   int
	frames cFrames
}

// NewFastCompactingScanner creates a new CompactingScanner with the given io.ReadSeeker.
// It performs a fast scan of the WAL file, assuming that the file is valid and does not
// need to be checked.
func NewFastCompactingScanner(r io.ReadSeeker) (*CompactingScanner, error) {
	return NewCompactingScanner(r, false)
}

// NewCompactingScanner creates a new CompactingScanner with the given io.ReadSeeker.
// If fullScan is true, the scanner will perform a full scan of the WAL file, performing
// a checksum on each frame. If fullScan is false, the scanner will only scan the file
// sufficiently to find the last valid frame for each page. This is faster when the
// caller knows that the entire WAL file is valid, and will not contain pages from a
// previous checkpointing operation.
func NewCompactingScanner(r io.ReadSeeker, fullScan bool) (*CompactingScanner, error) {
	walReader := NewReader(r)
	err := walReader.ReadHeader()
	if err != nil {
		return nil, err
	}

	hdr := &WALHeader{
		Magic:     walReader.magic,
		Version:   WALSupportedVersion,
		PageSize:  walReader.PageSize(),
		Seq:       walReader.seq,
		Salt1:     walReader.salt1,
		Salt2:     walReader.salt2,
		Checksum1: walReader.chksum1,
		Checksum2: walReader.chksum2,
	}

	s := &CompactingScanner{
		readSeeker: r,
		walReader:  walReader,
		header:     hdr,
		fullScan:   fullScan,
	}
	startT := time.Now()
	if err := s.scan(); err != nil {
		return nil, err
	}
	stats.Get(compactScanDuration).(*expvar.Int).Set(time.Since(startT).Milliseconds())

	return s, nil
}

// Header returns the header of the WAL file.
func (c *CompactingScanner) Header() (*WALHeader, error) {
	return c.header, nil
}

// Next return the next logical frame from the WAL file.
func (c *CompactingScanner) Next() (*Frame, error) {
	if c.cIdx >= len(c.frames) {
		return nil, io.EOF
	}

	frame := &Frame{
		Pgno:   c.frames[c.cIdx].Pgno,
		Commit: c.frames[c.cIdx].Commit,
		Data:   make([]byte, c.header.PageSize),
	}

	if _, err := c.readSeeker.Seek(c.frames[c.cIdx].Offset+WALFrameHeaderSize, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(c.readSeeker, frame.Data); err != nil {
		return nil, err
	}
	c.cIdx++

	return frame, nil
}

// Bytes returns a byte slice containing the entire contents of the compacted WAL file.
// The byte slice is suitable for writing to a new WAL file.
func (c *CompactingScanner) Bytes() ([]byte, error) {
	startT := time.Now()
	pageSz := int(c.header.PageSize)
	buf := make([]byte, WALHeaderSize+(len(c.frames)*WALFrameHeaderSize)+len(c.frames)*pageSz)
	c.header.Copy(buf)

	var bo binary.ByteOrder
	switch magic := c.header.Magic; magic {
	case 0x377f0682:
		bo = binary.LittleEndian
	case 0x377f0683:
		bo = binary.BigEndian
	default:
		return nil, fmt.Errorf("invalid wal header magic: %x", magic)
	}

	frmHdr := WALHeaderSize
	chksum1, chksum2 := c.header.Checksum1, c.header.Checksum2
	for _, frame := range c.frames {
		frmData := frmHdr + WALFrameHeaderSize

		binary.BigEndian.PutUint32(buf[frmHdr:], frame.Pgno)
		binary.BigEndian.PutUint32(buf[frmHdr+4:], frame.Commit)
		binary.BigEndian.PutUint32(buf[frmHdr+8:], c.header.Salt1)
		binary.BigEndian.PutUint32(buf[frmHdr+12:], c.header.Salt2)

		// Checksum of frame header: "...the first 8 bytes..."
		chksum1, chksum2 = WALChecksum(bo, chksum1, chksum2, buf[frmHdr:frmHdr+8])

		// Read the frame data.
		if _, err := c.readSeeker.Seek(frame.Offset+WALFrameHeaderSize, io.SeekStart); err != nil {
			return nil, fmt.Errorf("error seeking to frame offset: %s", err)
		}
		if _, err := io.ReadFull(c.readSeeker, buf[frmData:frmData+pageSz]); err != nil {
			return nil, fmt.Errorf("error reading frame data: %s", err)
		}

		// Update checksum using frame data: "..the content of all frames up to and including the current frame."
		chksum1, chksum2 = WALChecksum(bo, chksum1, chksum2, buf[frmData:frmData+pageSz])
		binary.BigEndian.PutUint32(buf[frmHdr+16:], chksum1)
		binary.BigEndian.PutUint32(buf[frmHdr+20:], chksum2)

		frmHdr += WALFrameHeaderSize + pageSz
	}
	stats.Get(compactLoadDuration).(*expvar.Int).Set(time.Since(startT).Milliseconds())
	stats.Get(compactLoadPageCount).(*expvar.Int).Set(int64(len(c.frames)))
	return buf, nil
}

func (c *CompactingScanner) scan() error {
	waitingForCommit := false
	txFrames := make(map[uint32]*cFrame)
	frames := make(map[uint32]*cFrame)
	var buf []byte
	if c.fullScan {
		buf = make([]byte, c.header.PageSize)
	}

	for {
		pgno, commit, err := c.walReader.ReadFrame(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		frame := &cFrame{
			Pgno:   pgno,
			Commit: commit,
			Offset: c.walReader.Offset(),
		}

		// Save latest frame information for each page.
		txFrames[pgno] = frame

		// If this is not a committing frame, continue to next frame.
		if commit == 0 {
			waitingForCommit = true
			continue
		}
		waitingForCommit = false

		// At the end of each transaction, copy frame information to main map.
		for k, v := range txFrames {
			frames[k] = v
		}
		txFrames = make(map[uint32]*cFrame)
	}
	if waitingForCommit {
		return ErrOpenTransaction
	}

	// Now we have the latest version of each frame. Next we need to sort
	// them by offset so we return them in the correct order.
	c.frames = make(cFrames, 0, len(frames))
	for _, frame := range frames {
		c.frames = append(c.frames, frame)
	}
	sort.Sort(c.frames)
	return nil
}
