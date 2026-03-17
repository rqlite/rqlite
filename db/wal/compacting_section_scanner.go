package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"sort"
)

// CompactingSectionScanner implements WALIterator to iterate over compacted WAL
// frames within a byte range [start, end). It scans all frames in the range, keeps
// only the latest version of each page (respecting transaction boundaries), and
// returns them in file offset order. If fullScan is false, frame checksums are
// not verified since the WAL file is trusted.
type CompactingSectionScanner struct {
	readSeeker io.ReadSeeker
	walReader  *Reader
	header     *WALHeader
	fullScan   bool
	start      int64
	end        int64

	frames cFrames
	fIdx   int
}

// NewCompactingSectionScanner creates a new CompactingSectionScanner that reads
// and compacts frames from the byte range [start, end) of the WAL accessible via
// r. The start and end offsets must be aligned to frame boundaries. The WAL header
// is always read from offset 0. If fullScan is true, the scanner will perform
// a checksum on each frame. If fullScan is false, the scanner will only scan
// the file sufficiently to find the last valid frame for each page.
//
// Scanning stops at the first invalid frame (e.g. salt mismatch, checksum
// failure, or partial read), even if the end offset has not been reached.
func NewCompactingSectionScanner(r io.ReadSeeker, start, end int64, fullScan bool) (*CompactingSectionScanner, error) {
	walReader := NewReader(r)
	if err := walReader.ReadHeader(); err != nil {
		return nil, err
	}

	pageSize := walReader.pageSize
	frameSize := int64(WALFrameHeaderSize) + int64(pageSize)

	if fullScan && start != WALHeaderSize {
		return nil, fmt.Errorf("fullScan requires start offset to be %d, got %d", WALHeaderSize, start)
	}
	if start > end {
		return nil, fmt.Errorf("start offset (%d) is past end offset (%d)", start, end)
	}
	if start != end {
		if (start-WALHeaderSize)%frameSize != 0 {
			return nil, fmt.Errorf("start offset %d is not frame-aligned", start)
		}
		if (end-WALHeaderSize)%frameSize != 0 {
			return nil, fmt.Errorf("end offset %d is not frame-aligned", end)
		}
	}

	s := &CompactingSectionScanner{
		readSeeker: r,
		walReader:  walReader,
		header: &WALHeader{
			Magic:     walReader.magic,
			Version:   WALSupportedVersion,
			PageSize:  pageSize,
			Seq:       walReader.seq,
			Salt1:     walReader.salt1,
			Salt2:     walReader.salt2,
			Checksum1: walReader.chksum1,
			Checksum2: walReader.chksum2,
		},
		fullScan: fullScan,
		start:    start,
		end:      end,
	}

	if err := s.scan(); err != nil {
		return nil, err
	}
	return s, nil
}

// Header returns the WAL header.
func (s *CompactingSectionScanner) Header() (*WALHeader, error) {
	return s.header, nil
}

// Next returns the next compacted frame. Returns io.EOF when all frames
// have been read.
func (s *CompactingSectionScanner) Next() (*Frame, error) {
	if s.fIdx >= len(s.frames) {
		return nil, io.EOF
	}

	cf := s.frames[s.fIdx]
	data := make([]byte, s.header.PageSize)
	if _, err := s.readSeeker.Seek(cf.Offset+WALFrameHeaderSize, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(s.readSeeker, data); err != nil {
		return nil, err
	}
	s.fIdx++

	return &Frame{
		Pgno:   cf.Pgno,
		Commit: cf.Commit,
		Data:   data,
	}, nil
}

// Empty reports whether the scanner has zero frames to deliver.
func (s *CompactingSectionScanner) Empty() bool {
	return len(s.frames) == 0
}

// Bytes returns a byte slice containing the entire contents of the compacted WAL file.
// The byte slice is suitable for writing to a new WAL file. For large WAL files, this
// may consume a lot of memory, so it should be used with care. It's mostly intended for
// use in testing, where the WAL files are small.
func (s *CompactingSectionScanner) Bytes() ([]byte, error) {
	pageSz := int(s.header.PageSize)
	buf := make([]byte, WALHeaderSize+(len(s.frames)*WALFrameHeaderSize)+len(s.frames)*pageSz)
	s.header.Copy(buf)

	var bo binary.ByteOrder
	switch magic := s.header.Magic; magic {
	case 0x377f0682:
		bo = binary.LittleEndian
	case 0x377f0683:
		bo = binary.BigEndian
	default:
		return nil, fmt.Errorf("invalid wal header magic: %x", magic)
	}

	frmHdr := WALHeaderSize
	chksum1, chksum2 := s.header.Checksum1, s.header.Checksum2
	for _, frame := range s.frames {
		frmData := frmHdr + WALFrameHeaderSize

		binary.BigEndian.PutUint32(buf[frmHdr:], frame.Pgno)
		binary.BigEndian.PutUint32(buf[frmHdr+4:], frame.Commit)
		binary.BigEndian.PutUint32(buf[frmHdr+8:], s.header.Salt1)
		binary.BigEndian.PutUint32(buf[frmHdr+12:], s.header.Salt2)

		// Checksum of frame header: "...the first 8 bytes..."
		chksum1, chksum2 = WALChecksum(bo, chksum1, chksum2, buf[frmHdr:frmHdr+8])

		// Read the frame data.
		if _, err := s.readSeeker.Seek(frame.Offset+WALFrameHeaderSize, io.SeekStart); err != nil {
			return nil, fmt.Errorf("error seeking to frame offset: %s", err)
		}
		if _, err := io.ReadFull(s.readSeeker, buf[frmData:frmData+pageSz]); err != nil {
			return nil, fmt.Errorf("error reading frame data: %s", err)
		}

		// Update checksum using frame data.
		chksum1, chksum2 = WALChecksum(bo, chksum1, chksum2, buf[frmData:frmData+pageSz])
		binary.BigEndian.PutUint32(buf[frmHdr+16:], chksum1)
		binary.BigEndian.PutUint32(buf[frmHdr+20:], chksum2)

		frmHdr += WALFrameHeaderSize + pageSz
	}
	return buf, nil
}

// scan reads all frame headers in [start, end) and builds a compacted frame
// list, keeping only the latest version of each page. Only committed transactions
// are included.
func (s *CompactingSectionScanner) scan() error {
	if s.start >= s.end {
		return nil
	}

	// Seek to the start of the section.
	if _, err := s.readSeeker.Seek(s.start, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start offset %d: %w", s.start, err)
	}

	var buf []byte
	if s.fullScan {
		buf = make([]byte, s.header.PageSize)
	}

	waitingForCommit := false
	txFrames := make(map[uint32]*cFrame)
	frames := make(map[uint32]*cFrame)

	for {
		pgno, commit, err := s.walReader.ReadFrame(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		offset := s.start + s.walReader.Offset() - WALHeaderSize
		if offset >= s.end {
			break
		}

		txFrames[pgno] = &cFrame{
			Pgno:   pgno,
			Commit: commit,
			Offset: offset,
		}

		if commit == 0 {
			waitingForCommit = true
			continue
		}
		waitingForCommit = false

		maps.Copy(frames, txFrames)
		clear(txFrames)
	}

	if waitingForCommit {
		return ErrOpenTransaction
	}

	s.frames = make(cFrames, 0, len(frames))
	for _, frame := range frames {
		s.frames = append(s.frames, frame)
	}
	sort.Sort(s.frames)
	return nil
}

type cFrame struct {
	Pgno   uint32
	Commit uint32
	Offset int64
}

type cFrames []*cFrame

func (c cFrames) Len() int           { return len(c) }
func (c cFrames) Less(i, j int) bool { return c[i].Offset < c[j].Offset }
func (c cFrames) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
