package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"sort"
)

// CompactingSectionScanner implements WALIterator to iterate over compacted WAL frames
// within a byte range [start, end). It scans all frames in the range, keeps
// only the latest version of each page (respecting transaction boundaries),
// and returns them in file offset order. Frame checksums are not verified
// since the WAL file is trusted; the Writer recomputes checksums when
// producing output.
type CompactingSectionScanner struct {
	r        io.ReaderAt
	header   *WALHeader
	pageSize uint32
	start    int64
	end      int64

	frames cFrames
	fIdx   int
}

// NewCompactingSectionScanner creates a new CompactingSectionScanner that reads and compacts
// frames from the byte range [start, end) of the WAL accessible via r. The
// start and end offsets must be aligned to frame boundaries. The WAL header
// is always read from offset 0.
func NewCompactingSectionScanner(r io.ReaderAt, start, end int64) (*CompactingSectionScanner, error) {
	hdr := make([]byte, WALHeaderSize)
	if _, err := r.ReadAt(hdr, 0); err != nil {
		return nil, fmt.Errorf("read WAL header: %w", err)
	}

	magic := binary.BigEndian.Uint32(hdr[0:])
	var bo binary.ByteOrder
	switch magic {
	case 0x377f0682:
		bo = binary.LittleEndian
	case 0x377f0683:
		bo = binary.BigEndian
	default:
		return nil, fmt.Errorf("invalid wal header magic: %x", magic)
	}

	if version := binary.BigEndian.Uint32(hdr[4:]); version != WALSupportedVersion {
		return nil, fmt.Errorf("unsupported wal version: %d", version)
	}

	chksum1 := binary.BigEndian.Uint32(hdr[24:])
	chksum2 := binary.BigEndian.Uint32(hdr[28:])
	if v0, v1 := WALChecksum(bo, 0, 0, hdr[:24]); v0 != chksum1 || v1 != chksum2 {
		return nil, fmt.Errorf("WAL header checksum mismatch")
	}

	pageSize := binary.BigEndian.Uint32(hdr[8:])
	frameSize := int64(WALFrameHeaderSize) + int64(pageSize)

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
		r: r,
		header: &WALHeader{
			Magic:     magic,
			Version:   WALSupportedVersion,
			PageSize:  pageSize,
			Seq:       binary.BigEndian.Uint32(hdr[12:]),
			Salt1:     binary.BigEndian.Uint32(hdr[16:]),
			Salt2:     binary.BigEndian.Uint32(hdr[20:]),
			Checksum1: chksum1,
			Checksum2: chksum2,
		},
		pageSize: pageSize,
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
	data := make([]byte, s.pageSize)
	if _, err := s.r.ReadAt(data, cf.Offset+WALFrameHeaderSize); err != nil {
		return nil, fmt.Errorf("read frame data at offset %d: %w", cf.Offset+WALFrameHeaderSize, err)
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

// scan reads all frame headers in [start, end) and builds a compacted frame
// list, keeping only the latest version of each page. Only committed
// transactions are included.
func (s *CompactingSectionScanner) scan() error {
	if s.start >= s.end {
		return nil
	}

	frmHdr := make([]byte, WALFrameHeaderSize)
	frameSize := int64(WALFrameHeaderSize) + int64(s.pageSize)

	waitingForCommit := false
	txFrames := make(map[uint32]*cFrame)
	frames := make(map[uint32]*cFrame)

	for offset := s.start; offset < s.end; offset += frameSize {
		if _, err := s.r.ReadAt(frmHdr, offset); err != nil {
			return fmt.Errorf("read frame header at offset %d: %w", offset, err)
		}

		pgno := binary.BigEndian.Uint32(frmHdr[0:])
		commit := binary.BigEndian.Uint32(frmHdr[4:])

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
		txFrames = make(map[uint32]*cFrame)
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
