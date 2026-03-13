package wal

import (
	"encoding/binary"
	"fmt"
	"io"
)

// SectionScanner implements WALIterator to iterate over WAL frames within a
// byte range [start, end). It reads frames directly without checksum
// verification, since the WAL file is trusted and the Writer recomputes
// checksums when producing output.
type SectionScanner struct {
	r        io.ReaderAt
	header   *WALHeader
	pageSize uint32
	start    int64
	end      int64
	offset   int64
}

// NewSectionScanner creates a new SectionScanner that reads frames from the
// byte range [start, end) of the WAL accessible via r. The start and end
// offsets must be aligned to frame boundaries. The WAL header is always read
// from offset 0.
func NewSectionScanner(r io.ReaderAt, start, end int64) (*SectionScanner, error) {
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

	return &SectionScanner{
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
		offset:   start,
	}, nil
}

// Header returns the WAL header.
func (s *SectionScanner) Header() (*WALHeader, error) {
	return s.header, nil
}

// Next returns the next frame in the section. Returns io.EOF when all frames
// in the range have been read.
func (s *SectionScanner) Next() (*Frame, error) {
	if s.offset >= s.end {
		return nil, io.EOF
	}

	frmHdr := make([]byte, WALFrameHeaderSize)
	if _, err := s.r.ReadAt(frmHdr, s.offset); err != nil {
		return nil, fmt.Errorf("read frame header at offset %d: %w", s.offset, err)
	}

	data := make([]byte, s.pageSize)
	if _, err := s.r.ReadAt(data, s.offset+WALFrameHeaderSize); err != nil {
		return nil, fmt.Errorf("read frame data at offset %d: %w", s.offset+WALFrameHeaderSize, err)
	}

	pgno := binary.BigEndian.Uint32(frmHdr[0:])
	commit := binary.BigEndian.Uint32(frmHdr[4:])

	s.offset += int64(WALFrameHeaderSize) + int64(s.pageSize)

	return &Frame{
		Pgno:   pgno,
		Commit: commit,
		Data:   data,
	}, nil
}

// Empty reports whether the scanner has zero frames to deliver.
func (s *SectionScanner) Empty() bool {
	return s.start >= s.end
}
