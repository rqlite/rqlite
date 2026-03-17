package wal

import (
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
	readSeeker io.ReadSeeker
	walReader  *Reader
	header     *WALHeader
	start      int64
	end        int64

	frames cFrames
	fIdx   int
}

// NewCompactingSectionScanner creates a new CompactingSectionScanner that reads and compacts
// frames from the byte range [start, end) of the WAL accessible via r. The
// start and end offsets must be aligned to frame boundaries. The WAL header
// is always read from offset 0.
func NewCompactingSectionScanner(r io.ReadSeeker, start, end int64) (*CompactingSectionScanner, error) {
	walReader := NewReader(r)
	if err := walReader.ReadHeader(); err != nil {
		return nil, err
	}

	pageSize := walReader.pageSize
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
		start: start,
		end:   end,
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

// scan reads all frame headers in [start, end) and builds a compacted frame
// list, keeping only the latest version of each page. Only committed
// transactions are included.
func (s *CompactingSectionScanner) scan() error {
	if s.start >= s.end {
		return nil
	}

	// Seek to the start of the section.
	if _, err := s.readSeeker.Seek(s.start, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start offset %d: %w", s.start, err)
	}

	waitingForCommit := false
	txFrames := make(map[uint32]*cFrame)
	frames := make(map[uint32]*cFrame)
	frameSize := int64(WALFrameHeaderSize) + int64(s.header.PageSize)

	for offset := s.start; offset < s.end; offset += frameSize {
		pgno, commit, err := s.walReader.ReadFrame(nil)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
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
