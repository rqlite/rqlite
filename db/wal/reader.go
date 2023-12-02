package wal

import (
	"encoding/binary"
	"fmt"
	"io"
)

// SQLite constants
const (
	WALHeaderSize      = 32
	WALFrameHeaderSize = 24
)

// Reader wraps an io.Reader and parses SQLite WAL frames.
//
// This reader verifies the salt & checksum integrity while it reads. It does
// not enforce transaction boundaries (i.e. it may return uncommitted frames).
// It is the responsibility of the caller to handle this.
//
// Reader has been copied from the litefs source code. Many thanks to the
// authors of that software. See https://github.com/superfly/litefs for more
// details.
type Reader struct {
	r      io.Reader
	frameN int

	bo       binary.ByteOrder
	pageSize uint32
	seq      uint32

	salt1, salt2     uint32
	chksum1, chksum2 uint32
}

// NewReader returns a new instance of Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

// PageSize returns the page size from the header. Must call ReadHeader() first.
func (r *Reader) PageSize() uint32 { return r.pageSize }

// Offset returns the file offset of the last read frame.
// Returns zero if no frames have been read.
func (r *Reader) Offset() int64 {
	if r.frameN == 0 {
		return 0
	}
	return WALHeaderSize + ((int64(r.frameN) - 1) * (WALFrameHeaderSize + int64(r.pageSize)))
}

// ReadHeader reads the WAL header into the reader. Returns io.EOF if WAL is invalid.
func (r *Reader) ReadHeader() error {
	// If we have a partial WAL, then mark WAL as done.
	hdr := make([]byte, WALHeaderSize)
	if _, err := io.ReadFull(r.r, hdr); err == io.ErrUnexpectedEOF {
		return io.EOF
	} else if err != nil {
		return err
	}

	// Determine byte order of checksums.
	switch magic := binary.BigEndian.Uint32(hdr[0:]); magic {
	case 0x377f0682:
		r.bo = binary.LittleEndian
	case 0x377f0683:
		r.bo = binary.BigEndian
	default:
		return fmt.Errorf("invalid wal header magic: %x", magic)
	}

	// If the header checksum doesn't match then we may have failed with
	// a partial WAL header write during checkpointing.
	chksum1 := binary.BigEndian.Uint32(hdr[24:])
	chksum2 := binary.BigEndian.Uint32(hdr[28:])
	if v0, v1 := WALChecksum(r.bo, 0, 0, hdr[:24]); v0 != chksum1 || v1 != chksum2 {
		return io.EOF
	}

	// Verify version is correct.
	if version := binary.BigEndian.Uint32(hdr[4:]); version != 3007000 {
		return fmt.Errorf("unsupported wal version: %d", version)
	}

	r.pageSize = binary.BigEndian.Uint32(hdr[8:])
	r.seq = binary.BigEndian.Uint32(hdr[12:])
	r.salt1 = binary.BigEndian.Uint32(hdr[16:])
	r.salt2 = binary.BigEndian.Uint32(hdr[20:])
	r.chksum1, r.chksum2 = chksum1, chksum2

	return nil
}

// ReadFrame reads the next frame from the WAL and returns the page number.
// Returns io.EOF at the end of the valid WAL.
func (r *Reader) ReadFrame(data []byte) (pgno, commit uint32, err error) {
	if len(data) != int(r.pageSize) {
		return 0, 0, fmt.Errorf("WALReader.ReadFrame(): buffer size (%d) must match page size (%d)", len(data), r.pageSize)
	}

	// Read WAL frame header.
	hdr := make([]byte, WALFrameHeaderSize)
	if _, err := io.ReadFull(r.r, hdr); err == io.ErrUnexpectedEOF {
		return 0, 0, io.EOF
	} else if err != nil {
		return 0, 0, err
	}

	// Read WAL page data.
	if _, err := io.ReadFull(r.r, data); err == io.ErrUnexpectedEOF {
		return 0, 0, io.EOF
	} else if err != nil {
		return 0, 0, err
	}

	// Verify salt matches the salt in the header.
	salt1 := binary.BigEndian.Uint32(hdr[8:])
	salt2 := binary.BigEndian.Uint32(hdr[12:])
	if r.salt1 != salt1 || r.salt2 != salt2 {
		return 0, 0, io.EOF
	}

	// Verify the checksum is valid.
	chksum1 := binary.BigEndian.Uint32(hdr[16:])
	chksum2 := binary.BigEndian.Uint32(hdr[20:])
	r.chksum1, r.chksum2 = WALChecksum(r.bo, r.chksum1, r.chksum2, hdr[:8]) // frame header
	r.chksum1, r.chksum2 = WALChecksum(r.bo, r.chksum1, r.chksum2, data)    // frame data
	if r.chksum1 != chksum1 || r.chksum2 != chksum2 {
		return 0, 0, io.EOF
	}

	pgno = binary.BigEndian.Uint32(hdr[0:])
	commit = binary.BigEndian.Uint32(hdr[4:])

	r.frameN++

	return pgno, commit, nil
}

// WALChecksum computes a running SQLite WAL checksum over a byte slice.
func WALChecksum(bo binary.ByteOrder, s0, s1 uint32, b []byte) (uint32, uint32) {
	assert(len(b)%8 == 0, "misaligned checksum byte slice")

	// Iterate over 8-byte units and compute checksum.
	for i := 0; i < len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return s0, s1
}

func assert(condition bool, msg string) {
	if !condition {
		panic("assertion failed: " + msg)
	}
}
