package wal

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WALHeader represents the header of a WAL file.
type WALHeader struct {
	Magic     uint32
	Version   uint32
	PageSize  uint32
	Seq       uint32
	Salt1     uint32
	Salt2     uint32
	Checksum1 uint32
	Checksum2 uint32
}

// Copy copies the WALHeader to the given byte slice. If the byte slice
// is too small, a panic occurs.
func (h *WALHeader) Copy(b []byte) {
	if len(b) < WALHeaderSize {
		panic("byte slice too small")
	}
	binary.BigEndian.PutUint32(b[0:], h.Magic)
	binary.BigEndian.PutUint32(b[4:], h.Version)
	binary.BigEndian.PutUint32(b[8:], h.PageSize)
	binary.BigEndian.PutUint32(b[12:], h.Seq)
	binary.BigEndian.PutUint32(b[16:], h.Salt1)
	binary.BigEndian.PutUint32(b[20:], h.Salt2)
	binary.BigEndian.PutUint32(b[24:], h.Checksum1)
	binary.BigEndian.PutUint32(b[28:], h.Checksum2)
}

// Frame points to a single WAL frame in a WAL file.
type Frame struct {
	Pgno   uint32
	Commit uint32
	Data   []byte
}

// WALIterator defines the interface for WAL frame iteration.
type WALIterator interface {
	Header() (*WALHeader, error)
	Next() (*Frame, error)
}

// Writer is used to write a WAL file.
type Writer struct {
	r                WALIterator
	rHeader          *WALHeader
	chksum1, chksum2 uint32
	bo               binary.ByteOrder
}

// NewWriter returns a new Writer.
func NewWriter(r WALIterator) (*Writer, error) {
	w := &Writer{
		r: r,
	}
	rh, err := w.r.Header()
	if err != nil {
		return nil, err
	}
	w.rHeader = rh
	w.chksum1, w.chksum2 = w.rHeader.Checksum1, w.rHeader.Checksum2
	switch magic := w.rHeader.Magic; magic {
	case 0x377f0682:
		w.bo = binary.LittleEndian
	case 0x377f0683:
		w.bo = binary.BigEndian
	default:
		return nil, fmt.Errorf("invalid wal header magic: %x", magic)
	}

	return w, nil
}

// WriteTo writes the frames from the WALIterator to the given io.Writer.
func (w *Writer) WriteTo(ww io.Writer) (n int64, retErr error) {
	nn, err := w.writeWALHeader(ww)
	if err != nil {
		return nn, err
	}
	n += nn

	for {
		frame, err := w.r.Next()
		if err != nil {
			if err == io.EOF {
				break // No more frames!
			}
			return n, err
		}

		if nn, err = w.writeFrame(ww, frame); err != nil {
			return n + nn, err
		}
		n += nn
	}

	return n, nil
}

func (w *Writer) writeWALHeader(ww io.Writer) (n int64, err error) {
	wHeader := make([]byte, WALHeaderSize)

	binary.BigEndian.PutUint32(wHeader[0:], w.rHeader.Magic)
	binary.BigEndian.PutUint32(wHeader[4:], w.rHeader.Version)

	// Database page size
	binary.BigEndian.PutUint32(wHeader[8:], w.rHeader.PageSize)

	// Checkpoint sequence number
	binary.BigEndian.PutUint32(wHeader[12:], w.rHeader.Seq)

	// Salt values, reusing the original salt values.
	binary.BigEndian.PutUint32(wHeader[16:], w.rHeader.Salt1)
	binary.BigEndian.PutUint32(wHeader[20:], w.rHeader.Salt2)

	// Checksum of header
	w.chksum1, w.chksum2 = w.rHeader.Checksum1, w.rHeader.Checksum2
	binary.BigEndian.PutUint32(wHeader[24:], w.chksum1)
	binary.BigEndian.PutUint32(wHeader[28:], w.chksum2)

	// Write the header to the new WAL file.
	nn, err := ww.Write(wHeader)
	return int64(nn), err
}

func (w *Writer) writeFrame(ww io.Writer, frame *Frame) (n int64, err error) {
	frmHdr := make([]byte, WALFrameHeaderSize)

	// Calculate the frame header.
	binary.BigEndian.PutUint32(frmHdr[0:], frame.Pgno)
	binary.BigEndian.PutUint32(frmHdr[4:], frame.Commit)
	binary.BigEndian.PutUint32(frmHdr[8:], w.rHeader.Salt1)
	binary.BigEndian.PutUint32(frmHdr[12:], w.rHeader.Salt2)

	// Checksum of frame header: "...the first 8 bytes..."
	w.chksum1, w.chksum2 = WALChecksum(w.bo, w.chksum1, w.chksum2, frmHdr[:8])

	// Update checksum using frame data: "..the content of all frames up to and including the current frame."
	w.chksum1, w.chksum2 = WALChecksum(w.bo, w.chksum1, w.chksum2, frame.Data)
	binary.BigEndian.PutUint32(frmHdr[16:], w.chksum1)
	binary.BigEndian.PutUint32(frmHdr[20:], w.chksum2)

	// Write the frame header and data.
	nn, err := ww.Write(frmHdr)
	if err != nil {
		return n + int64(nn), err
	}
	n += int64(nn)

	nn, err = ww.Write(frame.Data)
	return n + int64(nn), err
}
