package command

import (
	"encoding/binary"
	"io"

	"github.com/rqlite/rqlite/v8/command/proto"
)

// BackupWriter is a writer that strips the BackupHeader from the stream
// and writes the remaining bytes to the underlying writer.
type BackupWriter struct {
	w io.Writer

	interceptedLen bool   // Flag indicating if we have intercepted the Protobuf length
	lenBytes       []byte // Buffer to store the protobuf length
	len            uint64 // Decoded length of the Protobuf message

	interceptedPB bool   // Flag indicating if we have intercepted the Protobuf message
	pbBuffer      []byte // Buffer to store the Protobuf message
	hdr           *proto.BackupHeader
}

// NewBackupWriter returns a new BackupWriter.
func NewBackupWriter(w io.Writer) *BackupWriter {
	return &BackupWriter{
		w:   w,
		hdr: &proto.BackupHeader{},
	}
}

// Write writes the given bytes to the underlying writer, stripping the
// BackupHeader from the stream.
func (w *BackupWriter) Write(p []byte) (n int, err error) {
	// Intercept the first 8 bytes to length of the protobuf message
	if !w.interceptedLen {
		remaining := protoBufferLengthSize - len(w.lenBytes)
		toCopy := min(remaining, len(p))

		w.lenBytes = append(w.lenBytes, p[:toCopy]...)
		n += toCopy

		// Check if we have intercepted protoBufferLengthSize bytes
		if len(w.lenBytes) == protoBufferLengthSize {
			w.len = binary.LittleEndian.Uint64(w.lenBytes)
			w.interceptedLen = true
		}

		// If the length of p was less than or equal to the number of bytes
		// we needed then we're done for now.
		if len(p) <= toCopy {
			return n, nil
		}

		// There are more bytes remaining, so adjust p to account for the bytes
		// we have already processed and proceed to the next step.
		p = p[toCopy:]
	}

	// Possibly accumulate the next len bytes so we can unmashal the protobuf message
	if !w.interceptedPB && len(w.pbBuffer) < int(w.len) {
		remaining := int(w.len) - len(w.pbBuffer)
		toCopy := min(remaining, len(p))

		w.pbBuffer = append(w.pbBuffer, p[:toCopy]...)
		n += toCopy

		// Check if we have accumulated len bytes
		if len(w.pbBuffer) == int(w.len) {
			err := UnmarshalBackupHeader(w.pbBuffer, w.hdr)
			if err != nil {
				return n, err
			}
			w.interceptedPB = true

			// Clear the buffer as it's no longer needed
			w.pbBuffer = nil
		}

		// If the length of p was less than or equal to the number of bytes
		// we needed then we're done for now.
		if len(p) <= toCopy {
			return n, nil
		}

		// Adjust p for the remaining bytes
		p = p[toCopy:]
	}

	// Pass any additional bytes to the underlying writer
	baseN, baseErr := w.w.Write(p)
	n += baseN
	return n, baseErr
}
