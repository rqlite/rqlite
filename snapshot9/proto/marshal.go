package proto

import (
	"encoding/binary"
	"io"

	pb "google.golang.org/protobuf/proto"
)

const (
	ProtobufLength = 8
)

// MarshalAndWrite marshals the given Proof into a byte slice, then writes the length
// of the byte slice as a little-endian ecnodeuint64, followed by the byte slice itself,
// to the given writer.
func MarshalAndWrite(w io.Writer, p *Proof) (int, error) {
	b, err := pb.Marshal(p)
	if err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(b))); err != nil {
		return 0, err
	}
	return w.Write(b)
}
