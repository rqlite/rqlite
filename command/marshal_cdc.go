package command

import (
	"github.com/rqlite/rqlite/v8/command/proto"
	pb "google.golang.org/protobuf/proto"
)

// MarshalCDCEvents marshals a CDCEvents object into a byte slice.
func MarshalCDCEvents(e *proto.CDCEvents) ([]byte, error) {
	return pb.Marshal(e)
}

// UnmarshalCDCEvents unmarshals a byte slice into a CDCEvents object.
func UnmarshalCDCEvents(data []byte) (*proto.CDCEvents, error) {
	e := &proto.CDCEvents{}
	if err := pb.Unmarshal(data, e); err != nil {
		return nil, err
	}
	return e, nil
}
