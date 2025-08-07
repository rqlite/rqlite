package command

import (
	"github.com/rqlite/rqlite/v8/command/proto"
	pb "google.golang.org/protobuf/proto"
)

// MarshalCDCIndexedChangeSet marshals a CDCIndexedChangeSet object into a byte slice.
func MarshalCDCIndexedEventGroup(e *proto.CDCIndexedEventGroup) ([]byte, error) {
	if e == nil {
		return nil, nil
	}
	return pb.Marshal(e)
}

// UnmarshalCDCIndexedChangeSet unmarshals a byte slice into a CDCIndexedChangeSet object.
func UnmarshalCDCIndexedChangeSet(data []byte) (*proto.CDCIndexedEventGroup, error) {
	if data == nil {
		return nil, nil
	}
	e := &proto.CDCIndexedEventGroup{}
	if err := pb.Unmarshal(data, e); err != nil {
		return nil, err
	}
	return e, nil
}
