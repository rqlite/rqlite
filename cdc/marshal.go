package cdc

import (
	"github.com/rqlite/rqlite/v8/command/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

func MarshalJSON(evs []*proto.CDCEvents) ([]byte, error) {
	if len(evs) == 0 {
		return nil, nil
	}

	mo := protojson.MarshalOptions{}
	batchMsg := &proto.CDCEventsBatch{Payload: evs}
	return mo.Marshal(batchMsg)
}
