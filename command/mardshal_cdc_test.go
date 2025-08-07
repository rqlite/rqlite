package command

import (
	"testing"

	"github.com/rqlite/rqlite/v8/command/proto"
	pb "google.golang.org/protobuf/proto"
)

func Test_MarshalUnmarshal_CDCEvents(t *testing.T) {
	events := &proto.CDCEvents{
		Events: []*proto.CDCEvent{
			{
				Op:    proto.CDCEvent_INSERT,
				Table: "users",
				NewRow: &proto.CDCRow{
					Values: []*proto.CDCValue{
						{Value: &proto.CDCValue_I{I: 1}},
						{Value: &proto.CDCValue_S{S: "Alice"}},
					},
				},
			},
			{
				Op:    proto.CDCEvent_UPDATE,
				Table: "users",
			},
		},
	}

	data, err := MarshalCDCEvents(events)
	if err != nil {
		t.Fatalf("failed to marshal CDCEvents: %v", err)
	}

	unmarshaledEvents, err := UnmarshalCDCEvents(data)
	if err != nil {
		t.Fatalf("failed to unmarshal CDCEvents: %v", err)
	}
	if !pb.Equal(unmarshaledEvents, events) {
		t.Fatalf("unmarshaled events do not match original: got %+v, want %+v", unmarshaledEvents, events)
	}
}
