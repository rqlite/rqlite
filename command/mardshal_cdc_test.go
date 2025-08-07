package command

import (
	"testing"

	"github.com/rqlite/rqlite/v8/command/proto"
	pb "google.golang.org/protobuf/proto"
)

func Test_MarshalUnmarshal_CDCEvents(t *testing.T) {
	testCases := []struct {
		name   string
		events *proto.CDCEvents
	}{
		{
			name: "full event with all data types",
			events: &proto.CDCEvents{
				Events: []*proto.CDCEvent{
					{
						Op:    proto.CDCEvent_INSERT,
						Table: "users",
						NewRow: &proto.CDCRow{
							Values: []*proto.CDCValue{
								{Value: &proto.CDCValue_I{I: 1}},
								{Value: &proto.CDCValue_S{S: "Alice"}},
								{Value: &proto.CDCValue_D{D: 3.14}},
								{Value: &proto.CDCValue_B{B: true}},
								{Value: &proto.CDCValue_Y{Y: []byte("blob")}}},
						},
					},
					{
						Op:    proto.CDCEvent_UPDATE,
						Table: "users",
					},
					{
						Op:    proto.CDCEvent_DELETE,
						Table: "products",
					},
				},
			},
		},
		{
			name:   "empty events slice",
			events: &proto.CDCEvents{Events: []*proto.CDCEvent{}},
		},
		{
			name:   "nil events slice",
			events: &proto.CDCEvents{Events: nil},
		},
		{
			name:   "nil events object",
			events: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := MarshalCDCEvents(tc.events)
			if err != nil {
				t.Fatalf("failed to marshal CDCEvents: %v", err)
			}

			unmarshaledEvents, err := UnmarshalCDCEvents(data)
			if err != nil {
				t.Fatalf("failed to unmarshal CDCEvents: %v", err)
			}

			if !pb.Equal(unmarshaledEvents, tc.events) {
				t.Fatalf("unmarshaled events do not match original: got %+v, want %+v", unmarshaledEvents, tc.events)
			}
		})
	}
}
