package json

import (
	"encoding/json"

	"github.com/rqlite/rqlite/v8/command/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// CDCMessagesEnvelope is the envelope for CDC messages as transported over HTTP.
type CDCMessagesEnvelope struct {
	ServiceID string        `json:"service_id,omitempty"`
	NodeID    string        `json:"node_id"`
	Payload   []*CDCMessage `json:"payload"`
}

// CDCMessage represents a single CDC message containing an index and a list of events.
type CDCMessage struct {
	Index     uint64             `json:"index"`
	Timestamp int64              `json:"ts_ns,omitempty"`
	Events    []*CDCMessageEvent `json:"events"`
}

// CDCMessageEvent represents a single CDC event within a CDC message.
type CDCMessageEvent struct {
	Op       string         `json:"op"`
	Table    string         `json:"table,omitempty"`
	NewRowId int64          `json:"new_row_id,omitempty"`
	OldRowId int64          `json:"old_row_id,omitempty"`
	Before   map[string]any `json:"before,omitempty"`
	After    map[string]any `json:"after,omitempty"`
}

// MarshalToEnvelopeJSON converts a slice of CDC events to a JSON envelope format.
func MarshalToEnvelopeJSON(serviceID, nodeID string, ts bool, evs []*proto.CDCIndexedEventGroup) ([]byte, error) {
	if len(evs) == 0 {
		return nil, nil
	}

	envelope := &CDCMessagesEnvelope{
		ServiceID: serviceID,
		NodeID:    nodeID,
		Payload:   make([]*CDCMessage, 0, len(evs)),
	}

	for _, ev := range evs {
		if ev.Flush {
			continue
		}

		p := &CDCMessage{
			Index:  ev.Index,
			Events: make([]*CDCMessageEvent, len(ev.Events)),
		}
		if ts {
			p.Timestamp = ev.CommitTimestamp
		}
		for j, event := range ev.Events {
			p.Events[j] = &CDCMessageEvent{
				Op:       event.Op.String(),
				Table:    event.Table,
				NewRowId: event.NewRowId,
				OldRowId: event.OldRowId,
			}
		}
		envelope.Payload = append(envelope.Payload, p)
	}

	return json.Marshal(envelope)
}

// UnmarshalFromEnvelopeJSON converts a JSON envelope format into a CDCMessagesEnvelope structure.
func UnmarshalFromEnvelopeJSON(data []byte, env *CDCMessagesEnvelope) error {
	return json.Unmarshal(data, env)
}

var pj = protojson.MarshalOptions{
	// Control JSON shape:
	// - UseProtoNames: snake_case field names from .proto instead of lowerCamel JSON names
	// - EmitUnpopulated: include fields with zero values
	// - UseEnumNumbers: render enums as integers (false => strings)
	UseProtoNames:   true,
	EmitUnpopulated: false,
	UseEnumNumbers:  false,

	// Resolve google.protobuf.Any to type URLs if you use Any:
	Resolver: protoregistry.GlobalTypes,
}

func WriteJSON(m *proto.CDCIndexedEventGroup) ([]byte, error) {
	// Marshal directly to []byte; write it immediately.
	// Avoid string conversions (which copy) and avoid extra buffers.
	return pj.Marshal(m)
}
