package cdc

import (
	"encoding/json"

	"github.com/rqlite/rqlite/v8/command/proto"
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
		Payload:   make([]*CDCMessage, len(evs)),
	}

	for i, ev := range evs {
		envelope.Payload[i] = &CDCMessage{
			Index:  ev.Index,
			Events: make([]*CDCMessageEvent, len(ev.Events)),
		}
		if ts {
			envelope.Payload[i].Timestamp = ev.CommitTimestamp
		}

		for j, event := range ev.Events {
			envelope.Payload[i].Events[j] = &CDCMessageEvent{
				Op:       event.Op.String(),
				Table:    event.Table,
				NewRowId: event.NewRowId,
				OldRowId: event.OldRowId,
			}
		}
	}

	return json.Marshal(envelope)
}

// UnmarshalFromEnvelopeJSON converts a JSON envelope format into a CDCMessagesEnvelope structure.
func UnmarshalFromEnvelopeJSON(data []byte, env *CDCMessagesEnvelope) error {
	return json.Unmarshal(data, env)
}
