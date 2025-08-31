package cdc

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
)

// CDCMessagesEnvelope is the envelope for CDC messages as transported over HTTP.
type CDCMessagesEnvelope struct {
	ServiceID string        `json:"service_id,omitempty"`
	NodeID    string        `json:"node_id"`
	Payload   []*CDCMessage `json:"payload"`
	Timestamp int64         `json:"ts_ns,omitempty"`
}

// CDCMessage represents a single CDC message containing an index and a list of events.
type CDCMessage struct {
	Index  uint64             `json:"index"`
	Events []*CDCMessageEvent `json:"events"`
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

// cdcRowToMap converts a CDCRow protobuf to a map for JSON serialization.
// Since column names are not available in the protobuf, we use index-based keys.
func cdcRowToMap(row *proto.CDCRow) map[string]any {
	if row == nil || len(row.Values) == 0 {
		return nil
	}

	result := make(map[string]any, len(row.Values))
	for i, value := range row.Values {
		key := fmt.Sprintf("col_%d", i)

		// Convert CDCValue to its underlying Go type
		switch v := value.Value.(type) {
		case *proto.CDCValue_I:
			result[key] = v.I
		case *proto.CDCValue_D:
			result[key] = v.D
		case *proto.CDCValue_B:
			result[key] = v.B
		case *proto.CDCValue_S:
			result[key] = v.S
		case *proto.CDCValue_Y:
			result[key] = v.Y
		default:
			// Handle nil or unknown types
			result[key] = nil
		}
	}
	return result
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
	if ts {
		envelope.Timestamp = time.Now().UnixNano()
	}

	for i, ev := range evs {
		envelope.Payload[i] = &CDCMessage{
			Index:  ev.Index,
			Events: make([]*CDCMessageEvent, len(ev.Events)),
		}

		for j, event := range ev.Events {
			msgEvent := &CDCMessageEvent{
				Op:       event.Op.String(),
				Table:    event.Table,
				NewRowId: event.NewRowId,
				OldRowId: event.OldRowId,
			}

			// Populate Before field from old_row data
			if event.OldRow != nil {
				msgEvent.Before = cdcRowToMap(event.OldRow)
			}

			// Populate After field from new_row data
			if event.NewRow != nil {
				msgEvent.After = cdcRowToMap(event.NewRow)
			}

			envelope.Payload[i].Events[j] = msgEvent
		}
	}

	return json.Marshal(envelope)
}

// UnmarshalFromEnvelopeJSON converts a JSON envelope format into a CDCMessagesEnvelope structure.
func UnmarshalFromEnvelopeJSON(data []byte, env *CDCMessagesEnvelope) error {
	return json.Unmarshal(data, env)
}
