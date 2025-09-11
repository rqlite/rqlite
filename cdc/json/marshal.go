package json

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
	Op       string `json:"op"`
	Table    string `json:"table,omitempty"`
	NewRowId int64  `json:"new_row_id,omitempty"`
	OldRowId int64  `json:"old_row_id,omitempty"`
	Before   []any  `json:"before,omitempty"`
	After    []any  `json:"after,omitempty"`
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

	getV := func(v *proto.CDCValue) any {
		if v == nil {
			return nil
		}
		switch v.GetValue().(type) {
		case *proto.CDCValue_D:
			return v.GetD()
		case *proto.CDCValue_I:
			return v.GetI()
		case *proto.CDCValue_S:
			return v.GetS()
		case *proto.CDCValue_B:
			return v.GetB()
		case *proto.CDCValue_Y:
			return v.GetY()
		default:
			return nil
		}
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

			if event.OldRow != nil {
				for _, v := range event.OldRow.Values {
					p.Events[j].Before = append(p.Events[j].Before, getV(v))
				}
			}
			if event.NewRow != nil {
				for _, v := range event.NewRow.Values {
					p.Events[j].After = append(p.Events[j].After, getV(v))
				}
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
