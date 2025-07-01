package cdc

import (
	"encoding/json"

	"github.com/rqlite/rqlite/v8/command/proto"
)

type CDCMessagesEnvelope struct {
	Payload []*CDCMessage `json:"payload"`
}

type CDCMessage struct {
	Index  uint64             `json:"index"`
	Events []*CDCMessageEvent `json:"events"`
}

type CDCMessageEvent struct {
	Op       string         `json:"op"`
	Table    string         `json:"table,omitempty"`
	NewRowId int64          `json:"new_row_id,omitempty"`
	OldRowId int64          `json:"old_row_id,omitempty"`
	Before   map[string]any `json:"before,omitempty"`
	After    map[string]any `json:"after,omitempty"`
}

func MarshalToEnvelopeJSON(evs []*proto.CDCEvents) ([]byte, error) {
	if len(evs) == 0 {
		return nil, nil
	}

	envelope := &CDCMessagesEnvelope{
		Payload: make([]*CDCMessage, len(evs)),
	}

	for i, ev := range evs {
		envelope.Payload[i] = &CDCMessage{
			Index:  ev.Index,
			Events: make([]*CDCMessageEvent, len(ev.Events)),
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

func UnmarshalFromEnvelopeJSON(data []byte, env *CDCMessagesEnvelope) error {
	return json.Unmarshal(data, env)
}
