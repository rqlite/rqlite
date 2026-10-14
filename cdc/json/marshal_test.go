package json

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// Test_MarshalToEnvelopeJSON_NonFiniteFloats tests that floating-point values
// which have no JSON representation are encoded as strings, rather than
// causing the whole envelope to fail to marshal.
func Test_MarshalToEnvelopeJSON_NonFiniteFloats(t *testing.T) {
	ev := &proto.CDCEvent{
		Op:          proto.CDCEvent_UPDATE,
		Table:       "foo",
		ColumnNames: []string{"id", "a", "b", "c", "d"},
		OldRowId:    1,
		NewRowId:    1,
		OldRow: &proto.CDCRow{
			Values: []*proto.CDCValue{
				{Value: &proto.CDCValue_I{I: 1}},
				{Value: &proto.CDCValue_D{D: 1.5}},
				{Value: &proto.CDCValue_D{D: math.Inf(1)}},
				{Value: &proto.CDCValue_D{D: math.Inf(-1)}},
				{Value: &proto.CDCValue_D{D: math.NaN()}},
			},
		},
		NewRow: &proto.CDCRow{
			Values: []*proto.CDCValue{
				{Value: &proto.CDCValue_I{I: 1}},
				{Value: &proto.CDCValue_D{D: math.Inf(1)}},
				{Value: &proto.CDCValue_D{D: 2.5}},
				{Value: &proto.CDCValue_D{D: math.NaN()}},
				{Value: &proto.CDCValue_D{D: math.Inf(-1)}},
			},
		},
	}
	evs := []*proto.CDCIndexedEventGroup{{
		Index:  7,
		Events: []*proto.CDCEvent{ev},
	}}

	b, err := MarshalToEnvelopeJSON("svc", "node1", false, evs)
	if err != nil {
		t.Fatalf("failed to marshal envelope: %v", err)
	}
	if !json.Valid(b) {
		t.Fatalf("marshalled envelope is not valid JSON: %s", b)
	}

	env := &CDCMessagesEnvelope{}
	if err := UnmarshalFromEnvelopeJSON(b, env); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}
	if len(env.Payload) != 1 || len(env.Payload[0].Events) != 1 {
		t.Fatalf("unexpected payload shape: %s", b)
	}
	got := env.Payload[0].Events[0]
	if got.Error != "" {
		t.Fatalf("unexpected event error: %s", got.Error)
	}

	expBefore := map[string]any{"id": float64(1), "a": 1.5, "b": "Infinity", "c": "-Infinity", "d": "NaN"}
	expAfter := map[string]any{"id": float64(1), "a": "Infinity", "b": 2.5, "c": "NaN", "d": "-Infinity"}
	for k, v := range expBefore {
		if got.Before[k] != v {
			t.Fatalf("before[%s]: got %v (%T), want %v (%T)", k, got.Before[k], got.Before[k], v, v)
		}
	}
	for k, v := range expAfter {
		if got.After[k] != v {
			t.Fatalf("after[%s]: got %v (%T), want %v (%T)", k, got.After[k], got.After[k], v, v)
		}
	}
}
