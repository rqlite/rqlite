package cdc

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/rqlite/rqlite/v8/command/proto"
)

func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestMarshalToEnvelopeJSON_WithRowData(t *testing.T) {
	// Create test CDC events with old and new row data
	events := []*proto.CDCIndexedEventGroup{
		{
			Index: 123,
			Events: []*proto.CDCEvent{
				{
					Op:       proto.CDCEvent_INSERT,
					Table:    "users",
					NewRowId: 1,
					NewRow: &proto.CDCRow{
						Values: []*proto.CDCValue{
							{Value: &proto.CDCValue_I{I: 1}},
							{Value: &proto.CDCValue_S{S: "alice"}},
							{Value: &proto.CDCValue_D{D: 25.5}},
							{Value: &proto.CDCValue_B{B: true}},
						},
					},
				},
				{
					Op:       proto.CDCEvent_UPDATE,
					Table:    "users",
					OldRowId: 1,
					NewRowId: 1,
					OldRow: &proto.CDCRow{
						Values: []*proto.CDCValue{
							{Value: &proto.CDCValue_I{I: 1}},
							{Value: &proto.CDCValue_S{S: "alice"}},
							{Value: &proto.CDCValue_D{D: 25.5}},
							{Value: &proto.CDCValue_B{B: true}},
						},
					},
					NewRow: &proto.CDCRow{
						Values: []*proto.CDCValue{
							{Value: &proto.CDCValue_I{I: 1}},
							{Value: &proto.CDCValue_S{S: "alice_updated"}},
							{Value: &proto.CDCValue_D{D: 26.0}},
							{Value: &proto.CDCValue_B{B: false}},
						},
					},
				},
				{
					Op:       proto.CDCEvent_DELETE,
					Table:    "users",
					OldRowId: 1,
					OldRow: &proto.CDCRow{
						Values: []*proto.CDCValue{
							{Value: &proto.CDCValue_I{I: 1}},
							{Value: &proto.CDCValue_S{S: "alice_updated"}},
							{Value: &proto.CDCValue_D{D: 26.0}},
							{Value: &proto.CDCValue_B{B: false}},
						},
					},
				},
			},
		},
	}

	// Marshal to JSON
	data, err := MarshalToEnvelopeJSON("test-service", "test-node", false, events)
	if err != nil {
		t.Fatalf("MarshalToEnvelopeJSON failed: %v", err)
	}

	// Print the JSON for inspection
	t.Logf("Generated JSON: %s", string(data))

	// Unmarshal back to verify structure
	var envelope CDCMessagesEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	// Verify the envelope structure
	if envelope.ServiceID != "test-service" {
		t.Errorf("Expected ServiceID 'test-service', got '%s'", envelope.ServiceID)
	}
	if envelope.NodeID != "test-node" {
		t.Errorf("Expected NodeID 'test-node', got '%s'", envelope.NodeID)
	}
	if len(envelope.Payload) != 1 {
		t.Errorf("Expected 1 payload item, got %d", len(envelope.Payload))
	}

	payload := envelope.Payload[0]
	if payload.Index != 123 {
		t.Errorf("Expected index 123, got %d", payload.Index)
	}
	if len(payload.Events) != 3 {
		t.Errorf("Expected 3 events, got %d", len(payload.Events))
	}

	// Test INSERT event (only After should be set)
	insertEvent := payload.Events[0]
	if insertEvent.Op != "INSERT" {
		t.Errorf("Expected op 'INSERT', got '%s'", insertEvent.Op)
	}
	if insertEvent.Table != "users" {
		t.Errorf("Expected table 'users', got '%s'", insertEvent.Table)
	}
	if insertEvent.NewRowId != 1 {
		t.Errorf("Expected NewRowId 1, got %d", insertEvent.NewRowId)
	}
	if insertEvent.Before != nil {
		t.Errorf("Expected Before to be nil for INSERT, got %v", insertEvent.Before)
	}
	if insertEvent.After == nil {
		t.Fatalf("Expected After to be set for INSERT")
	}

	// Check the After data structure and content
	if len(insertEvent.After) != 4 {
		t.Errorf("Expected 4 columns in After, got %d", len(insertEvent.After))
	}

	// Check individual values (accounting for JSON number conversion to float64)
	if v, ok := insertEvent.After["col_0"]; !ok || v != float64(1) {
		t.Errorf("Expected col_0 to be 1.0, got %v", v)
	}
	if v, ok := insertEvent.After["col_1"]; !ok || v != "alice" {
		t.Errorf("Expected col_1 to be 'alice', got %v", v)
	}
	if v, ok := insertEvent.After["col_2"]; !ok || v != 25.5 {
		t.Errorf("Expected col_2 to be 25.5, got %v", v)
	}
	if v, ok := insertEvent.After["col_3"]; !ok || v != true {
		t.Errorf("Expected col_3 to be true, got %v", v)
	}

	// Test UPDATE event (both Before and After should be set)
	updateEvent := payload.Events[1]
	if updateEvent.Op != "UPDATE" {
		t.Errorf("Expected op 'UPDATE', got '%s'", updateEvent.Op)
	}
	if updateEvent.Before == nil {
		t.Fatalf("Expected Before to be set for UPDATE")
	}
	if updateEvent.After == nil {
		t.Fatalf("Expected After to be set for UPDATE")
	}

	// Check Before data
	if len(updateEvent.Before) != 4 {
		t.Errorf("Expected 4 columns in Before, got %d", len(updateEvent.Before))
	}
	if v, ok := updateEvent.Before["col_1"]; !ok || v != "alice" {
		t.Errorf("Expected Before col_1 to be 'alice', got %v", v)
	}

	// Check After data
	if len(updateEvent.After) != 4 {
		t.Errorf("Expected 4 columns in After, got %d", len(updateEvent.After))
	}
	if v, ok := updateEvent.After["col_1"]; !ok || v != "alice_updated" {
		t.Errorf("Expected After col_1 to be 'alice_updated', got %v", v)
	}
	if v, ok := updateEvent.After["col_3"]; !ok || v != false {
		t.Errorf("Expected After col_3 to be false, got %v", v)
	}

	// Test DELETE event (only Before should be set)
	deleteEvent := payload.Events[2]
	if deleteEvent.Op != "DELETE" {
		t.Errorf("Expected op 'DELETE', got '%s'", deleteEvent.Op)
	}
	if deleteEvent.After != nil {
		t.Errorf("Expected After to be nil for DELETE, got %v", deleteEvent.After)
	}
	if deleteEvent.Before == nil {
		t.Fatalf("Expected Before to be set for DELETE")
	}

	// Check Before data for DELETE
	if len(deleteEvent.Before) != 4 {
		t.Errorf("Expected 4 columns in Before, got %d", len(deleteEvent.Before))
	}
	if v, ok := deleteEvent.Before["col_1"]; !ok || v != "alice_updated" {
		t.Errorf("Expected Before col_1 to be 'alice_updated', got %v", v)
	}
}

func TestCdcRowToMap_NilOrEmpty(t *testing.T) {
	// Test nil row
	result := cdcRowToMap(nil)
	if result != nil {
		t.Errorf("Expected nil for nil row, got %v", result)
	}

	// Test empty row
	emptyRow := &proto.CDCRow{Values: []*proto.CDCValue{}}
	result = cdcRowToMap(emptyRow)
	if result != nil {
		t.Errorf("Expected nil for empty row, got %v", result)
	}
}

func TestCdcRowToMap_AllTypes(t *testing.T) {
	// Test all CDC value types
	row := &proto.CDCRow{
		Values: []*proto.CDCValue{
			{Value: &proto.CDCValue_I{I: 42}},
			{Value: &proto.CDCValue_D{D: 3.14}},
			{Value: &proto.CDCValue_B{B: true}},
			{Value: &proto.CDCValue_S{S: "hello"}},
			{Value: &proto.CDCValue_Y{Y: []byte("world")}},
			{Value: nil}, // Test nil value
		},
	}

	result := cdcRowToMap(row)
	if result == nil {
		t.Fatalf("Expected non-nil result")
	}

	expected := map[string]any{
		"col_0": int64(42),
		"col_1": 3.14,
		"col_2": true,
		"col_3": "hello",
		"col_4": []byte("world"),
		"col_5": nil,
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestMarshalToEnvelopeJSON_NoRowData(t *testing.T) {
	// Test events without row data (current behavior should still work)
	events := []*proto.CDCIndexedEventGroup{
		{
			Index: 456,
			Events: []*proto.CDCEvent{
				{
					Op:       proto.CDCEvent_INSERT,
					Table:    "simple",
					NewRowId: 10,
					// No NewRow or OldRow data
				},
			},
		},
	}

	data, err := MarshalToEnvelopeJSON("", "node1", false, events)
	if err != nil {
		t.Fatalf("MarshalToEnvelopeJSON failed: %v", err)
	}

	var envelope CDCMessagesEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	event := envelope.Payload[0].Events[0]
	if event.Before != nil {
		t.Errorf("Expected Before to be nil when no OldRow, got %v", event.Before)
	}
	if event.After != nil {
		t.Errorf("Expected After to be nil when no NewRow, got %v", event.After)
	}
}
