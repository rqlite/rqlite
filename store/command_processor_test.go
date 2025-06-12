package store

import (
	"testing"

	"github.com/rqlite/rqlite/v8/command"
	"github.com/rqlite/rqlite/v8/command/proto"
)

func Test_ExecuteQueryResponsesMutation(t *testing.T) {
	var e ExecuteQueryResponses
	if e.Mutation() {
		t.Fatalf("expected no mutations for empty ExecuteQueryResponses")
	}
}

func Test_ExecuteQueryResponsesMutation_Check(t *testing.T) {
	eqr0 := &proto.ExecuteQueryResponse{
		Result: &proto.ExecuteQueryResponse_E{
			E: &proto.ExecuteResult{
				RowsAffected: 0,
			},
		},
	}
	eqr1 := &proto.ExecuteQueryResponse{
		Result: &proto.ExecuteQueryResponse_E{
			E: &proto.ExecuteResult{
				RowsAffected: 1,
			},
		},
	}
	qqr := &proto.ExecuteQueryResponse{
		Result: &proto.ExecuteQueryResponse_Q{
			Q: &proto.QueryRows{
				Columns: []string{"foo"},
				Types:   []string{"text"},
			},
		},
	}

	e := ExecuteQueryResponses{eqr0}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteQueryResponses{eqr1}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteQueryResponses{eqr0, eqr1}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteQueryResponses{eqr0, eqr1, qqr}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteQueryResponses{eqr0, qqr}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteQueryResponses{qqr}
	if e.Mutation() {
		t.Fatalf("expected no mutations")
	}
	e = ExecuteQueryResponses{qqr, qqr}
	if e.Mutation() {
		t.Fatalf("expected no mutations")
	}
}

func Test_CommandProcessor_SetKey(t *testing.T) {
	keyReq := &proto.SetKeyRequest{
		Key:   []byte("foo"),
		Value: []byte("bar"),
	}
	// Marshal the SetKeyRequest into bytes
	data, err := command.MarshalSetKeyRequest(keyReq)
	if err != nil {
		t.Fatalf("failed to marshal SetKeyRequest: %v", err)
	}

	req := &proto.Command{
		Type:       proto.Command_COMMAND_TYPE_SET_KEY,
		SubCommand: data,
	}

	m, err := command.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal command: %v", err)
	}

	kv := &mockKeyValueSetter{}
	processor := NewCommandProcessor(nil, nil)
	_, _, r := processor.Process(m, nil, kv)
	if resp, ok := r.(*fsmGenericResponse); !ok {
		t.Fatalf("expected fsmGenericResponse, got %T", r)
	} else if resp.error != nil {
		t.Fatalf("expected no error, got %v", resp.error)
	}
	if string(kv.key) != "foo" || string(kv.value) != "bar" {
		t.Fatalf("expected key 'foo' and value 'bar', got key '%s' and value '%s'", kv.key, kv.value)
	}
}

type mockKeyValueSetter struct {
	key   []byte
	value []byte
}

func (m *mockKeyValueSetter) Set(key []byte, value []byte) error {
	m.key = key
	m.value = value
	return nil
}
