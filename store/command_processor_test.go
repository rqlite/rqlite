package store

import (
	"testing"

	"github.com/rqlite/rqlite/v8/command/proto"
)

func Test_ExecuteResultsMutation_Nil(t *testing.T) {
	var e ExecuteResults
	if e.Mutation() {
		t.Fatalf("expected no mutations for empty ExecuteResults")
	}
}

func Test_ExecuteResultsMutation_Check(t *testing.T) {
	er0 := &proto.ExecuteResult{
		RowsAffected: 0,
	}
	er1 := &proto.ExecuteResult{
		RowsAffected: 1,
	}

	e := ExecuteResults{er0}
	if e.Mutation() {
		t.Fatalf("expected no mutations")
	}
	e = ExecuteResults{er1}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteResults{er0, er1}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
}

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
	if e.Mutation() {
		t.Fatalf("expected no mutations")
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
	if e.Mutation() {
		t.Fatalf("expected no mutations")
	}
	e = ExecuteQueryResponses{qqr}
	if e.Mutation() {
		t.Fatalf("expected no mutations")
	}
}
