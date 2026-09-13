package store

import (
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
)

func Test_ExecuteQueryResponsesMutation(t *testing.T) {
	var e ExecuteQueryResponses
	if e.Mutation() {
		t.Fatalf("expected no mutations for empty ExecuteQueryResponses")
	}
}

func Test_ExecuteQueryResponsesMutation_Check(t *testing.T) {
	eqr0 := &proto.ExecuteQueryResponse{
		Mutated: true,
		Result: &proto.ExecuteQueryResponse_E{
			E: &proto.ExecuteResult{
				RowsAffected: 0,
			},
		},
	}
	eqr1 := &proto.ExecuteQueryResponse{
		Mutated: true,
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

func Test_ExecuteQueryResponsesMutation_Returning(t *testing.T) {
	e := ExecuteQueryResponses{
		{Result: &proto.ExecuteQueryResponse_Q{Q: &proto.QueryRows{}}},
		{
			Mutated: true,
			Result:  &proto.ExecuteQueryResponse_Q{Q: &proto.QueryRows{}},
		},
	}
	if !e.Mutation() {
		t.Fatal("expected mutation for write returning rows")
	}
}
