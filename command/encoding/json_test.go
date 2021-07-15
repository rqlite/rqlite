package encoding

import (
	"testing"

	"github.com/rqlite/rqlite/command"
)

func Test_MarshalExecuteResult(t *testing.T) {
	var b []byte
	var err error
	var r *command.ExecuteResult

	r = &command.ExecuteResult{
		LastInsertId: 1,
		RowsAffected: 2,
		Time:         1234,
	}
	b, err = JSONMarshal(r)
	if err != nil {
		t.Fatalf("failed to marshal ExecuteResult: %s", err.Error())
	}
	if exp, got := `{"last_insert_id":1,"rows_affected":2,"time":1234}`, string(b); exp != got {
		t.Fatalf("failed to marshal ExecuteResult: exp %s, got %s", exp, got)
	}

	b, err = JSONMarshal(*r)
	if err != nil {
		t.Fatalf("failed to marshal ExecuteResult: %s", err.Error())
	}
	if exp, got := `{"last_insert_id":1,"rows_affected":2,"time":1234}`, string(b); exp != got {
		t.Fatalf("failed to marshal ExecuteResult: exp %s, got %s", exp, got)
	}

	r = &command.ExecuteResult{
		LastInsertId: 4,
		RowsAffected: 5,
		Error:        "something went wrong",
		Time:         6789,
	}
	b, err = JSONMarshal(r)
	if err != nil {
		t.Fatalf("failed to marshal ExecuteResult: %s", err.Error())
	}
	if exp, got := `{"last_insert_id":4,"rows_affected":5,"error":"something went wrong","time":6789}`, string(b); exp != got {
		t.Fatalf("failed to marshal ExecuteResult: exp %s, got %s", exp, got)
	}
}
