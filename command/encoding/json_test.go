package encoding

import (
	"testing"

	"github.com/rqlite/rqlite/command"
)

// Test_MarshalExecuteResult tests JSON marshaling of an ExecuteResult
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

	b, err = JSONMarshalIndent(r, "", "    ")
	if err != nil {
		t.Fatalf("failed to marshal ExecuteResult: %s", err.Error())
	}
	exp := `{
    "last_insert_id": 4,
    "rows_affected": 5,
    "error": "something went wrong",
    "time": 6789
}`
	got := string(b)
	if exp != got {
		t.Fatalf("failed to pretty marshal ExecuteResult: exp: %s, got: %s", exp, got)
	}
}

// Test_MarshalExecuteResults tests JSON marshaling of a slice of ExecuteResults
func Test_MarshalExecuteResults(t *testing.T) {
	var b []byte
	var err error

	r1 := &command.ExecuteResult{
		LastInsertId: 1,
		RowsAffected: 2,
		Time:         1234,
	}
	r2 := &command.ExecuteResult{
		LastInsertId: 3,
		RowsAffected: 4,
		Time:         5678,
	}
	b, err = JSONMarshal([]*command.ExecuteResult{r1, r2})
	if err != nil {
		t.Fatalf("failed to marshal ExecuteResults: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":2,"time":1234},{"last_insert_id":3,"rows_affected":4,"time":5678}]`, string(b); exp != got {
		t.Fatalf("failed to marshal ExecuteResults: exp %s, got %s", exp, got)
	}
}

// Test_MarshalQueryRows tests JSON marshaling of a QueryRows
func Test_MarshalQueryRows(t *testing.T) {
	var b []byte
	var err error
	var r *command.QueryRows

	r = &command.QueryRows{
		Columns: []string{"c1", "c2", "c3"},
		Types:   []string{"int", "float", "string"},
		Time:    6789,
	}
	values := make([]*command.Parameter, len(r.Columns))
	values[0] = &command.Parameter{
		Value: &command.Parameter_I{
			I: 123,
		},
	}
	values[1] = &command.Parameter{
		Value: &command.Parameter_D{
			D: 678.0,
		},
	}
	values[2] = &command.Parameter{
		Value: &command.Parameter_S{
			S: "fiona",
		},
	}

	r.Values = []*command.Values{
		&command.Values{Parameters: values},
	}

	b, err = JSONMarshal(r)
	if err != nil {
		t.Fatalf("failed to marshal QueryRows: %s", err.Error())
	}
	if exp, got := `{"columns":["c1","c2","c3"],"types":["int","float","string"],"values":[[123,678,"fiona"]],"time":6789}`, string(b); exp != got {
		t.Fatalf("failed to marshal QueryRows: exp %s, got %s", exp, got)
	}

	b, err = JSONMarshalIndent(r, "", "    ")
	if err != nil {
		t.Fatalf("failed to marshal QueryRows: %s", err.Error())
	}
	exp := `{
    "columns": [
        "c1",
        "c2",
        "c3"
    ],
    "types": [
        "int",
        "float",
        "string"
    ],
    "values": [
        [
            123,
            678,
            "fiona"
        ]
    ],
    "time": 6789
}`
	got := string(b)
	if exp != got {
		t.Fatalf("failed to pretty marshal QueryRows: exp: %s, got: %s", exp, got)
	}
}

// Test_MarshalQueryRowses tests JSON marshaling of a slice of QueryRows
func Test_MarshalQueryRowses(t *testing.T) {
	var b []byte
	var err error
	var r *command.QueryRows

	r = &command.QueryRows{
		Columns: []string{"c1", "c2", "c3"},
		Types:   []string{"int", "float", "string"},
		Time:    6789,
	}
	values := make([]*command.Parameter, len(r.Columns))
	values[0] = &command.Parameter{
		Value: &command.Parameter_I{
			I: 123,
		},
	}
	values[1] = &command.Parameter{
		Value: &command.Parameter_D{
			D: 678.0,
		},
	}
	values[2] = &command.Parameter{
		Value: &command.Parameter_S{
			S: "fiona",
		},
	}

	r.Values = []*command.Values{
		&command.Values{Parameters: values},
	}

	b, err = JSONMarshal([]*command.QueryRows{r, r})
	if err != nil {
		t.Fatalf("failed to marshal QueryRows: %s", err.Error())
	}
	if exp, got := `[{"columns":["c1","c2","c3"],"types":["int","float","string"],"values":[[123,678,"fiona"]],"time":6789},{"columns":["c1","c2","c3"],"types":["int","float","string"],"values":[[123,678,"fiona"]],"time":6789}]`, string(b); exp != got {
		t.Fatalf("failed to marshal QueryRows: exp %s, got %s", exp, got)
	}
}
