package encoding

import (
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// TestByteSliceAsArray_MarshalJSON_Empty tests marshaling an empty ByteSliceAsArray.
func TestByteSliceAsArray_MarshalJSON_Empty(t *testing.T) {
	var b ByteSliceAsArray = []byte{}
	expected := "[]"

	bytes, err := b.MarshalJSON()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(bytes) != expected {
		t.Errorf("expected %s, got %s", expected, string(bytes))
	}
}

// TestByteSliceAsArray_MarshalJSON_SingleElement tests marshaling a ByteSliceAsArray with a single element.
func TestByteSliceAsArray_MarshalJSON_SingleElement(t *testing.T) {
	var b ByteSliceAsArray = []byte{42}
	expected := "[42]"

	bytes, err := b.MarshalJSON()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(bytes) != expected {
		t.Errorf("expected %s, got %s", expected, string(bytes))
	}
}

// TestByteSliceAsArray_MarshalJSON_MultipleElements tests marshaling a ByteSliceAsArray with multiple elements.
func TestByteSliceAsArray_MarshalJSON_MultipleElements(t *testing.T) {
	var b ByteSliceAsArray = []byte{0, 255, 213, 127, 42}
	expected := "[0,255,213,127,42]"

	bytes, err := b.MarshalJSON()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(bytes) != expected {
		t.Errorf("expected %s, got %s", expected, string(bytes))
	}
}

func Test_JSONNoEscaping(t *testing.T) {
	enc := Encoder{}
	m := map[string]string{
		"a": "b",
		"c": "d ->> e",
	}
	b, err := enc.JSONMarshal(m)
	if err != nil {
		t.Fatalf("failed to marshal simple map: %s", err.Error())
	}
	if exp, got := `{"a":"b","c":"d ->> e"}`, string(b); exp != got {
		t.Fatalf("incorrect marshal result: exp %s, got %s", exp, got)
	}
}

// Test_MarshalExecuteResult tests JSON marshaling of an ExecuteResult
func Test_MarshalExecuteResult(t *testing.T) {
	var b []byte
	var err error
	var r *proto.ExecuteResult
	enc := Encoder{}

	r = &proto.ExecuteResult{
		LastInsertId: 1,
		RowsAffected: 2,
		Time:         1234,
	}
	b, err = enc.JSONMarshal(r)
	if err != nil {
		t.Fatalf("failed to marshal ExecuteResult: %s", err.Error())
	}
	if exp, got := `{"last_insert_id":1,"rows_affected":2,"time":1234}`, string(b); exp != got {
		t.Fatalf("failed to marshal ExecuteResult: exp %s, got %s", exp, got)
	}

	r = &proto.ExecuteResult{
		LastInsertId: 4,
		RowsAffected: 5,
		Error:        "something went wrong",
		Time:         6789,
	}

	b, err = enc.JSONMarshal(r)
	if err != nil {
		t.Fatalf("failed to marshal ExecuteResult: %s", err.Error())
	}
	if exp, got := `{"last_insert_id":4,"rows_affected":5,"error":"something went wrong","time":6789}`, string(b); exp != got {
		t.Fatalf("failed to marshal ExecuteResult: exp %s, got %s", exp, got)
	}

	b, err = enc.JSONMarshalIndent(r, "", "    ")
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
	enc := Encoder{}

	r1 := &proto.ExecuteResult{
		LastInsertId: 1,
		RowsAffected: 2,
		Time:         1234,
	}
	r2 := &proto.ExecuteResult{
		LastInsertId: 3,
		RowsAffected: 4,
		Time:         5678,
	}
	b, err = enc.JSONMarshal([]*proto.ExecuteResult{r1, r2})
	if err != nil {
		t.Fatalf("failed to marshal ExecuteResults: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":2,"time":1234},{"last_insert_id":3,"rows_affected":4,"time":5678}]`, string(b); exp != got {
		t.Fatalf("failed to marshal ExecuteResults: exp %s, got %s", exp, got)
	}
}

// Test_MarshalQueryRowsError tests error cases
func Test_MarshalQueryRowsError(t *testing.T) {
	var err error
	var r *proto.QueryRows
	enc := Encoder{}

	r = &proto.QueryRows{
		Columns: []string{"c1", "c2"},
		Types:   []string{"int", "float", "string"},
		Time:    6789,
	}

	_, err = enc.JSONMarshal(r)
	if err != ErrTypesColumnsLengthViolation {
		t.Fatalf("succeeded marshaling QueryRows: %s", err)
	}

	enc.Associative = true
	_, err = enc.JSONMarshal(r)
	if err != ErrTypesColumnsLengthViolation {
		t.Fatalf("succeeded marshaling QueryRows (associative): %s", err)
	}
}

// Test_MarshalQueryRows tests JSON marshaling of a QueryRows
func Test_MarshalQueryRows(t *testing.T) {
	var b []byte
	var err error
	var r *proto.QueryRows
	enc := Encoder{}

	r = &proto.QueryRows{
		Columns: []string{"c1", "c2", "c3"},
		Types:   []string{"int", "float", "string"},
		Time:    6789,
	}
	values := make([]*proto.Parameter, len(r.Columns))
	values[0] = &proto.Parameter{
		Value: &proto.Parameter_I{
			I: 123,
		},
	}
	values[1] = &proto.Parameter{
		Value: &proto.Parameter_D{
			D: 678.0,
		},
	}
	values[2] = &proto.Parameter{
		Value: &proto.Parameter_S{
			S: "fiona",
		},
	}

	r.Values = []*proto.Values{
		{Parameters: values},
	}

	b, err = enc.JSONMarshal(r)
	if err != nil {
		t.Fatalf("failed to marshal QueryRows: %s", err.Error())
	}
	if exp, got := `{"columns":["c1","c2","c3"],"types":["int","float","string"],"values":[[123,678,"fiona"]],"time":6789}`, string(b); exp != got {
		t.Fatalf("failed to marshal QueryRows: exp %s, got %s", exp, got)
	}

	b, err = enc.JSONMarshalIndent(r, "", "    ")
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

// Test_MarshalQueryAssociativeRows tests JSON (pretty) marshaling of a QueryRows
func Test_MarshalQueryAssociativeRows(t *testing.T) {
	var b []byte
	var err error
	var r *proto.QueryRows
	enc := Encoder{
		Associative: true,
	}

	r = &proto.QueryRows{
		Columns: []string{"c1", "c2", "c3"},
		Types:   []string{"int", "float", "string"},
		Time:    6789,
	}
	values := make([]*proto.Parameter, len(r.Columns))
	values[0] = &proto.Parameter{
		Value: &proto.Parameter_I{
			I: 123,
		},
	}
	values[1] = &proto.Parameter{
		Value: &proto.Parameter_D{
			D: 678.0,
		},
	}
	values[2] = &proto.Parameter{
		Value: &proto.Parameter_S{
			S: "fiona",
		},
	}

	r.Values = []*proto.Values{
		{Parameters: values},
	}

	b, err = enc.JSONMarshal(r)
	if err != nil {
		t.Fatalf("failed to marshal QueryRows: %s", err.Error())
	}
	if exp, got := `{"types":{"c1":"int","c2":"float","c3":"string"},"rows":[{"c1":123,"c2":678,"c3":"fiona"}],"time":6789}`, string(b); exp != got {
		t.Fatalf("failed to marshal QueryRows: exp %s, got %s", exp, got)
	}

	b, err = enc.JSONMarshalIndent(r, "", "    ")
	if err != nil {
		t.Fatalf("failed to marshal QueryRows: %s", err.Error())
	}
	exp := `{
    "types": {
        "c1": "int",
        "c2": "float",
        "c3": "string"
    },
    "rows": [
        {
            "c1": 123,
            "c2": 678,
            "c3": "fiona"
        }
    ],
    "time": 6789
}`
	got := string(b)
	if exp != got {
		t.Fatalf("failed to pretty marshal QueryRows: exp: %s, got: %s", exp, got)
	}
}

// Test_MarshalQueryRows_Blob tests JSON marshaling of QueryRows with
// BLOB values.
func Test_MarshalQueryRows_Blob(t *testing.T) {
	var b []byte
	var err error
	var r *proto.QueryRows
	enc := Encoder{}

	r = &proto.QueryRows{
		Columns: []string{"c1", "c2"},
		Types:   []string{"blob", "string"},
	}
	values := make([]*proto.Parameter, len(r.Columns))
	values[0] = &proto.Parameter{
		Value: &proto.Parameter_Y{
			Y: []byte("hello"),
		},
	}
	values[1] = &proto.Parameter{
		Value: &proto.Parameter_S{
			S: "fiona",
		},
	}

	r.Values = []*proto.Values{
		{Parameters: values},
	}

	b, err = enc.JSONMarshal(r)
	if err != nil {
		t.Fatalf("failed to marshal QueryRows: %s", err.Error())
	}
	if exp, got := `{"columns":["c1","c2"],"types":["blob","string"],"values":[["aGVsbG8=","fiona"]]}`, string(b); exp != got {
		t.Fatalf("failed to marshal QueryRows: exp %s, got %s", exp, got)
	}

	b, err = enc.JSONMarshalIndent(r, "", "    ")
	if err != nil {
		t.Fatalf("failed to marshal QueryRows: %s", err.Error())
	}
	exp := `{
    "columns": [
        "c1",
        "c2"
    ],
    "types": [
        "blob",
        "string"
    ],
    "values": [
        [
            "aGVsbG8=",
            "fiona"
        ]
    ]
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
	var r *proto.QueryRows
	enc := Encoder{}

	r = &proto.QueryRows{
		Columns: []string{"c1", "c2", "c3"},
		Types:   []string{"int", "float", "string"},
		Time:    6789,
	}
	values := make([]*proto.Parameter, len(r.Columns))
	values[0] = &proto.Parameter{
		Value: &proto.Parameter_I{
			I: 123,
		},
	}
	values[1] = &proto.Parameter{
		Value: &proto.Parameter_D{
			D: 678.0,
		},
	}
	values[2] = &proto.Parameter{
		Value: &proto.Parameter_S{
			S: "fiona",
		},
	}

	r.Values = []*proto.Values{
		{Parameters: values},
	}

	b, err = enc.JSONMarshal([]*proto.QueryRows{r, r})
	if err != nil {
		t.Fatalf("failed to marshal QueryRows: %s", err.Error())
	}
	if exp, got := `[{"columns":["c1","c2","c3"],"types":["int","float","string"],"values":[[123,678,"fiona"]],"time":6789},{"columns":["c1","c2","c3"],"types":["int","float","string"],"values":[[123,678,"fiona"]],"time":6789}]`, string(b); exp != got {
		t.Fatalf("failed to marshal QueryRows: exp %s, got %s", exp, got)
	}
}

// Test_MarshalQueryAssociativeRowses tests JSON marshaling of a slice of associative QueryRows
func Test_MarshalQueryAssociativeRowses(t *testing.T) {
	var b []byte
	var err error
	var r *proto.QueryRows
	enc := Encoder{
		Associative: true,
	}

	r = &proto.QueryRows{
		Columns: []string{"c1", "c2", "c3"},
		Types:   []string{"int", "float", "string"},
		Time:    6789,
	}
	values := make([]*proto.Parameter, len(r.Columns))
	values[0] = &proto.Parameter{
		Value: &proto.Parameter_I{
			I: 123,
		},
	}
	values[1] = &proto.Parameter{
		Value: &proto.Parameter_D{
			D: 678.0,
		},
	}
	values[2] = &proto.Parameter{
		Value: &proto.Parameter_S{
			S: "fiona",
		},
	}

	r.Values = []*proto.Values{
		{Parameters: values},
	}

	b, err = enc.JSONMarshal([]*proto.QueryRows{r, r})
	if err != nil {
		t.Fatalf("failed to marshal QueryRows: %s", err.Error())
	}
	if exp, got := `[{"types":{"c1":"int","c2":"float","c3":"string"},"rows":[{"c1":123,"c2":678,"c3":"fiona"}],"time":6789},{"types":{"c1":"int","c2":"float","c3":"string"},"rows":[{"c1":123,"c2":678,"c3":"fiona"}],"time":6789}]`, string(b); exp != got {
		t.Fatalf("failed to marshal QueryRows: exp %s, got %s", exp, got)
	}
}

func Test_MarshalExecuteQueryResponse(t *testing.T) {
	enc := Encoder{}

	tests := []struct {
		name      string
		responses []*proto.ExecuteQueryResponse
		expected  string
	}{
		{
			name: "Test with ExecuteResult",
			responses: []*proto.ExecuteQueryResponse{
				{
					Result: &proto.ExecuteQueryResponse_E{
						E: &proto.ExecuteResult{
							LastInsertId: 123,
							RowsAffected: 456,
						},
					},
				},
			},
			expected: `[{"last_insert_id":123,"rows_affected":456}]`,
		},
		{
			name: "Test with QueryRows",
			responses: []*proto.ExecuteQueryResponse{
				{
					Result: &proto.ExecuteQueryResponse_Q{
						Q: &proto.QueryRows{
							Columns: []string{"column1", "column2"},
							Types:   []string{"type1", "type2"},
							Values: []*proto.Values{
								{
									Parameters: []*proto.Parameter{
										{
											Value: &proto.Parameter_I{
												I: 123,
											},
										},
										{
											Value: &proto.Parameter_S{
												S: "fiona",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: `[{"columns":["column1","column2"],"types":["type1","type2"],"values":[[123,"fiona"]]}]`,
		},
		{
			name: "Test with ExecuteResult and QueryRows",
			responses: []*proto.ExecuteQueryResponse{
				{
					Result: &proto.ExecuteQueryResponse_E{
						E: &proto.ExecuteResult{
							LastInsertId: 123,
							RowsAffected: 456,
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_Error{
						Error: "unique constraint failed",
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_E{
						E: &proto.ExecuteResult{
							Error: "not null constraint failed",
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_E{
						E: &proto.ExecuteResult{
							Error: "unique constraint failed",
							ErrorV2: &proto.Error{
								Message: "unique constraint failed",
								Sqlite: &proto.SQLiteErrorCodes{
									Code:         19,
									ExtendedCode: 2067,
								},
							},
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_Q{
						Q: &proto.QueryRows{
							Error: "query timeout",
							ErrorV2: &proto.Error{
								Message: "query timeout",
							},
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_Q{
						Q: &proto.QueryRows{
							Columns: []string{"column1", "column2"},
							Types:   []string{"int", "text"},
							Values: []*proto.Values{
								{
									Parameters: []*proto.Parameter{
										{
											Value: &proto.Parameter_I{
												I: 456,
											},
										},
										{
											Value: &proto.Parameter_S{
												S: "declan",
											},
										},
									},
								},
							},
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_Q{
						Q: &proto.QueryRows{
							Columns: []string{"column1", "column2"},
							Types:   []string{"int", "text"},
							Values:  []*proto.Values{},
						},
					},
				},
			},
			expected: `[{"last_insert_id":123,"rows_affected":456},{"error":"unique constraint failed","error_v2":{"message":"unique constraint failed"}},{"error":"not null constraint failed"},{"error":"unique constraint failed","error_v2":{"message":"unique constraint failed","sqlite":{"code":19,"extended_code":2067}}},{"error":"query timeout","error_v2":{"message":"query timeout"}},{"columns":["column1","column2"],"types":["int","text"],"values":[[456,"declan"]]},{"columns":["column1","column2"],"types":["int","text"]}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := enc.JSONMarshal(tt.responses)
			if err != nil {
				t.Errorf("failed to marshal ExecuteQueryResponse: %v", err)
			}

			if string(data) != tt.expected {
				t.Errorf("unexpected JSON output: got %v want %v", string(data), tt.expected)
			}
		})
	}
}

func Test_MarshalExecuteQueryAssociativeResponse(t *testing.T) {
	enc := Encoder{
		Associative: true,
	}

	tests := []struct {
		name      string
		responses []*proto.ExecuteQueryResponse
		expected  string
	}{
		{
			name: "Test with ExecuteResult",
			responses: []*proto.ExecuteQueryResponse{
				{
					Result: &proto.ExecuteQueryResponse_E{
						E: &proto.ExecuteResult{
							LastInsertId: 123,
							RowsAffected: 456,
						},
					},
				},
			},
			expected: `[{"last_insert_id":123,"rows_affected":456,"rows":null}]`,
		},
		{
			name: "Test with QueryRows",
			responses: []*proto.ExecuteQueryResponse{
				{
					Result: &proto.ExecuteQueryResponse_Q{
						Q: &proto.QueryRows{
							Columns: []string{"column1", "column2"},
							Types:   []string{"type1", "type2"},
							Values: []*proto.Values{
								{
									Parameters: []*proto.Parameter{
										{
											Value: &proto.Parameter_I{
												I: 123,
											},
										},
										{
											Value: &proto.Parameter_S{
												S: "fiona",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: `[{"types":{"column1":"type1","column2":"type2"},"rows":[{"column1":123,"column2":"fiona"}]}]`,
		},
		{
			name: "Test with ExecuteResult and QueryRows",
			responses: []*proto.ExecuteQueryResponse{
				{
					Result: &proto.ExecuteQueryResponse_E{
						E: &proto.ExecuteResult{
							LastInsertId: 123,
							RowsAffected: 456,
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_Error{
						Error: "unique constraint failed",
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_E{
						E: &proto.ExecuteResult{
							Error: "not null constraint failed",
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_E{
						E: &proto.ExecuteResult{
							Error: "unique constraint failed",
							ErrorV2: &proto.Error{
								Message: "unique constraint failed",
								Sqlite: &proto.SQLiteErrorCodes{
									Code:         19,
									ExtendedCode: 2067,
								},
							},
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_Q{
						Q: &proto.QueryRows{
							Error: "query timeout",
							ErrorV2: &proto.Error{
								Message: "query timeout",
							},
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_Q{
						Q: &proto.QueryRows{
							Columns: []string{"column1", "column2"},
							Types:   []string{"int", "text"},
							Values: []*proto.Values{
								{
									Parameters: []*proto.Parameter{
										{
											Value: &proto.Parameter_I{
												I: 456,
											},
										},
										{
											Value: &proto.Parameter_S{
												S: "declan",
											},
										},
									},
								},
							},
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_Q{
						Q: &proto.QueryRows{
							Columns: []string{"aaa", "bbb"},
							Types:   []string{"int", "text"},
							Values:  []*proto.Values{},
						},
					},
				},
				{
					Result: &proto.ExecuteQueryResponse_Q{
						Q: &proto.QueryRows{
							Columns: []string{"ccc", "ddd"},
							Types:   []string{"int", "text"},
							Values:  nil,
						},
					},
				},
			},
			expected: `[{"last_insert_id":123,"rows_affected":456,"rows":null},{"error":"unique constraint failed","error_v2":{"message":"unique constraint failed"}},{"error":"not null constraint failed"},{"error":"unique constraint failed","error_v2":{"message":"unique constraint failed","sqlite":{"code":19,"extended_code":2067}}},{"rows":[],"error":"query timeout","error_v2":{"message":"query timeout"}},{"types":{"column1":"int","column2":"text"},"rows":[{"column1":456,"column2":"declan"}]},{"types":{"aaa":"int","bbb":"text"},"rows":[]},{"types":{"ccc":"int","ddd":"text"},"rows":[]}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := enc.JSONMarshal(tt.responses)
			if err != nil {
				t.Errorf("failed to marshal ExecuteQueryResponse: %v", err)
			}

			if string(data) != tt.expected {
				t.Errorf("unexpected JSON output: got %v want %v", string(data), tt.expected)
			}
		})
	}
}

// Test_MarshalErrorFormat tests that the error format selects which forms of
// a statement error are rendered, across every kind of result.
func Test_MarshalErrorFormat(t *testing.T) {
	sqliteErr := &proto.Error{
		Message: "no such table: foo",
		Sqlite:  &proto.SQLiteErrorCodes{Code: 1, ExtendedCode: 1},
	}
	responses := []*proto.ExecuteQueryResponse{
		{Result: &proto.ExecuteQueryResponse_E{E: &proto.ExecuteResult{LastInsertId: 1, RowsAffected: 1}}},
		{Result: &proto.ExecuteQueryResponse_E{E: &proto.ExecuteResult{Error: "no such table: foo", ErrorV2: sqliteErr}}},
		{Result: &proto.ExecuteQueryResponse_Q{Q: &proto.QueryRows{Error: "no such table: foo", ErrorV2: sqliteErr}}},
		{Result: &proto.ExecuteQueryResponse_Error{Error: "legacy failure"}},
	}
	rows := []*proto.QueryRows{
		{Columns: []string{"id"}, Types: []string{"integer"}},
		{Error: "query timeout", ErrorV2: &proto.Error{Message: "query timeout"}},
	}
	const v2 = `"error_v2":{"message":"no such table: foo","sqlite":{"code":1,"extended_code":1}}`

	tests := []struct {
		name        string
		format      ErrorFormat
		associative bool
		expResp     string
		expRows     string
	}{
		{
			name:    "both",
			format:  ErrorFormatBoth,
			expResp: `[{"last_insert_id":1,"rows_affected":1},{"error":"no such table: foo",` + v2 + `},{"error":"no such table: foo",` + v2 + `},{"error":"legacy failure","error_v2":{"message":"legacy failure"}}]`,
			expRows: `[{"columns":["id"],"types":["integer"]},{"error":"query timeout","error_v2":{"message":"query timeout"}}]`,
		},
		{
			name:    "v1",
			format:  ErrorFormatV1,
			expResp: `[{"last_insert_id":1,"rows_affected":1},{"error":"no such table: foo"},{"error":"no such table: foo"},{"error":"legacy failure"}]`,
			expRows: `[{"columns":["id"],"types":["integer"]},{"error":"query timeout"}]`,
		},
		{
			name:    "v2",
			format:  ErrorFormatV2,
			expResp: `[{"last_insert_id":1,"rows_affected":1},{` + v2 + `},{` + v2 + `},{"error_v2":{"message":"legacy failure"}}]`,
			expRows: `[{"columns":["id"],"types":["integer"]},{"error_v2":{"message":"query timeout"}}]`,
		},
		{
			name:        "both associative",
			format:      ErrorFormatBoth,
			associative: true,
			expResp:     `[{"last_insert_id":1,"rows_affected":1,"rows":null},{"error":"no such table: foo",` + v2 + `},{"rows":[],"error":"no such table: foo",` + v2 + `},{"error":"legacy failure","error_v2":{"message":"legacy failure"}}]`,
			expRows:     `[{"types":{"id":"integer"},"rows":[]},{"rows":[],"error":"query timeout","error_v2":{"message":"query timeout"}}]`,
		},
		{
			name:        "v1 associative",
			format:      ErrorFormatV1,
			associative: true,
			expResp:     `[{"last_insert_id":1,"rows_affected":1,"rows":null},{"error":"no such table: foo"},{"rows":[],"error":"no such table: foo"},{"error":"legacy failure"}]`,
			expRows:     `[{"types":{"id":"integer"},"rows":[]},{"rows":[],"error":"query timeout"}]`,
		},
		{
			name:        "v2 associative",
			format:      ErrorFormatV2,
			associative: true,
			expResp:     `[{"last_insert_id":1,"rows_affected":1,"rows":null},{` + v2 + `},{"rows":[],` + v2 + `},{"error_v2":{"message":"legacy failure"}}]`,
			expRows:     `[{"types":{"id":"integer"},"rows":[]},{"rows":[],"error_v2":{"message":"query timeout"}}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := Encoder{Associative: tt.associative, ErrorFormat: tt.format}
			b, err := enc.JSONMarshal(responses)
			if err != nil {
				t.Fatalf("failed to marshal responses: %s", err)
			}
			if got := string(b); got != tt.expResp {
				t.Fatalf("unexpected responses JSON\ngot: %s\nexp: %s", got, tt.expResp)
			}
			b, err = enc.JSONMarshal(rows)
			if err != nil {
				t.Fatalf("failed to marshal rows: %s", err)
			}
			if got := string(b); got != tt.expRows {
				t.Fatalf("unexpected rows JSON\ngot: %s\nexp: %s", got, tt.expRows)
			}
		})
	}
}

// Test_ErrorFormatFromString tests parsing of error format names.
func Test_ErrorFormatFromString(t *testing.T) {
	for _, tt := range []struct {
		s   string
		exp ErrorFormat
	}{
		{"", ErrorFormatBoth}, {"v1", ErrorFormatV1}, {"V1", ErrorFormatV1},
		{"v2", ErrorFormatV2}, {"V2", ErrorFormatV2}, {"v3", ErrorFormatBoth}, {"both", ErrorFormatBoth},
	} {
		if got := ErrorFormatFromString(tt.s); got != tt.exp {
			t.Fatalf("ErrorFormatFromString(%q) = %d, want %d", tt.s, got, tt.exp)
		}
	}
}
