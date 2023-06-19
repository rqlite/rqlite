package http

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

func Test_NilRequest(t *testing.T) {
	_, err := ParseRequest(nil)
	if err != ErrNoStatements {
		t.Fatalf("nil request did not result in correct error")
	}
}

func Test_EmptyRequests(t *testing.T) {
	b := []byte(`[]`)
	_, err := ParseRequest(b)
	if err != ErrNoStatements {
		t.Fatalf("empty simple request did not result in correct error")
	}

	b = []byte(`[[]]`)
	_, err = ParseRequest(b)
	if err != ErrNoStatements {
		t.Fatalf("empty parameterized request did not result in correct error")
	}
}

func Test_SingleSimpleRequest(t *testing.T) {
	s := "SELECT * FROM FOO"
	b := []byte(fmt.Sprintf(`["%s"]`, s))

	stmts, err := ParseRequest(b)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 1 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Sql)
	}
	if stmts[0].Parameters != nil {
		t.Fatal("statement parameters are not nil")
	}
}

func Test_SingleSimpleInvalidRequest(t *testing.T) {
	s := "SELECT * FROM FOO"
	b := []byte(fmt.Sprintf(`["%s"`, s))

	_, err := ParseRequest(b)
	if err != ErrInvalidJSON {
		t.Fatal("got unexpected error for invalid request")
	}
}

func Test_DoubleSimpleRequest(t *testing.T) {
	s0 := "SELECT * FROM FOO"
	s1 := "SELECT * FROM BAR"
	b := []byte(fmt.Sprintf(`["%s", "%s"]`, s0, s1))

	stmts, err := ParseRequest(b)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 2 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s0 {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s0, stmts[0].Sql)
	}
	if stmts[0].Parameters != nil {
		t.Fatal("statement parameters are not nil")
	}
	if stmts[1].Sql != s1 {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s1, stmts[1].Sql)
	}
	if stmts[1].Parameters != nil {
		t.Fatal("statement parameters are not nil")
	}
}

func Test_SingleParameterizedRequest(t *testing.T) {
	s := "SELECT * FROM ? WHERE bar=?"
	p0 := "FOO"
	p1 := 1
	b := []byte(fmt.Sprintf(`[["%s", "%s", %d]]`, s, p0, p1))

	stmts, err := ParseRequest(b)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 1 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Sql)
	}

	if len(stmts[0].Parameters) != 2 {
		t.Fatalf("incorrect number of parameters returned: %d", len(stmts[0].Parameters))
	}
	if stmts[0].Parameters[0].GetS() != p0 {
		t.Fatalf("incorrect parameter, exp %s, got %s", p0, stmts[0].Parameters[0])
	}
	if stmts[0].Parameters[1].GetI() != int64(p1) {
		t.Fatalf("incorrect parameter, exp %d, got %d", p1, int(stmts[0].Parameters[1].GetI()))
	}
}

func Test_SingleParameterizedRequestReturning(t *testing.T) {
	s := "UPDATE ? SET foo='bob' WHERE bar=? RETURNING *"
	p0 := "FOO"
	p1 := 1
	f1 := 1.1
	n1 := int64(1676555296046783000)

	type testCase struct {
		req           []byte
		expect        interface{}
		wantReturning bool
	}
	for _, tc := range []*testCase{
		{req: []byte(fmt.Sprintf(`[[true, "%s", "%s", %d]]`, s, p0, p1)), expect: p1, wantReturning: true},
		// wantReturning is false even though the request has the returning clause
		{req: []byte(fmt.Sprintf(`[["%s", "%s", %.2f]]`, s, p0, f1)), expect: f1, wantReturning: false},
		{req: []byte(fmt.Sprintf(`[[true, "%s", "%s", %d]]`, s, p0, n1)), expect: n1, wantReturning: true},
	} {
		stmts, err := ParseRequest(tc.req)
		if err != nil {
			t.Fatalf("failed to parse request: %s", err.Error())
		}

		if len(stmts) != 1 {
			t.Fatalf("incorrect number of statements returned: %d", len(stmts))
		}
		if stmts[0].Sql != s {
			t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Sql)
		}

		if len(stmts[0].Parameters) != 2 {
			t.Fatalf("incorrect number of parameters returned: %d", len(stmts[0].Parameters))
		}
		if stmts[0].Parameters[0].GetS() != p0 {
			t.Fatalf("incorrect parameter, exp %s, got %s", p0, stmts[0].Parameters[0])
		}
		switch ex := tc.expect.(type) {
		case int64:
			if stmts[0].Parameters[1].GetI() != ex {
				t.Fatalf("incorrect parameter, exp %d, got %d", ex, stmts[0].Parameters[1].GetI())
			}
		case float64:
			if stmts[0].Parameters[1].GetD() != ex {
				t.Fatalf("incorrect parameter, exp %f, got %f", ex, stmts[0].Parameters[1].GetD())
			}
		}
		if stmts[0].Returning != tc.wantReturning {
			t.Fatalf("incorrect returning, exp %v, got %v", tc.wantReturning, stmts[0].Returning)
		}
	}
}

func Test_SingleParameterizedRequestLargeNumber(t *testing.T) {
	s := "SELECT * FROM ? WHERE bar=?"
	p0 := "FOO"
	p1 := int64(1676555296046783000)
	b := []byte(fmt.Sprintf(`[["%s", "%s", %d]]`, s, p0, p1))

	stmts, err := ParseRequest(b)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 1 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Sql)
	}

	if len(stmts[0].Parameters) != 2 {
		t.Fatalf("incorrect number of parameters returned: %d", len(stmts[0].Parameters))
	}
	if stmts[0].Parameters[0].GetS() != p0 {
		t.Fatalf("incorrect parameter, exp %s, got %s", p0, stmts[0].Parameters[0])
	}
	if (stmts[0].Parameters[1].GetI()) != p1 {
		t.Fatalf("incorrect parameter, exp %d, got %d", p1, int(stmts[0].Parameters[1].GetI()))
	}
}

func Test_SingleParameterizedRequestNull(t *testing.T) {
	s := "INSERT INTO test(name, value) VALUES(?, ?)"
	p0 := "fiona"
	b := []byte(fmt.Sprintf(`[["%s", "%s", null]]`, s, p0))

	stmts, err := ParseRequest(b)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 1 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Sql)
	}

	if len(stmts[0].Parameters) != 2 {
		t.Fatalf("incorrect number of parameters returned: %d", len(stmts[0].Parameters))
	}
	if stmts[0].Parameters[0].GetS() != p0 {
		t.Fatalf("incorrect parameter, exp %s, got %s", p0, stmts[0].Parameters[0])
	}
	if stmts[0].Parameters[1].GetValue() != nil {
		t.Fatalf("incorrect nil parameter")
	}
}

func Test_SingleNamedParameterizedRequest(t *testing.T) {
	if _, err := strconv.ParseInt(string("1.23"), 10, 64); err == nil {
		// Just be sure that strconv.ParseInt fails on float, since
		// the internal implementation of ParseRequest relies on it.
		t.Fatal("strconv.ParseInt should fail on float")
	}

	s := "SELECT * FROM foo WHERE bar=:bar AND qux=:qux"
	b := []byte(fmt.Sprintf(`[["%s", %s]]`, s, mustJSONMarshal(map[string]interface{}{"bar": 3, "qux": "some string", "baz": 3.1457})))

	stmts, err := ParseRequest(b)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 1 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Sql)
	}

	if len(stmts[0].Parameters) != 3 {
		t.Fatalf("incorrect number of parameters returned: %d", len(stmts[0].Parameters))
	}

	// build a map of the parameters for easier comparison
	params := make(map[string]interface{})
	for _, p := range stmts[0].Parameters {
		if p.GetName() == "bar" {
			params[p.GetName()] = p.GetI()
		} else if p.GetName() == "qux" {
			params[p.GetName()] = p.GetS()
		} else if p.GetName() == "baz" {
			params[p.GetName()] = p.GetD()
		} else {
			t.Fatalf("unexpected parameter name: %s", p.GetName())
		}
	}

	exp := map[string]interface{}{
		"bar": int64(3),
		"qux": "some string",
		"baz": 3.1457,
	}

	if !reflect.DeepEqual(exp, params) {
		t.Fatalf("incorrect parameters, exp %s, got %s", exp, params)
	}
}

func Test_SingleNamedParameterizedRequestNils(t *testing.T) {
	s := "SELECT * FROM foo WHERE bar=:bar AND qux=:qux"
	b := []byte(fmt.Sprintf(`[["%s", %s]]`, s, mustJSONMarshal(map[string]interface{}{"bar": 666, "qux": "some string", "baz": nil})))

	stmts, err := ParseRequest(b)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 1 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Sql)
	}

	if len(stmts[0].Parameters) != 3 {
		t.Fatalf("incorrect number of parameters returned: %d", len(stmts[0].Parameters))
	}

	// build a map of the parameters for easier comparison
	params := make(map[string]interface{})
	for _, p := range stmts[0].Parameters {
		if p.GetName() == "bar" {
			params[p.GetName()] = p.GetI()
		} else if p.GetName() == "qux" {
			params[p.GetName()] = p.GetS()
		} else if p.GetName() == "baz" {
			params[p.GetName()] = p.GetValue()
		} else {
			t.Fatalf("unexpected parameter name: %s", p.GetName())
		}
	}

	exp := map[string]interface{}{
		"bar": int64(666),
		"qux": "some string",
		"baz": nil,
	}

	if !reflect.DeepEqual(exp, params) {
		t.Fatalf("incorrect parameters, exp %s, got %s", exp, params)
	}
}

func Test_SingleParameterizedRequestNoParams(t *testing.T) {
	s := "SELECT * FROM foo"
	b := []byte(fmt.Sprintf(`[["%s"]]`, s))

	stmts, err := ParseRequest(b)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 1 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Sql)
	}

	if len(stmts[0].Parameters) != 0 {
		t.Fatalf("incorrect number of parameters returned: %d", len(stmts[0].Parameters))
	}
}

func Test_SingleParameterizedRequestNoParamsMixed(t *testing.T) {
	s1 := "SELECT * FROM foo"
	s2 := "SELECT * FROM foo WHERE name=?"
	p2 := "bar"
	b := []byte(fmt.Sprintf(`[["%s"], ["%s", "%s"]]`, s1, s2, p2))

	stmts, err := ParseRequest(b)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 2 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s1 {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s1, stmts[0].Sql)
	}
	if len(stmts[0].Parameters) != 0 {
		t.Fatalf("incorrect number of parameters returned: %d", len(stmts[0].Parameters))
	}

	if stmts[1].Sql != s2 {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s2, stmts[0].Sql)
	}
	if len(stmts[1].Parameters) != 1 {
		t.Fatalf("incorrect number of parameters returned: %d", len(stmts[0].Parameters))
	}
	if stmts[1].Parameters[0].GetS() != p2 {
		t.Fatalf("incorrect parameter, exp %s, got %s", p2, stmts[1].Parameters[0])
	}
}

func Test_SingleSimpleParameterizedRequest(t *testing.T) {
	s := "SELECT * FROM ? ?"
	b := []byte(fmt.Sprintf(`[["%s"]]`, s))

	stmts, err := ParseRequest(b)
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 1 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Sql)
	}

	if stmts[0].Parameters != nil {
		t.Fatal("statement parameters are not nil")
	}
}

func Test_SingleInvalidParameterizedRequest(t *testing.T) {
	s := "SELECT * FROM ? ?"
	p0 := "FOO"
	p1 := 1
	b := []byte(fmt.Sprintf(`[["%s", "%s", %d]`, s, p0, p1))

	_, err := ParseRequest(b)
	if err != ErrInvalidJSON {
		t.Fatal("got unexpected error for invalid request")
	}
}

func Test_MixedInvalidRequest(t *testing.T) {
	b := []byte(`[["SELECT * FROM foo"], "SELECT * FROM bar"]`)

	_, err := ParseRequest(b)
	if err != ErrInvalidJSON {
		t.Fatal("got unexpected error for invalid request")
	}
}

func Test_SingleInvalidTypeRequests(t *testing.T) {
	_, err := ParseRequest([]byte(`[1]`))
	if err != ErrInvalidJSON {
		t.Fatal("got unexpected error for invalid request")
	}

	_, err = ParseRequest([]byte(`[[1]]`))
	if err != ErrInvalidRequest {
		t.Fatal("got unexpected error for invalid request")
	}

	_, err = ParseRequest([]byte(`[[1, "x", 2]]`))
	if err != ErrInvalidRequest {
		t.Fatal("got unexpected error for invalid request")
	}

	_, err = ParseRequest([]byte(`[[true, 1, "x", 2]]`))
	if err != ErrInvalidRequest {
		t.Fatal("got unexpected error for invalid request")
	}
}

func mustJSONMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("failed to JSON marshal value")
	}
	return b
}
