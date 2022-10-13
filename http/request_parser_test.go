package http

import (
	"encoding/json"
	"fmt"
	"testing"
)

func Test_NilRequest(t *testing.T) {
	_, err := ParseRequest(nil)
	if err != ErrNoStatements {
		t.Fatalf("nil request did not result in correct error")
	}
}

func Test_EmptyRequests(t *testing.T) {
	b := []byte(fmt.Sprintf(`[]`))
	_, err := ParseRequest(b)
	if err != ErrNoStatements {
		t.Fatalf("empty simple request did not result in correct error")
	}

	b = []byte(fmt.Sprintf(`[[]]`))
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
	if int(stmts[0].Parameters[1].GetD()) != p1 {
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
	s := "SELECT * FROM foo WHERE bar=:bar AND qux=:qux"
	b := []byte(fmt.Sprintf(`[["%s", %s]]`, s, mustJSONMarshal(map[string]interface{}{"bar": 1, "qux": "some string"})))

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
	_, err := ParseRequest([]byte(fmt.Sprintf(`[1]`)))
	if err != ErrInvalidJSON {
		t.Fatal("got unexpected error for invalid request")
	}

	_, err = ParseRequest([]byte(fmt.Sprintf(`[[1]]`)))
	if err != ErrInvalidRequest {
		t.Fatal("got unexpected error for invalid request")
	}

	_, err = ParseRequest([]byte(fmt.Sprintf(`[[1, "x", 2]]`)))
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
