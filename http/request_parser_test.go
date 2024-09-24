package http

import (
	"bytes"
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

func Test_EmptyRequests_Simple(t *testing.T) {
	b := []byte(`[]`)
	_, err := ParseRequest(bytes.NewReader(b))
	if err != ErrNoStatements {
		t.Fatalf("empty simple request did not result in correct error")
	}
}

func Test_EmptyRequests_Parameterized(t *testing.T) {
	b := []byte(`[[]]`)
	_, err := ParseRequest(bytes.NewReader(b))
	if err != ErrInvalidRequest {
		t.Fatalf("empty parameterized request did not result in correct error: %s", err)
	}
}

func Test_SingleSimpleRequest(t *testing.T) {
	s := "SELECT * FROM FOO"
	b := []byte(fmt.Sprintf(`["%s"]`, s))

	stmts, err := ParseRequest(bytes.NewReader(b))
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

	_, err := ParseRequest(bytes.NewReader(b))
	if err != ErrInvalidJSON {
		t.Fatal("got unexpected error for invalid request")
	}
}

func Test_DoubleSimpleRequest(t *testing.T) {
	s0 := "SELECT * FROM FOO"
	s1 := "SELECT * FROM BAR"
	b := []byte(fmt.Sprintf(`["%s", "%s"]`, s0, s1))

	stmts, err := ParseRequest(bytes.NewReader(b))
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

	stmts, err := ParseRequest(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}

	if len(stmts) != 1 {
		t.Fatalf("incorrect number of statements returned: %d", len(stmts))
	}
	if stmts[0].Sql != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Sql)
	}

	if got, exp := len(stmts[0].Parameters), 2; got != exp {
		t.Fatalf("incorrect number of parameters returned, exp %d, got %d", exp, got)
	}
	if stmts[0].Parameters[0].GetS() != p0 {
		t.Fatalf("incorrect parameter, exp %s, got %s", p0, stmts[0].Parameters[0])
	}
	if stmts[0].Parameters[1].GetI() != int64(p1) {
		t.Fatalf("incorrect parameter, exp %d, got %d", p1, int(stmts[0].Parameters[1].GetI()))
	}
}

func Test_SingleParameterizedRequestLargeNumber(t *testing.T) {
	s := "SELECT * FROM ? WHERE bar=?"
	p0 := "FOO"
	p1 := int64(1676555296046783000)
	b := []byte(fmt.Sprintf(`[["%s", "%s", %d]]`, s, p0, p1))

	stmts, err := ParseRequest(bytes.NewReader(b))
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

func Test_SingleParameterizedRequestHexValue(t *testing.T) {
	s := "SELECT * FROM ? WHERE bar=?"
	p0 := "FOO"
	p1 := []byte{0x01, 0x02, 0x03}
	b := []byte(fmt.Sprintf(`[["%s", "%s", "x'010203'"]]`, s, p0))

	stmts, err := ParseRequest(bytes.NewReader(b))
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
	if !bytes.Equal(stmts[0].Parameters[1].GetY(), p1) {
		t.Fatalf("incorrect parameter, exp %s, got %s", p1, stmts[0].Parameters[1].GetY())
	}
}

func Test_SingleParameterizedRequestHexValue_Falback(t *testing.T) {
	s := "SELECT * FROM ? WHERE bar=?"
	p0 := "FOO"
	p1 := "x'010203"
	b := []byte(fmt.Sprintf(`[["%s", "%s", "%s"]]`, s, p0, p1))

	stmts, err := ParseRequest(bytes.NewReader(b))
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
	if stmts[0].Parameters[1].GetS() != p1 {
		t.Fatalf("incorrect parameter, exp %s, got %s", p1, stmts[0].Parameters[1].GetS())
	}
}

func Test_SingleParameterizedRequestByteArray(t *testing.T) {
	s := "SELECT * FROM ? WHERE bar=?"
	p0 := "FOO"
	p1 := []byte{0x01, 0x02, 0x03}
	b := []byte(fmt.Sprintf(`[["%s", "%s", %s]]`, s, p0, byteSliceToStringArray(p1)))

	stmts, err := ParseRequest(bytes.NewReader(b))
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
	if !bytes.Equal(stmts[0].Parameters[1].GetY(), p1) {
		t.Fatalf("incorrect parameter, exp %s, got %s", p1, stmts[0].Parameters[1].GetY())
	}
}

func Test_SingleParameterizedRequestByteArray_Invalid(t *testing.T) {
	s := "SELECT * FROM ? WHERE bar=?"
	p0 := "FOO"

	b := []byte(fmt.Sprintf(`[["%s", "%s", [7889,2,3]]]`, s, p0))
	if _, err := ParseRequest(bytes.NewReader(b)); err == nil {
		t.Fatalf("expected error for invalid byte array")
	}
	b = []byte(fmt.Sprintf(`[["%s", "%s", [-4,2,3]]]`, s, p0))
	if _, err := ParseRequest(bytes.NewReader(b)); err == nil {
		t.Fatalf("expected error for invalid byte array")
	}
}

func Test_SingleParameterizedRequestNull(t *testing.T) {
	s := "INSERT INTO test(name, value) VALUES(?, ?)"
	p0 := "fiona"
	b := []byte(fmt.Sprintf(`[["%s", "%s", null]]`, s, p0))

	stmts, err := ParseRequest(bytes.NewReader(b))
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

	stmts, err := ParseRequest(bytes.NewReader(b))
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

	stmts, err := ParseRequest(bytes.NewReader(b))
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

	stmts, err := ParseRequest(bytes.NewReader(b))
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

	stmts, err := ParseRequest(bytes.NewReader(b))
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

	stmts, err := ParseRequest(bytes.NewReader(b))
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

	_, err := ParseRequest(bytes.NewReader(b))
	if err != ErrInvalidJSON {
		t.Fatal("got unexpected error for invalid request")
	}
}

func Test_MixedInvalidRequest(t *testing.T) {
	b := []byte(`[["SELECT * FROM foo"], "SELECT * FROM bar"]`)

	_, err := ParseRequest(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("failed to parse request: %s", err.Error())
	}
}

func Test_SingleInvalidTypeRequests(t *testing.T) {
	b := []byte(`[1]`)
	_, err := ParseRequest(bytes.NewReader(b))
	if err != ErrInvalidRequest {
		t.Fatalf("got unexpected error for invalid request: %s", err)
	}

	b = []byte(`[[1]]`)
	_, err = ParseRequest(bytes.NewReader(b))
	if err != ErrInvalidRequest {
		t.Fatal("got unexpected error for invalid request")
	}

	b = []byte(`[[1, "x", 2]]`)
	_, err = ParseRequest(bytes.NewReader(b))
	if err != ErrInvalidRequest {
		t.Fatal("got unexpected error for invalid request")
	}
}

func byteSliceToStringArray(b []byte) string {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	for i, v := range b {
		if i > 0 {
			buffer.WriteString(",")
		}
		buffer.WriteString(fmt.Sprintf("%d", v))
	}
	buffer.WriteString("]")
	return buffer.String()
}

func mustJSONMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("failed to JSON marshal value")
	}
	return b
}
