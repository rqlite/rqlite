package http

import (
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
	if stmts[0].Query != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Query)
	}
	if stmts[0].Parameters != nil {
		t.Fatal("statment parameters are not nil")
	}
}

func Test_SingleSimpleInvalidRequest(t *testing.T) {
	s := "SELECT * FROM FOO"
	b := []byte(fmt.Sprintf(`["%s"`, s))

	_, err := ParseRequest(b)
	if err != ErrInvalidRequest {
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
	if stmts[0].Query != s0 {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s0, stmts[0].Query)
	}
	if stmts[0].Parameters != nil {
		t.Fatal("statment parameters are not nil")
	}
	if stmts[1].Query != s1 {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s1, stmts[1].Query)
	}
	if stmts[1].Parameters != nil {
		t.Fatal("statment parameters are not nil")
	}
}

func Test_SingleParameterizedRequest(t *testing.T) {
	s := "SELECT * FROM ? ?"
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
	if stmts[0].Query != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Query)
	}

	if len(stmts[0].Parameters) != 2 {
		t.Fatalf("incorrect number of parameters returned: %d", len(stmts[0].Parameters))
	}
	if stmts[0].Parameters[0] != p0 {
		t.Fatalf("incorrect paramter, exp %s, got %s", p0, stmts[0].Parameters[0])
	}
	if int(stmts[0].Parameters[1].(float64)) != p1 {
		t.Fatalf("incorrect paramter, exp %d, got %d", p1, stmts[0].Parameters[1])
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
	if stmts[0].Query != s {
		t.Fatalf("incorrect statement parsed, exp %s, got %s", s, stmts[0].Query)
	}

	if stmts[0].Parameters != nil {
		t.Fatal("statment parameters are not nil")
	}
}

func Test_SingleInvalidParameterizedRequest(t *testing.T) {
	s := "SELECT * FROM ? ?"
	p0 := "FOO"
	p1 := 1
	b := []byte(fmt.Sprintf(`[["%s", "%s", %d]`, s, p0, p1))

	_, err := ParseRequest(b)
	if err != ErrInvalidRequest {
		t.Fatal("got unexpected error for invalid request")
	}
}

func Test_MixedInvalidRequest(t *testing.T) {
	b := []byte(`[["SELECT * FROM foo"], "SELECT * FROM bar"]`)

	_, err := ParseRequest(b)
	if err != ErrInvalidRequest {
		t.Fatal("got unexpected error for invalid request")
	}
}
