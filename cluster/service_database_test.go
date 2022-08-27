package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/command/encoding"
)

const shortWait = 1 * time.Second
const longWait = 5 * time.Second

var (
	NO_CREDS = (*Credentials)(nil)
)

func Test_ServiceExecute(t *testing.T) {
	ln, mux := mustNewMux()
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.
	db := mustNewMockDatabase()
	cred := mustNewMockCredentialStore()
	s := New(tn, db, cred)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	c := NewClient(mustNewDialer(1, false, false), 30*time.Second)

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %s", err.Error())
	}

	// Ready for Execute tests now.
	db.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
		if er.Request.Statements[0].Sql != "some SQL" {
			t.Fatalf("incorrect SQL statement received")
		}
		return nil, errors.New("execute failed")
	}
	_, err := c.Execute(executeRequestFromString("some SQL"), s.Addr(), NO_CREDS, longWait)
	if err == nil {
		t.Fatalf("client failed to report error")
	}
	if err.Error() != "execute failed" {
		t.Fatalf("incorrect error message received, got: %s", err.Error())
	}

	db.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
		if er.Request.Statements[0].Sql != "some SQL" {
			t.Fatalf("incorrect SQL statement received")
		}
		result := &command.ExecuteResult{
			LastInsertId: 1234,
			RowsAffected: 5678,
		}
		return []*command.ExecuteResult{result}, nil
	}
	res, err := c.Execute(executeRequestFromString("some SQL"), s.Addr(), NO_CREDS, longWait)
	if err != nil {
		t.Fatalf("failed to execute query: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1234,"rows_affected":5678}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results for execute, expected %s, got %s", exp, got)
	}

	db.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
		if er.Request.Statements[0].Sql != "some SQL" {
			t.Fatalf("incorrect SQL statement received")
		}
		result := &command.ExecuteResult{
			Error: "no such table",
		}
		return []*command.ExecuteResult{result}, nil
	}
	res, err = c.Execute(executeRequestFromString("some SQL"), s.Addr(), NO_CREDS, longWait)
	if err != nil {
		t.Fatalf("failed to execute: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table"}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results for execute, expected %s, got %s", exp, got)
	}

	db.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
		time.Sleep(longWait)
		return nil, nil
	}
	_, err = c.Execute(executeRequestFromString("some SQL"), s.Addr(), NO_CREDS, shortWait)
	if err == nil {
		t.Fatalf("failed to receive expected error")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("failed to receive expected error, got: %T %s", err, err)
	}

	// Clean up resources.
	if err := ln.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_ServiceQuery(t *testing.T) {
	ln, mux := mustNewMux()
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.
	db := mustNewMockDatabase()
	cred := mustNewMockCredentialStore()
	s := New(tn, db, cred)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	c := NewClient(mustNewDialer(1, false, false), 30*time.Second)

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %s", err.Error())
	}

	// Ready for Query tests now.
	db.queryFn = func(er *command.QueryRequest) ([]*command.QueryRows, error) {
		if er.Request.Statements[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("incorrect SQL query received")
		}
		return nil, errors.New("query failed")
	}
	_, err := c.Query(queryRequestFromString("SELECT * FROM foo"), s.Addr(), NO_CREDS, longWait)
	if err == nil {
		t.Fatalf("client failed to report error")
	}
	if err.Error() != "query failed" {
		t.Fatalf("incorrect error message received, got: %s", err.Error())
	}

	db.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, error) {
		if qr.Request.Statements[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("incorrect SQL statement received")
		}
		rows := &command.QueryRows{
			Columns: []string{"c1", "c2"},
			Types:   []string{"t1", "t2"},
		}
		return []*command.QueryRows{rows}, nil
	}
	res, err := c.Query(queryRequestFromString("SELECT * FROM foo"), s.Addr(), NO_CREDS, longWait)
	if err != nil {
		t.Fatalf("failed to query: %s", err.Error())
	}
	if exp, got := `[{"columns":["c1","c2"],"types":["t1","t2"]}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	db.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, error) {
		if qr.Request.Statements[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("incorrect SQL statement received")
		}
		rows := &command.QueryRows{
			Error: "no such table",
		}
		return []*command.QueryRows{rows}, nil
	}
	res, err = c.Query(queryRequestFromString("SELECT * FROM foo"), s.Addr(), NO_CREDS, longWait)
	if err != nil {
		t.Fatalf("failed to query: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table"}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	db.queryFn = func(er *command.QueryRequest) ([]*command.QueryRows, error) {
		time.Sleep(longWait)
		return nil, nil
	}
	_, err = c.Query(queryRequestFromString("some SQL"), s.Addr(), NO_CREDS, shortWait)
	if err == nil {
		t.Fatalf("failed to receive expected error")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("failed to receive expected error, got: %T %s", err, err)
	}

	// Clean up resources.
	if err := ln.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

// Test_ServiceQueryLarge ensures that query responses larger than 64K are
// encoded and decoded correctly.
func Test_ServiceQueryLarge(t *testing.T) {
	ln, mux := mustNewMux()
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.
	db := mustNewMockDatabase()
	cred := mustNewMockCredentialStore()
	s := New(tn, db, cred)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	c := NewClient(mustNewDialer(1, false, false), 30*time.Second)

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %s", err.Error())
	}

	var b strings.Builder
	for i := 0; i < 100000; i++ {
		b.WriteString("bar")
	}
	if b.Len() < 64000 {
		t.Fatalf("failed to generate a large enough string for test")
	}

	// Ready for Query tests now.
	db.queryFn = func(qr *command.QueryRequest) ([]*command.QueryRows, error) {
		parameter := &command.Parameter{
			Value: &command.Parameter_S{
				S: b.String(),
			},
		}
		value := &command.Values{
			Parameters: []*command.Parameter{parameter},
		}

		rows := &command.QueryRows{
			Columns: []string{"c1"},
			Types:   []string{"t1"},
			Values:  []*command.Values{value},
		}
		return []*command.QueryRows{rows}, nil
	}
	res, err := c.Query(queryRequestFromString("SELECT * FROM foo"), s.Addr(), NO_CREDS, longWait)
	if err != nil {
		t.Fatalf("failed to query: %s", err.Error())
	}
	if exp, got := fmt.Sprintf(`[{"columns":["c1"],"types":["t1"],"values":[["%s"]]}]`, b.String()), asJSON(res); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	// Clean up resources.
	if err := ln.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

// Test_BinaryEncoding_Backwards ensures that software earlier than v6.6.2
// can communicate with v6.6.2+ releases. v6.6.2 increased the maximum size
// of cluster responses.
func Test_BinaryEncoding_Backwards(t *testing.T) {
	// Simulate a 6.6.2 encoding a response size less than 16 bits.
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b[0:], uint32(1234))

	// Can older software decode it OK?
	if binary.LittleEndian.Uint16(b[0:]) != 1234 {
		t.Fatal("failed to read correct value")
	}
}

func executeRequestFromString(s string) *command.ExecuteRequest {
	return executeRequestFromStrings([]string{s})
}

// queryRequestFromStrings converts a slice of strings into a command.ExecuteRequest
func executeRequestFromStrings(s []string) *command.ExecuteRequest {
	stmts := make([]*command.Statement, len(s))
	for i := range s {
		stmts[i] = &command.Statement{
			Sql: s[i],
		}
	}
	return &command.ExecuteRequest{
		Request: &command.Request{
			Statements:  stmts,
			Transaction: false,
		},
		Timings: false,
	}
}

func queryRequestFromString(s string) *command.QueryRequest {
	return queryRequestFromStrings([]string{s})
}

// queryRequestFromStrings converts a slice of strings into a command.QueryRequest
func queryRequestFromStrings(s []string) *command.QueryRequest {
	stmts := make([]*command.Statement, len(s))
	for i := range s {
		stmts[i] = &command.Statement{
			Sql: s[i],
		}
	}
	return &command.QueryRequest{
		Request: &command.Request{
			Statements:  stmts,
			Transaction: false,
		},
		Timings: false,
	}
}

func asJSON(v interface{}) string {
	b, err := encoding.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}
