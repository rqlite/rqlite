package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command"
	"github.com/rqlite/rqlite/v8/command/encoding"
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
	mgr := mustNewMockManager()
	cred := mustNewMockCredentialStore()
	s := New(tn, db, mgr, cred)
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
	if !strings.Contains(err.Error(), "i/o timeout") {
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
	mgr := mustNewMockManager()
	s := New(tn, db, mgr, cred)
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
	if !strings.Contains(err.Error(), "i/o timeout") {
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
	mgr := mustNewMockManager()
	cred := mustNewMockCredentialStore()
	s := New(tn, db, mgr, cred)
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

func Test_ServiceBackup(t *testing.T) {
	ln, mux := mustNewMux()
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.
	db := mustNewMockDatabase()
	mgr := mustNewMockManager()
	cred := mustNewMockCredentialStore()
	s := New(tn, db, mgr, cred)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	c := NewClient(mustNewDialer(1, false, false), 30*time.Second)

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %s", err.Error())
	}

	// Ready for Backup tests now.
	testData := []byte("this is SQLite data")
	db.backupFn = func(br *command.BackupRequest, dst io.Writer) error {
		if br.Format != command.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY {
			t.Fatalf("wrong backup format requested")
		}
		dst.Write(testData)
		return nil
	}

	buf := new(bytes.Buffer)
	err := c.Backup(backupRequestBinary(true), s.Addr(), NO_CREDS, longWait, buf)
	if err != nil {
		t.Fatalf("failed to backup database: %s", err.Error())
	}

	if !bytes.Equal(buf.Bytes(), testData) {
		t.Fatalf("backup data is not as expected, exp: %s, got: %s", testData, buf.Bytes())
	}

	// Clean up resources.
	if err := ln.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_ServiceLoad(t *testing.T) {
	ln, mux := mustNewMux()
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.
	db := mustNewMockDatabase()
	mgr := mustNewMockManager()
	cred := mustNewMockCredentialStore()
	s := New(tn, db, mgr, cred)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	c := NewClient(mustNewDialer(1, false, false), 30*time.Second)

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %s", err.Error())
	}

	// Ready for Load tests now.
	called := false
	testData := []byte("this is SQLite data")
	db.loadFn = func(lr *command.LoadRequest) error {
		called = true
		if !bytes.Equal(lr.Data, testData) {
			t.Fatalf("load data is not as expected, exp: %s, got: %s", testData, lr.Data)
		}
		return nil
	}

	err := c.Load(loadRequest(testData), s.Addr(), NO_CREDS, longWait)
	if err != nil {
		t.Fatalf("failed to load database: %s", err.Error())
	}

	if !called {
		t.Fatal("load not called on database")
	}

	// Clean up resources.
	if err := ln.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_ServiceRemoveNode(t *testing.T) {
	ln, mux := mustNewMux()
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.
	db := mustNewMockDatabase()
	mgr := mustNewMockManager()
	cred := mustNewMockCredentialStore()
	s := New(tn, db, mgr, cred)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	c := NewClient(mustNewDialer(1, false, false), 30*time.Second)

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %s", err.Error())
	}

	expNodeID := "node_1"
	called := false
	mgr.removeNodeFn = func(rn *command.RemoveNodeRequest) error {
		called = true
		if rn.Id != expNodeID {
			t.Fatalf("node ID is wrong, exp: %s, got %s", expNodeID, rn.Id)
		}
		return nil
	}

	err := c.RemoveNode(removeNodeRequest(expNodeID), s.Addr(), NO_CREDS, longWait)
	if err != nil {
		t.Fatalf("failed to remove node: %s", err.Error())
	}

	if !called {
		t.Fatal("RemoveNode not called on manager")
	}

	// Clean up resources.
	if err := ln.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_ServiceJoinNode(t *testing.T) {
	ln, mux := mustNewMux()
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.
	db := mustNewMockDatabase()
	mgr := mustNewMockManager()
	cred := mustNewMockCredentialStore()
	s := New(tn, db, mgr, cred)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	c := NewClient(mustNewDialer(1, false, false), 30*time.Second)

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %s", err.Error())
	}

	expNodeAddr := "test-node-addr"
	called := false
	mgr.joinFn = func(jr *command.JoinRequest) error {
		called = true
		if jr.Address != expNodeAddr {
			t.Fatalf("node address is wrong, exp: %s, got %s", expNodeAddr, jr.Address)
		}
		return nil
	}

	req := &command.JoinRequest{
		Address: expNodeAddr,
	}
	err := c.Join(req, s.Addr(), nil, longWait)
	if err != nil {
		t.Fatalf("failed to join node: %s", err.Error())
	}

	if !called {
		t.Fatal("JoinNode not called on manager")
	}

	// Clean up resources
	if err := ln.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

// Test_ServiceJoinNodeForwarded ensures that a JoinNode request is forwarded
// to the leader if the node receiving the request is not the leader.
func Test_ServiceJoinNodeForwarded(t *testing.T) {
	headerByte := byte(1)
	cred := mustNewMockCredentialStore()
	c := NewClient(mustNewDialer(headerByte, false, false), 30*time.Second)
	leaderJoinCalled := false

	// Create the Leader service.
	lnL, muxL := mustNewMux()
	go muxL.Serve()
	tnL := muxL.Listen(headerByte)
	dbL := mustNewMockDatabase()
	mgrL := mustNewMockManager()
	sL := New(tnL, dbL, mgrL, cred)
	if sL == nil {
		t.Fatalf("failed to create cluster service for Leader")
	}
	mgrL.joinFn = func(jr *command.JoinRequest) error {
		leaderJoinCalled = true
		return nil
	}
	if err := sL.Open(); err != nil {
		t.Fatalf("failed to open cluster service on Leader: %s", err.Error())
	}

	// Create the Follower service.
	lnF, muxF := mustNewMux()
	go muxF.Serve()
	tnF := muxF.Listen(headerByte)
	dbF := mustNewMockDatabase()
	mgrF := mustNewMockManager()
	sF := New(tnF, dbF, mgrF, cred)
	if sL == nil {
		t.Fatalf("failed to create cluster service for Follower")
	}
	mgrF.joinFn = func(jr *command.JoinRequest) error {
		return fmt.Errorf("not leader")
	}
	mgrF.leaderAddrFn = func() (string, error) {
		return sL.Addr(), nil
	}
	if err := sF.Open(); err != nil {
		t.Fatalf("failed to open cluster service on Follower: %s", err.Error())
	}

	req := &command.JoinRequest{
		Address: "some client",
	}
	err := c.Join(req, sF.Addr(), nil, longWait)
	if err != nil {
		t.Fatalf("failed to join node: %s", err.Error())
	}

	if !leaderJoinCalled {
		t.Fatal("JoinNode not called on leader")
	}

	// Clean up resources
	if err := lnL.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := sL.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
	if err := lnF.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := sF.Close(); err != nil {
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

func executeQueryRequestFromString(s string) *command.ExecuteQueryRequest {
	return executeQueryRequestFromStrings([]string{s})
}

func executeQueryRequestFromStrings(s []string) *command.ExecuteQueryRequest {
	stmts := make([]*command.Statement, len(s))
	for i := range s {
		stmts[i] = &command.Statement{
			Sql: s[i],
		}
	}
	return &command.ExecuteQueryRequest{
		Request: &command.Request{
			Statements: stmts,
		},
	}
}

func backupRequestBinary(leader bool) *command.BackupRequest {
	return &command.BackupRequest{
		Format: command.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY,
		Leader: leader,
	}
}

func loadRequest(b []byte) *command.LoadRequest {
	return &command.LoadRequest{
		Data: b,
	}
}

func removeNodeRequest(id string) *command.RemoveNodeRequest {
	return &command.RemoveNodeRequest{
		Id: id,
	}
}

func asJSON(v interface{}) string {
	enc := encoding.Encoder{}
	b, err := enc.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}
