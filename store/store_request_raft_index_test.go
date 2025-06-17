package store

import (
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
)

// Test_StoreRequestRaftIndex tests that Store.Request returns the correct Raft index
func Test_StoreRequestRaftIndex(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	defer s.Close(true)

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Test 1: Write request should return a non-zero index
	writeReq := executeQueryRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
	}, proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG, false, false)

	results, raftIndex, err := s.Request(writeReq)
	if err != nil {
		t.Fatalf("failed to execute write request: %s", err.Error())
	}

	if results == nil {
		t.Fatalf("expected results, got nil")
	}

	if raftIndex == 0 {
		t.Fatalf("expected non-zero Raft index for write request, got %d", raftIndex)
	}

	t.Logf("Successfully executed write request and received Raft index: %d", raftIndex)

	// Test 2: Read-only request with NONE consistency should return index 0
	readReq := executeQueryRequestFromStrings([]string{
		`SELECT * FROM foo`,
	}, proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE, false, false)

	results2, readIndex, err2 := s.Request(readReq)
	if err2 != nil {
		t.Fatalf("failed to execute read request: %s", err2.Error())
	}

	if results2 == nil {
		t.Fatalf("expected results for read request, got nil")
	}

	if readIndex != 0 {
		t.Fatalf("expected index 0 for read-only request, got %d", readIndex)
	}

	t.Logf("Successfully executed read-only request and received index: %d", readIndex)

	// Test 3: Another write request should return a higher index
	writeReq2 := executeQueryRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(1, "test")`,
	}, proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG, false, false)

	results3, raftIndex2, err3 := s.Request(writeReq2)
	if err3 != nil {
		t.Fatalf("failed to execute second write request: %s", err3.Error())
	}

	if results3 == nil {
		t.Fatalf("expected results for second write request, got nil")
	}

	if raftIndex2 <= raftIndex {
		t.Fatalf("expected second Raft index (%d) to be greater than first (%d)", raftIndex2, raftIndex)
	}

	t.Logf("Successfully executed second write request and received higher Raft index: %d", raftIndex2)

	// Test 4: STRONG read should go through Raft and return a non-zero index
	strongReadReq := executeQueryRequestFromStrings([]string{
		`SELECT * FROM foo`,
	}, proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG, false, false)

	results4, strongReadIndex, err4 := s.Request(strongReadReq)
	if err4 != nil {
		t.Fatalf("failed to execute strong read request: %s", err4.Error())
	}

	if results4 == nil {
		t.Fatalf("expected results for strong read request, got nil")
	}

	if strongReadIndex == 0 {
		t.Fatalf("expected non-zero Raft index for STRONG read request, got %d", strongReadIndex)
	}

	t.Logf("Successfully executed STRONG read request and received Raft index: %d", strongReadIndex)
}
