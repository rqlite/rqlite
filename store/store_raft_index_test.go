package store

import (
	"testing"
	"time"
)

func Test_StoreExecuteRaftIndex(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	defer s.Close(true)

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Execute a command and verify we get a non-zero Raft index
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
	}, false, false)

	results, raftIndex, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	if results == nil {
		t.Fatalf("expected results, got nil")
	}

	if raftIndex == 0 {
		t.Fatalf("expected non-zero Raft index, got %d", raftIndex)
	}

	t.Logf("Successfully executed command and received Raft index: %d", raftIndex)

	// Execute another command and verify the index increases
	er2 := executeRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(1, "test")`,
	}, false, false)

	results2, raftIndex2, err2 := s.Execute(er2)
	if err2 != nil {
		t.Fatalf("failed to execute second command: %s", err2.Error())
	}

	if results2 == nil {
		t.Fatalf("expected results for second command, got nil")
	}

	if raftIndex2 <= raftIndex {
		t.Fatalf("expected second Raft index (%d) to be greater than first (%d)", raftIndex2, raftIndex)
	}

	t.Logf("Successfully executed second command and received higher Raft index: %d", raftIndex2)
}
