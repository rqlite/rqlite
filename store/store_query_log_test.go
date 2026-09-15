package store

import (
	"context"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/db/querylog"
)

// Test_StoreQueryLog_Disabled verifies that when QueryLogger is nil the store
// opens and operates normally without any logging.
func Test_StoreQueryLog_Disabled(t *testing.T) {
	s, ln := mustNewStore(t)
	defer s.Close(true)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap store: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to get leader: %s", err)
	}

	er := executeRequestFromString(`CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)`, false, false)
	if _, _, err := s.Execute(context.Background(), er); err != nil {
		t.Fatalf("failed to execute CREATE TABLE: %s", err)
	}
}

// Test_StoreQueryLog_Enabled verifies that attaching a QueryLogger does not
// interfere with normal store operations.
// Log output capture is covered by same-package tests in db/querylog.
func Test_StoreQueryLog_Enabled(t *testing.T) {
	// Use zero threshold (log-everything) so any statement would be logged.
	cfg := NewDBConfig()
	zero := time.Duration(0)
	cfg.QueryLogger = querylog.New(&querylog.Config{MinDuration: &zero})

	ly := mustMockLayer("localhost:0")
	s := New(&Config{
		DBConf: cfg,
		Dir:    t.TempDir(),
		ID:     "test-node",
	}, ly)
	defer s.Close(true)
	defer ly.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap store: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to get leader: %s", err)
	}

	er := executeRequestFromString(`CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)`, false, false)
	if _, _, err := s.Execute(context.Background(), er); err != nil {
		t.Fatalf("failed to execute CREATE TABLE: %s", err)
	}

	er = executeRequestFromString(`INSERT INTO t (name) VALUES ('alice')`, false, false)
	if _, _, err := s.Execute(context.Background(), er); err != nil {
		t.Fatalf("failed to execute INSERT: %s", err)
	}
}
