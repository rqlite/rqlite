package store

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"

	dbpkg "github.com/rqlite/rqlite/v10/db"
)

// Verifies that when QueryLogConfig is nil,
// the store opens and operates normally without any logging.
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

// Verifies that when QueryLogConfig is set,
// SQL statements executed against the store appear in the query log.
func Test_StoreQueryLog_Enabled(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	cfg := NewDBConfig()
	cfg.QueryLogConfig = &dbpkg.QueryLogConfig{Logger: logger}

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

	output := buf.String()
	if !strings.Contains(output, "CREATE TABLE t") {
		t.Fatalf("expected CREATE TABLE in query log, got:\n%s", output)
	}
	if !strings.Contains(output, "INSERT INTO t") {
		t.Fatalf("expected INSERT in query log, got:\n%s", output)
	}
}
