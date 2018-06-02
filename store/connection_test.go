package store

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func Test_NewConnectionNoStore(t *testing.T) {
	c := NewConnection(nil, nil, 1234, 0, 0)
	if c == nil {
		t.Fatal("failed to create new connection")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %s", err.Error())
	}
}

func Test_MultiNodeExecuteQuery(t *testing.T) {
	t.Parallel()

	s0 := mustNewStore(true)
	defer os.RemoveAll(s0.Path())
	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	s0.WaitForLeader(10 * time.Second)

	s1 := mustNewStore(true)
	defer os.RemoveAll(s1.Path())
	if err := s1.Open(false); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	// Join the second node to the first.
	if err := s0.Join(s1.ID(), s1.Addr(), nil); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	// Write data using explicit connection on store0
	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}
	c0 := mustNewConnection(s0)
	re, err := c0.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	r, err := c0.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Query using default connection on store0
	if err := s0.WaitForAppliedIndex(re.Raft.Index, 5*time.Second); err != nil {
		t.Fatalf("error waiting for leader to apply index: %s:", err.Error())
	}
	r, err = s0.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Weak})
	if err != nil {
		t.Fatalf("failed to query leader node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Query using default connection on store1
	if err := s1.WaitForAppliedIndex(re.Raft.Index, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}
	r, err = s1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Weak})
	if err == nil {
		t.Fatalf("successfully queried non-leader node")
	}
	r, err = s1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Strong})
	if err == nil {
		t.Fatalf("successfully queried non-leader node")
	}
	r, err = s1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Kill the leader and check that query can still be satisfied via None consistency.
	c0.Close()
	s0.Close(true)
	r, err = s1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Strong})
	if err == nil {
		t.Fatalf("successfully queried non-leader node: %s", err.Error())
	}
	r, err = s1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query node with None consistency: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_ConnectionExecuteClosed(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)
	c := mustNewConnection(s)

	// Now close the connection and ensure this error is handled. Connections
	// could be closed during use due to timeouts.
	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %s", err.Error())
	}
	if err := c.AbortTransaction(); err != ErrConnectionDoesNotExist {
		t.Fatalf("wrong error returned by closed connection: %v", err)
	}
}

func Test_ConnectionQueryClosed(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)
	c := mustNewConnection(s)

	// Now close the connection and ensure this error is handled. Connections
	// could be closed during use due to timeouts.
	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %s", err.Error())
	}
	_, err := c.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Strong})
	if err != ErrConnectionDoesNotExist {
		t.Fatalf("wrong error returned by closed connection: %v", err)
	}
	_, err = c.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Weak})
	if err != ErrConnectionDoesNotExist {
		t.Fatalf("wrong error returned by closed connection: %v", err)
	}
}

func Test_ConnectionSimpleTimeoutChecks(t *testing.T) {
	t.Parallel()

	c := NewConnection(nil, nil, 1234, 1*time.Second, 1*time.Second)
	if c.IdleTimedOut() {
		t.Fatal("connection incorrectly marked as timed out")
	}
	if c.TxTimedOut() {
		t.Fatal("transaction incorrectly marked as timed out")
	}
	time.Sleep(2 * time.Second)
	if !c.IdleTimedOut() {
		t.Fatal("connection incorrectly not marked as timed out")
	}
	if c.TxTimedOut() {
		t.Fatal("transaction incorrectly marked as timed out")
	}
}

func Test_ConnectionIdleTimeout(t *testing.T) {
	// Test is explicitly not parallel because it accesses global Store stats.

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)
	c0 := mustNewConnectionWithTimeouts(s, 3*time.Second, 0)
	_, ok := s.Connection(c0.ID)
	if !ok {
		t.Fatal("connection not in store after connecting")
	}
	if c0.IdleTimedOut() {
		t.Fatal("connection incorrectly marked as timed out")
	}
	if !pollExpvarStat(stats.Get(numConnTimeouts).String, "1", 10*time.Second) {
		t.Fatalf("connection has not idle-closed: %s", stats.Get(numConnTimeouts).String())
	}

	// Make a second connection, to make sure handling the first connection
	// didn't leave the Store in a bad state.
	c1 := mustNewConnectionWithTimeouts(s, 3*time.Second, 0)
	_, ok = s.Connection(c1.ID)
	if !ok {
		t.Fatal("connection not in store after connecting")
	}
	if c1.IdleTimedOut() {
		t.Fatal("connection incorrectly marked as timed out")
	}
	if !pollExpvarStat(stats.Get(numConnTimeouts).String, "2", 10*time.Second) {
		t.Fatalf("connection has not idle-closed: %s", stats.Get(numConnTimeouts).String())
	}
}

func Test_ConnectionIdleNoTimeout(t *testing.T) {
	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)
	c := mustNewConnectionWithTimeouts(s, 60*time.Second, 0)
	_, ok := s.Connection(c.ID)
	if !ok {
		t.Fatal("connection not in store after connecting")
	}
	defer c.Close()
	if c.IdleTimedOut() {
		t.Fatal("connection incorrectly marked as timed out")
	}

	// Wait, and check that connection is still open.
	time.Sleep(5 * time.Second)
	_, ok = s.Connection(c.ID)
	if !ok {
		t.Fatal("connection not available before idle-close")
	}
	if c.IdleTimedOut() {
		t.Fatal("connection incorrectly marked as timed out")
	}
}

func Test_ConnectionTxTimeout(t *testing.T) {
	// Test is explicitly not parallel because it accesses global Store stats.

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)
	c := mustNewConnectionWithTimeouts(s, 0, 3*time.Second)
	_, ok := s.Connection(c.ID)
	if !ok {
		t.Fatal("connection not in store after connecting")
	}
	if c.TxTimedOut() {
		t.Fatal("transaction incorrectly marked as timed out")
	}

	_, err := c.Execute(&ExecuteRequest{[]string{"BEGIN"}, false, false})
	if err != nil {
		t.Fatalf("failed to begin transaction: %s", err.Error())
	}
	if !c.TransactionActive() {
		t.Fatal("transaction not active")
	}

	if !pollExpvarStat(stats.Get(numConnTimeouts).String, "1", 10*time.Second) {
		t.Fatalf("connection has not aborted tx: %s", stats.Get(numConnTimeouts).String())
	}
}

func Test_ConnectionTxNoTimeout(t *testing.T) {
	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)
	c := mustNewConnectionWithTimeouts(s, 0, 10*time.Second)
	_, ok := s.Connection(c.ID)
	if !ok {
		t.Fatal("connection not in store after connecting")
	}
	defer c.Close()

	_, err := c.Execute(&ExecuteRequest{[]string{"BEGIN"}, false, false})
	if err != nil {
		t.Fatalf("failed to begin transaction: %s", err.Error())
	}
	if !c.TransactionActive() {
		t.Fatal("transaction not active")
	}

	time.Sleep(5 * time.Second)
	if !c.TransactionActive() {
		t.Fatal("transaction not active before timeout")
	}
}

func Test_TxStateChange(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)
	c := mustNewConnection(s)

	txState := NewTxStateChange(c)
	txState.CheckAndSet()
	if !c.TxStartedAt.IsZero() || c.TransactionActive() {
		t.Fatal("transaction marked as started")
	}

	txState = NewTxStateChange(c)
	c.Execute(&ExecuteRequest{[]string{"BEGIN"}, false, false})
	txState.CheckAndSet()
	if c.TxStartedAt.IsZero() || !c.TransactionActive() {
		t.Fatal("transaction not marked as started")
	}

	txState = NewTxStateChange(c)
	c.Execute(&ExecuteRequest{[]string{"INSERT blah blah"}, false, false})
	txState.CheckAndSet()
	if c.TxStartedAt.IsZero() || !c.TransactionActive() {
		t.Fatal("transaction not still marked as started")
	}

	txState = NewTxStateChange(c)
	c.Query(&QueryRequest{[]string{"SELECT * FROM foo"}, false, false, None})
	txState.CheckAndSet()
	if c.TxStartedAt.IsZero() || !c.TransactionActive() {
		t.Fatal("transaction not still marked as started")
	}

	txState = NewTxStateChange(c)
	c.Execute(&ExecuteRequest{[]string{"COMMIT"}, false, false})
	txState.CheckAndSet()
	if !c.TxStartedAt.IsZero() || c.TransactionActive() {
		t.Fatal("transaction still marked as started")
	}

	txState = NewTxStateChange(c)
	c.Execute(&ExecuteRequest{[]string{"BEGIN"}, false, false})
	txState.CheckAndSet()
	if c.TxStartedAt.IsZero() || !c.TransactionActive() {
		t.Fatal("transaction not marked as started")
	}

	txState = NewTxStateChange(c)
	c.Execute(&ExecuteRequest{[]string{"ROLLBACK"}, false, false})
	txState.CheckAndSet()
	if !c.TxStartedAt.IsZero() || c.TransactionActive() {
		t.Fatal("transaction still marked as started")
	}
}

func mustNewConnection(s *Store) *Connection {
	c, err := s.Connect(nil)
	if err != nil {
		panic(fmt.Sprintf("failed to connect to store: %s", err.Error()))
	}
	return c
}

func mustNewConnectionWithTimeouts(s *Store, it, tt time.Duration) *Connection {
	c, err := s.Connect(&ConnectionOptions{it, tt})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to store: %s", err.Error()))
	}
	return c
}
