package store

import (
	"errors"
	"expvar"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/db"
)

// Test_MultiNodeSimple tests that a the core operation of a multi-node
// cluster works as expected. That is, with a two node cluster, writes
// actually replicate, and reads are consistent.
func Test_MultiNodeSimple(t *testing.T) {
	s0, ln := mustNewStore(t)
	defer ln.Close()

	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()

	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join single-node store: %s", err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Write some data.
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if _, err := s0.WaitForAppliedFSM(5 * time.Second); err != nil {
		t.Fatalf("failed to wait for FSM to apply on leader")
	}
	testPoll(t, func() bool {
		return s0.DBAppliedIndex() == s1.DBAppliedIndex()
	}, 250*time.Millisecond, 3*time.Second)

	ci, err := s0.CommitIndex()
	if err != nil {
		t.Fatalf("failed to retrieve commit index: %s", err.Error())
	}
	if exp, got := uint64(4), ci; exp != got {
		t.Fatalf("wrong commit index, got: %d, exp: %d", got, exp)
	}
	lci, err := s0.LeaderCommitIndex()
	if err != nil {
		t.Fatalf("failed to retrieve commit index: %s", err.Error())
	}
	if exp, got := uint64(4), lci; exp != got {
		t.Fatalf("wrong leader commit index, got: %d, exp: %d", got, exp)
	}

	if err := s0.WaitForCommitIndex(4, time.Second); err != nil {
		t.Fatalf("failed to wait for commit index: %s", err.Error())
	}
	if err := s0.WaitForCommitIndex(5, 500*time.Millisecond); err == nil {
		t.Fatalf("unexpectedly waited successfully for commit index")
	}

	// Now, do a NONE consistency query on each node, to actually confirm the data
	// has been replicated.
	testFn1 := func(t *testing.T, s *Store) {
		t.Helper()
		qr := queryRequestFromString("SELECT * FROM foo", false, false)
		qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
		r, err := s.Query(qr)
		if err != nil {
			t.Fatalf("failed to query single node: %s", err.Error())
		}
		if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
	testFn1(t, s0)
	testFn1(t, s1)

	ci, err = s1.CommitIndex()
	if err != nil {
		t.Fatalf("failed to retrieve commit index: %s", err.Error())
	}
	if exp, got := uint64(4), ci; exp != got {
		t.Fatalf("wrong commit index, got: %d, exp: %d", got, exp)
	}
	lci, err = s1.LeaderCommitIndex()
	if err != nil {
		t.Fatalf("failed to retrieve commit index: %s", err.Error())
	}
	if exp, got := uint64(4), lci; exp != got {
		t.Fatalf("wrong leader commit index, got: %d, exp: %d", got, exp)
	}

	if err := s1.WaitForCommitIndex(4, time.Second); err != nil {
		t.Fatalf("failed to wait for commit index: %s", err.Error())
	}
	if err := s1.WaitForCommitIndex(5, 500*time.Millisecond); err == nil {
		t.Fatalf("unexpectedly waited successfully for commit index")
	}

	// Write another row using Request
	rr := executeQueryRequestFromString("INSERT INTO foo(id, name) VALUES(2, 'fiona')", proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG, false, false)
	_, err = s0.Request(rr)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	testPoll(t, func() bool {
		return s0.DBAppliedIndex() == s1.DBAppliedIndex()
	}, 250*time.Millisecond, 3*time.Second)

	// Now, do a NONE consistency query on each node, to actually confirm the data
	// has been replicated.
	testFn2 := func(t *testing.T, s *Store) {
		t.Helper()
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
		qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
		r, err := s.Query(qr)
		if err != nil {
			t.Fatalf("failed to query single node: %s", err.Error())
		}
		if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2]]}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
	testFn2(t, s0)
	testFn2(t, s1)
}

// Test_MultiNodeNode_CommitIndexes tests that the commit indexes are
// correctly updated as nodes join and leave the cluster, and as
// commands are committed through the Raft log.
func Test_MultiNodeNode_CommitIndexes(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer s0.Close(true)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	_, err := s0.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join single-node store: %s", err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	testPoll(t, func() bool {
		// The config change command coming through the log due to s1 joining is not instant.
		return s1.raft.CommitIndex() == 3
	}, 50*time.Millisecond, 2*time.Second)
	if exp, got := uint64(0), s1.raftTn.CommandCommitIndex(); exp != got {
		t.Fatalf("wrong command commit index, got: %d, exp %d", got, exp)
	}

	// Send an FSM command through the log, ensure the indexes are correctly updated.
	// on the follower.
	s0.Noop("don't care")
	testPoll(t, func() bool {
		return s1.numNoops.Load() == 1
	}, 50*time.Millisecond, 2*time.Second)
	if exp, got := uint64(4), s1.raft.CommitIndex(); exp != got {
		t.Fatalf("wrong commit index, got: %d, exp %d", got, exp)
	}
	if exp, got := uint64(4), s1.raftTn.CommandCommitIndex(); exp != got {
		t.Fatalf("wrong command commit index, got: %d, exp %d", got, exp)
	}

	// Join another node to the cluster, which will result in Raft cluster
	// config commands through the log, but no FSM commands.
	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s2.Close(true)
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), true)); err != nil {
		t.Fatalf("failed to join single-node store: %s", err.Error())
	}
	if _, err := s2.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	testPoll(t, func() bool {
		// The config change command coming through the log due to s2 joining is not instant.
		return s2.raft.CommitIndex() == 5
	}, 50*time.Millisecond, 2*time.Second)
	if exp, got := uint64(4), s2.raftTn.CommandCommitIndex(); exp != got {
		t.Fatalf("wrong command commit index, got: %d, exp %d", got, exp)
	}

	// First node to join should also reflect the new cluster config
	// command.
	testPoll(t, func() bool {
		// The config change command coming through the log due to s2 joining is not instant.
		return s1.raft.CommitIndex() == 5
	}, 50*time.Millisecond, 2*time.Second)
	if exp, got := uint64(4), s1.raftTn.CommandCommitIndex(); exp != got {
		t.Fatalf("wrong command commit index, got: %d, exp %d", got, exp)
	}
}

// Test_MultiNodeSnapshot_ErrorMessage tests that a snapshot fails with a specific
// error message when the snapshot is attempted too soon after joining a cluster.
// Hashicorp Raft doesn't expose a typed error, so we have to check the error
// message in the production code. This test makes sure the text doesn't change.
func Test_MultiNodeSnapshot_ErrorMessage(t *testing.T) {
	s0, ln := mustNewStore(t)
	defer ln.Close()

	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	s0.Noop("don't care") // If it fails, we'll find out later.

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()

	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join single-node store: %s", err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	err := s0.Snapshot(0)
	if err == nil {
		t.Fatalf("expected error when snapshotting multi-node store immediately after joining")
	}
	if !strings.Contains(err.Error(), "wait until the configuration entry at") {
		t.Fatalf("expected error to contain 'wait until the configuration entry at', got %s", err.Error())
	}
}

// Test_MultiNodeSnapshot_BlockedSnapshot ensures that if a Snapshot is blocked by the
// Raft subsystem, the Store will revert to FullNeeded. If it didn't do this a WAL file
// could be missed, and the Snapshot Store be placed in an invalid state.
func Test_MultiNodeSnapshot_BlockedSnapshot(t *testing.T) {
	// Fire up first node and write one record.
	s0, ln := mustNewStore(t)
	defer ln.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Create a convenience function to insert a record.
	insertRecord := func(s *Store, n int) {
		t.Helper()
		stmts := make([]string, 0, n)
		for i := 0; i < n; i++ {
			stmts = append(stmts, `INSERT INTO foo(name) VALUES("fiona")`)
		}
		er = executeRequestFromStrings(stmts, false, false)
		_, err = s.Execute(er)
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}

	// Snapshot first node, then insert records.
	if err := s0.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}
	insertRecord(s0, 1000) // Need a larger number to ensure multiple pages are modified and brings out issue.

	// Fire up second node.
	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join single-node store: %s", err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Snapshotting should fail due to the config-change record at the head of the log.
	err = s0.Snapshot(0)
	if err == nil {
		t.Fatalf("expected error when snapshotting multi-node store immediately after joining")
	}
	if !strings.Contains(err.Error(), "wait until the configuration entry at") {
		t.Fatalf("expected error to contain 'wait until the configuration entry at', got %s", err.Error())
	}

	// Snapshot Store should be in FullNeeded mode now.
	fn, err := s0.snapshotStore.FullNeeded()
	if !fn {
		t.Fatalf("expected snapshot store to be in FullNeeded state")
	}

	// Insert another record into the cluster.
	insertRecord(s0, 1)

	// Should be two records in database.
	qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err := s0.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1001]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Snapshotting should work now.
	if err := s0.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}

	// Look inside the latest snapshot store and ensure it has the right data.
	files, err := filepath.Glob(filepath.Join(s0.snapshotDir, "*.db"))
	if err != nil {
		t.Fatalf("failed to list snapshot files: %s", err.Error())
	}
	if len(files) != 1 {
		t.Fatalf("expected one snapshot file, got %d", len(files))
	}
	db, err := db.Open(files[0], false, true)
	if err != nil {
		t.Fatalf("failed to open snapshot database: %s", err.Error())
	}
	defer db.Close()
	qr = queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
	rows, err := db.Query(qr.Request, false)
	if err != nil {
		t.Fatalf("failed to query snapshot database: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1001]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_MultiNodeDBAppliedIndex(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Join a second node to the first.
	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	_, err := s1.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for leader on follower: %s", err.Error())
	}

	// Check that the DBAppliedIndex is the same on both nodes.
	if s0.DBAppliedIndex() != s1.DBAppliedIndex() {
		t.Fatalf("applied index mismatch")
	}

	// Write some data, and check that the DBAppliedIndex remains the same on both nodes.
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(3, "fiona")`,
	}, false, false)
	_, err = s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if _, err := s0.WaitForAppliedFSM(5 * time.Second); err != nil {
		t.Fatalf("failed to wait for FSM to apply on leader")
	}
	testPoll(t, func() bool {
		return s0.DBAppliedIndex() == s1.DBAppliedIndex()
	}, 250*time.Millisecond, 3*time.Second)

	// Create a third node, make sure it joins the cluster, and check that the DBAppliedIndex
	// is correct.
	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s2.Close(true)
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	_, err = s2.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for leader on follower: %s", err.Error())
	}
	testPoll(t, func() bool {
		return s0.DBAppliedIndex() == s2.DBAppliedIndex()

	}, 250*time.Millisecond, 3*time.Second)

	// Noop, then snapshot, truncating all logs. Then have another node join the cluster.
	if af, err := s0.Noop("don't care"); err != nil || af.Error() != nil {
		t.Fatalf("failed to noop on single node: %s", err.Error())
	}
	if err := s0.Snapshot(1); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}
	s3, ln3 := mustNewStore(t)
	defer ln3.Close()
	if err := s3.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s3.Close(true)
	if err := s0.Join(joinRequest(s3.ID(), s3.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	_, err = s3.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for leader on follower: %s", err.Error())
	}
	testPoll(t, func() bool {
		return s0.DBAppliedIndex() <= s3.DBAppliedIndex()
	}, 250*time.Millisecond, 5*time.Second)

	// Write one last row, and everything should be in sync.
	er = executeRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
	}, false, false)
	_, err = s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if _, err := s0.WaitForAppliedFSM(5 * time.Second); err != nil {
		t.Fatalf("failed to wait for FSM to apply on leader")
	}

	testPoll(t, func() bool {
		i := s0.DBAppliedIndex()
		return i == s1.DBAppliedIndex() &&
			i == s2.DBAppliedIndex() &&
			i == s3.DBAppliedIndex()
	}, 100*time.Millisecond, 5*time.Second)
}

func Test_MultiNodeJoinRemove(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)
	if err := s1.Bootstrap(NewServer(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}

	// Get sorted list of cluster nodes.
	storeNodes := []string{s0.ID(), s1.ID()}
	sort.StringSlice(storeNodes).Sort()

	// Join the second node to the first.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	_, err := s1.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader address on follower: %s", err.Error())
	}

	nodes, err := s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}

	if len(nodes) != len(storeNodes) {
		t.Fatalf("size of cluster is not correct")
	}
	if storeNodes[0] != nodes[0].ID || storeNodes[1] != nodes[1].ID {
		t.Fatalf("cluster does not have correct nodes")
	}

	// Should timeout waiting for removal of other node
	err = s0.WaitForRemoval(s1.ID(), time.Second)
	// if err is nil then fail the test
	if err == nil {
		t.Fatalf("no error waiting for removal of nonexistent node")
	}
	if !errors.Is(err, ErrWaitForRemovalTimeout) {
		t.Fatalf("waiting for removal resulted in wrong error: %s", err.Error())
	}

	// Remove a node.
	if err := s0.Remove(removeNodeRequest(s1.ID())); err != nil {
		t.Fatalf("failed to remove %s from cluster: %s", s1.ID(), err.Error())
	}

	nodes, err = s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes post remove: %s", err.Error())
	}
	if len(nodes) != 1 {
		t.Fatalf("size of cluster is not correct post remove")
	}
	if s0.ID() != nodes[0].ID {
		t.Fatalf("cluster does not have correct nodes post remove")
	}

	// Should be no error now waiting for removal of other node
	err = s0.WaitForRemoval(s1.ID(), time.Second)
	// if err is nil then fail the test
	if err != nil {
		t.Fatalf("error waiting for removal of removed node")
	}
}

func Test_MultiNodeStepdown(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s2.Close(true)

	// Form the 3-node cluster
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	if _, err := s2.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Tell leader to step down. After this finishes there should be a new Leader.
	if err := s0.Stepdown(true); err != nil {
		t.Fatalf("leader failed to step down: %s", err.Error())
	}

	check := func() bool {
		leader, err := s1.WaitForLeader(10 * time.Second)
		if err != nil || leader == s0.Addr() {
			return false
		}
		return true
	}
	testPoll(t, check, 250*time.Millisecond, 10*time.Second)
}

func Test_MultiNodeStoreNotifyBootstrap(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s2.Close(true)

	s0.BootstrapExpect = 3
	if err := s0.Notify(notifyRequest(s0.ID(), ln0.Addr().String())); err != nil {
		t.Fatalf("failed to notify store: %s", err.Error())
	}
	if err := s0.Notify(notifyRequest(s0.ID(), ln0.Addr().String())); err != nil {
		t.Fatalf("failed to notify store -- not idempotent: %s", err.Error())
	}
	if err := s0.Notify(notifyRequest(s1.ID(), ln1.Addr().String())); err != nil {
		t.Fatalf("failed to notify store: %s", err.Error())
	}
	if err := s0.Notify(notifyRequest(s2.ID(), ln2.Addr().String())); err != nil {
		t.Fatalf("failed to notify store: %s", err.Error())
	}

	// Check that the cluster bootstrapped properly.
	reportedLeaders := make([]string, 3)
	for i, n := range []*Store{s0, s1, s2} {
		check := func() bool {
			nodes, err := n.Nodes()
			return err == nil && len(nodes) == 3
		}
		testPoll(t, check, 250*time.Millisecond, 10*time.Second)

		var err error
		reportedLeaders[i], err = n.WaitForLeader(10 * time.Second)
		if err != nil {
			t.Fatalf("failed to get leader on node %d (id=%s): %s", i, n.raftID, err.Error())
		}
	}
	if reportedLeaders[0] != reportedLeaders[1] || reportedLeaders[0] != reportedLeaders[2] {
		t.Fatalf("leader not the same on each node")
	}

	// Calling Notify() on a node that is part of a cluster should
	// be a no-op.
	if err := s0.Notify(notifyRequest(s1.ID(), ln1.Addr().String())); err != nil {
		t.Fatalf("failed to notify store that is part of cluster: %s", err.Error())
	}
}

// Test_MultiNodeStoreAutoRestoreBootstrap tests that a cluster will
// bootstrap correctly when each node is supplied with an auto-restore
// file. Only one node should do able to restore from the file.
func Test_MultiNodeStoreAutoRestoreBootstrap(t *testing.T) {
	ResetStats()
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	s2, ln2 := mustNewStore(t)
	defer ln2.Close()

	path0 := mustCopyFileToTempFile(filepath.Join("testdata", "load.sqlite"))
	path1 := mustCopyFileToTempFile(filepath.Join("testdata", "load.sqlite"))
	path2 := mustCopyFileToTempFile(filepath.Join("testdata", "load.sqlite"))

	s0.SetRestorePath(path0)
	s1.SetRestorePath(path1)
	s2.SetRestorePath(path2)

	for i, s := range []*Store{s0, s1, s2} {
		if err := s.Open(); err != nil {
			t.Fatalf("failed to open store %d: %s", i, err.Error())
		}
		defer s.Close(true)
	}

	// Trigger a bootstrap.
	s0.BootstrapExpect = 3
	for _, s := range []*Store{s0, s1, s2} {
		if err := s0.Notify(notifyRequest(s.ID(), s.ly.Addr().String())); err != nil {
			t.Fatalf("failed to notify store: %s", err.Error())
		}
	}

	// Wait for the cluster to bootstrap.
	_, err := s0.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader: %s", err.Error())
	}

	// Ultimately there is a hard-to-control timing issue here. Knowing
	// exactly when the leader has applied the restore is difficult, so
	// just wait a bit.
	time.Sleep(2 * time.Second)

	if !s0.Ready() {
		t.Fatalf("node is not ready")
	}

	qr := queryRequestFromString("SELECT * FROM foo WHERE id=2", false, true)
	r, err := s0.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[2,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	if pathExists(path0) || pathExists(path1) || pathExists(path2) {
		t.Fatalf("an auto-restore file was not removed")
	}

	numAuto := stats.Get(numAutoRestores).(*expvar.Int).Value()
	numAutoSkipped := stats.Get(numAutoRestoresSkipped).(*expvar.Int).Value()
	if exp, got := int64(1), numAuto; exp != got {
		t.Fatalf("unexpected number of auto-restores\nexp: %d\ngot: %d", exp, got)
	}
	if exp, got := int64(2), numAutoSkipped; exp != got {
		t.Fatalf("unexpected number of auto-restores skipped\nexp: %d\ngot: %d", exp, got)
	}
}

func Test_MultiNodeJoinNonVoterRemove(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	// Get sorted list of cluster nodes.
	storeNodes := []string{s0.ID(), s1.ID()}
	sort.StringSlice(storeNodes).Sort()

	// Join the second node to the first.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), false)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Check leader state on follower.
	got, err := s1.LeaderAddr()
	if err != nil {
		t.Fatalf("failed to get leader address: %s", err.Error())
	}
	if exp := s0.Addr(); got != exp {
		t.Fatalf("wrong leader address returned, got: %s, exp %s", got, exp)
	}
	id, err := waitForLeaderID(s1, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to retrieve leader ID: %s", err.Error())
	}
	if got, exp := id, s0.raftID; got != exp {
		t.Fatalf("wrong leader ID returned, got: %s, exp %s", got, exp)
	}

	nodes, err := s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}

	if len(nodes) != len(storeNodes) {
		t.Fatalf("size of cluster is not correct")
	}
	if storeNodes[0] != nodes[0].ID || storeNodes[1] != nodes[1].ID {
		t.Fatalf("cluster does not have correct nodes")
	}

	// Remove the non-voter.
	if err := s0.Remove(removeNodeRequest(s1.ID())); err != nil {
		t.Fatalf("failed to remove %s from cluster: %s", s1.ID(), err.Error())
	}

	nodes, err = s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes post remove: %s", err.Error())
	}
	if len(nodes) != 1 {
		t.Fatalf("size of cluster is not correct post remove")
	}
	if s0.ID() != nodes[0].ID {
		t.Fatalf("cluster does not have correct nodes post remove")
	}
}

func Test_MultiNodeExecuteQuery(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s2.Close(true)

	// Join the second node to the first as a voting node.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	// Join the third node to the first as a non-voting node.
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), false)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	s0FsmIdx, err := s0.WaitForAppliedFSM(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for fsmIndex: %s", err.Error())
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err := s0.Query(qr)
	if err != nil {
		t.Fatalf("failed to query leader node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Wait until the log entries have been applied to the voting follower,
	// and then query.
	if _, err := s1.WaitForFSMIndex(s0FsmIdx, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}

	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	_, err = s1.Query(qr)
	if err == nil {
		t.Fatalf("successfully queried non-leader node")
	}
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	_, err = s1.Query(qr)
	if err == nil {
		t.Fatalf("successfully queried non-leader node")
	}
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err = s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Wait until the 3 log entries have been applied to the non-voting follower,
	// and then query.
	if err := s2.WaitForAppliedIndex(3, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}

	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	_, err = s1.Query(qr)
	if err == nil {
		t.Fatalf("successfully queried non-voting node with Weak")
	}
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	_, err = s1.Query(qr)
	if err == nil {
		t.Fatalf("successfully queried non-voting node with Strong")
	}
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err = s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query non-voting node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_MultiNodeExecuteQueryFreshness(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	// Join the second node to the first.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err := s0.Query(qr)
	if err != nil {
		t.Fatalf("failed to query leader node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Wait until the 3 log entries have been applied to the follower,
	// and then query.
	if err := s1.WaitForAppliedIndex(3, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}

	// "Weak" consistency queries with 1 nanosecond freshness should pass, because freshness
	// is ignored in this case.
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	qr.Freshness = mustParseDuration("1ns").Nanoseconds()
	_, err = s0.Query(qr)
	if err != nil {
		t.Fatalf("Failed to ignore freshness if level is Weak: %s", err.Error())
	}
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	// "Strong" consistency queries with 1 nanosecond freshness should pass, because freshness
	// is ignored in this case.
	_, err = s0.Query(qr)
	if err != nil {
		t.Fatalf("Failed to ignore freshness if level is Strong: %s", err.Error())
	}

	// Kill leader.
	s0.Close(true)

	// "None" consistency queries should still work.
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	qr.Freshness = 0
	r, err = s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Wait for the freshness interval to pass.
	time.Sleep(mustParseDuration("1s"))

	// "None" consistency queries with 1 nanosecond freshness should fail, because at least
	// one nanosecond *should* have passed since leader died (surely!).
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	qr.Freshness = mustParseDuration("1ns").Nanoseconds()
	_, err = s1.Query(qr)
	if err == nil {
		t.Fatalf("freshness violating query didn't return an error")
	}
	if err != ErrStaleRead {
		t.Fatalf("freshness violating query returned wrong error: %s", err.Error())
	}

	// Freshness of 0 is ignored.
	qr.Freshness = 0
	r, err = s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// "None" consistency queries with 1 hour freshness should pass, because it should
	// not be that long since the leader died.
	qr.Freshness = mustParseDuration("1h").Nanoseconds()
	r, err = s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Check Stale-read detection works with Requests too.
	eqr := executeQueryRequestFromString("SELECT * FROM foo", proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE,
		false, false)
	eqr.Freshness = mustParseDuration("1ns").Nanoseconds()
	_, err = s1.Request(eqr)
	if err == nil {
		t.Fatalf("freshness violating request didn't return an error")
	}
	if err != ErrStaleRead {
		t.Fatalf("freshness violating request returned wrong error: %s", err.Error())
	}
	eqr.Freshness = 0
	eqresp, err := s1.Request(eqr)
	if err != nil {
		t.Fatalf("inactive freshness violating request returned an error")
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(eqresp); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_MultiNodeIsLeaderHasLeader(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)
	if err := s1.Bootstrap(NewServer(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}

	// Join the second node to the first.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	leader, err := s1.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader address on follower: %s", err.Error())
	}

	if !s0.HasLeader() {
		t.Fatalf("s0 does not have a leader")
	}
	if !s0.IsLeader() {
		t.Fatalf("s0 is not leader, leader is %s", leader)
	}
	if !s1.HasLeader() {
		t.Fatalf("s1 does not have a leader")
	}
	if s1.IsLeader() {
		t.Fatalf("s1 is leader, leader is %s", leader)
	}
}

func Test_MultiNodeStoreLogTruncation(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	s0.SnapshotThreshold = 4
	s0.SnapshotInterval = 100 * time.Millisecond

	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	nSnaps := stats.Get(numSnapshots).String()

	// Write more than s.SnapshotThreshold statements.
	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(3, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(5, "fiona")`,
	}
	for i := range queries {
		_, err := s0.Execute(executeRequestFromString(queries[i], false, false))
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}

	// Wait for the snapshot to happen and log to be truncated.
	f := func() bool {
		return stats.Get(numSnapshots).String() != nSnaps
	}
	testPoll(t, f, 100*time.Millisecond, 2*time.Second)

	// Do one more execute, to ensure there is at least one log not snapshot.
	// Without this, there is no guarantee fsmIndex will be set on s1.
	_, err := s0.Execute(executeRequestFromString(`INSERT INTO foo(id, name) VALUES(6, "fiona")`, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Fire up new node and ensure it picks up all changes. This will
	// involve getting a snapshot and truncated log.
	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	// Join the second node to the first.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	// Wait until the log entries have been applied to the follower,
	// and then query.
	if err := s1.WaitForAppliedIndex(8, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}
	qr := queryRequestFromString("SELECT count(*) FROM foo", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err := s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[6]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_MultiNodeExecuteQuery_LeaderReadOpt_AllUp(t *testing.T) {
	// Set up a 3-node cluster
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s2.Close(true)

	// Join the second node to the first as a voting node.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	// Join the third node to the first as a non-voting node.
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), false)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Execute some data on the leader
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on leader: %s", err.Error())
	}

	// Perform a strong read consistency query with leader read optimization on all nodes
	var leaderCount int
	for _, s := range []*Store{s0, s1, s2} {
		if s.IsLeader() {
			leaderCount++
		}
		qr := queryRequestFromString("SELECT * FROM foo", false, false)
		qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
		qr.LeaderReadOpt = true
		r, err := s.Query(qr)
		if err != nil {
			// if this node is not the leader, it will return an error
			if !s.IsLeader() {
				// follower nodes should return ErrNotLeader
				if !strings.Contains(err.Error(), "not leader") {
					t.Fatalf("unexpected error on follower node %s: %s", s.ID(), err.Error())
				}
				continue
			} else {
				t.Fatalf("failed to perform query with leader read optimization on a healthy cluster: %s", err.Error())
			}
		}

		// Check the results
		if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}
}

func Test_MultiNodeExecuteQuery_LeaderReadOpt_Quorum(t *testing.T) {
	// Set up a 3-node cluster
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s2.Close(true)

	// Join the second node to the first as a voting node.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	// Join the third node to the first as a non-voting node.
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), false)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Execute some data on the leader
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on leader: %s", err.Error())
	}

	// Kill one follower, still quorum
	s2.Close(true)

	// Wait for the leader to be elected again
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Perform a strong read consistency query with leader read optimization on the remaining nodes
	var leaderCount int
	for _, s := range []*Store{s0, s1} {
		if s.IsLeader() {
			leaderCount++
		}
		qr := queryRequestFromString("SELECT * FROM foo", false, false)
		qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
		qr.LeaderReadOpt = true
		r, err := s.Query(qr)
		if err != nil {
			// if this node is not the leader, it will return an error
			if !s.IsLeader() {
				// follower nodes should return ErrNotLeader
				if !strings.Contains(err.Error(), "not leader") {
					t.Fatalf("unexpected error on follower node %s: %s", s.ID(), err.Error())
				}
				continue
			} else {
				t.Fatalf("failed to perform query with leader read optimization on quorum: %s", err.Error())
			}
		}

		// Check the results
		if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}
}

func Test_MultiNodeExecuteQuery_LeaderReadOpt_NoQuorum(t *testing.T) {
	// Set up a 3-node cluster
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s2.Close(true)

	// Join the second node to the first as a voting node.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	// Join the third node to the first as a non-voting node.
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), false)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Execute some data on the leader
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on leader: %s", err.Error())
	}

	// Kill two followers, no quorum
	s1.Close(true)
	s2.Close(true)

	// Execute some data on the leader, wait for leadership to be lost
	er = executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err = s0.Execute(er)
	if err == nil {
		t.Fatalf("expected leader to not be able to commit, but it did")
	} else if !strings.Contains(err.Error(), "leadership lost while committing log") {
		t.Fatalf("unexpected error on leader: %s", err.Error())
	}

	// Perform a strong read consistency query with leader read optimization on the remaining nodes (should fail)
	if s0.IsLeader() {
		t.Fatalf("expected leader to not be s0, but it was")
	}
	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	qr.LeaderReadOpt = true
	_, err = s0.Query(qr)
	if err == nil {
		t.Fatalf("expected query to fail, but it did not")
	}
	if !strings.Contains(err.Error(), "not leader") {
		t.Fatalf("unexpected error on leader: %s", err.Error())
	}
}

func Test_MultiNodeExecuteQuery_LeaderReadOpt_NoQuorum_NoWait(t *testing.T) {
	// Set up a 3-node cluster
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s2.Close(true)

	// Join the second node to the first as a voting node.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	// Join the third node to the first as a non-voting node.
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), false)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Execute some data on the leader
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on leader: %s", err.Error())
	}

	// Kill two followers, no quorum
	s1.Close(true)
	s2.Close(true)

	// Don't wait for the leadership to be lost, test if the leader read optimization can handle this case
	// if no quorum, s0 should not provide read index availability

	// Perform a strong read consistency query with leader read optimization on the remaining nodes (should fail)
	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	qr.LeaderReadOpt = true
	_, err = s0.Query(qr)
	if err == nil {
		t.Fatalf("expected query to fail, but it did not")
	}
	if !strings.Contains(err.Error(), "not leader") {
		t.Fatalf("unexpected error on leader: %s", err.Error())
	}
}
