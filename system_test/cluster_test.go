package system

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/cluster"
	"github.com/rqlite/rqlite/v8/db"
	"github.com/rqlite/rqlite/v8/http"
	"github.com/rqlite/rqlite/v8/queue"
	"github.com/rqlite/rqlite/v8/store"
	"github.com/rqlite/rqlite/v8/tcp"
)

// Test_JoinLeaderNode tests a join operation between a leader and a new node.
func Test_JoinLeaderNode(t *testing.T) {
	leader := mustNewLeaderNode("leader1")
	defer leader.Deprovision()

	node2 := mustNewNode("node1", false)
	defer node2.Deprovision()
	if err := node2.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
}

// Test_MultiNodeCluster tests formation of a 3-node cluster, and its operation.
func Test_MultiNodeCluster(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c := Cluster{node1, node2}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	// Get a follower and confirm redirects work properly.
	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to get followers: %s", err.Error())
	}
	if len(followers) != 1 {
		t.Fatalf("got incorrect number of followers: %d", len(followers))
	}

	node3 := mustNewNode("node3", false)
	defer node3.Deprovision()
	if err := node3.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c = Cluster{node1, node2, node3}
	leader, err = c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	// Run queries against cluster.
	tests := []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = leader.Execute(tt.stmt)
		} else {
			r, err = leader.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}

	// Kill the leader and wait for the new leader.
	leader.Deprovision()
	c = c.RemoveNode(leader)
	leader, err = c.WaitForNewLeader(leader)
	if err != nil {
		t.Fatalf("failed to find new cluster leader after killing leader: %s", err.Error())
	}

	// Run queries against the now 2-node cluster.
	tests = []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{"error":"table foo already exists"}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("sinead")`,
			expected: `{"results":[{"last_insert_id":2,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"sinead"]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = leader.Execute(tt.stmt)
		} else {
			r, err = leader.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

// Test_MultiNodeClusterRANDOM tests operation of RANDOM() SQL rewriting. It checks that a rewritten
// statement is sent to follower.
func Test_MultiNodeClusterRANDOM(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c := Cluster{node1, node2}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	_, err = leader.Execute("CREATE TABLE foo (id integer not null primary key, name text)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	_, err = leader.Execute(`INSERT INTO foo(id, name) VALUES(RANDOM(), "sinead")`)
	if err != nil {
		t.Fatalf("failed to INSERT record: %s", err.Error())
	}
	r, err := leader.Query("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query for count: %s", err.Error())
	}
	if got, exp := r, `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}`; got != exp {
		t.Fatalf("wrong query results, exp %s, got %s", exp, got)
	}

	// Send a few Noops through to ensure SQLite database has been updated on each node.
	for i := 0; i < 5; i++ {
		node1.Noop("some_id")
	}

	// Check that row is *exactly* the same on each node. This could only happen if RANDOM was
	// rewritten by the Leader before committing to the Raft log.
	tFn := func() bool {
		r1, err := node1.QueryNoneConsistency("SELECT * FROM foo")
		if err != nil {
			t.Fatalf("failed to query node 1: %s", err.Error())
		}
		r2, err := node2.QueryNoneConsistency("SELECT * FROM foo")
		if err != nil {
			t.Fatalf("failed to query node 2: %s", err.Error())
		}
		return r1 == r2
	}
	trueOrTimeout(tFn, 10*time.Second)
}

// Test_MultiNodeCluster_TimeDate tests operation of time and date SQL rewriting.
// It checks that a rewritten statement is sent to follower.
func Test_MultiNodeCluster_TimeDate(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	_, err := node1.Execute("CREATE TABLE foo (date text)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	_, err = node1.Execute(`INSERT INTO foo(date) VALUES(time("now"))`)
	if err != nil {
		t.Fatalf("failed to INSERT record: %s", err.Error())
	}
	r, err := node1.QueryStrongConsistency("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query for count: %s", err.Error())
	}
	if got, exp := r, `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}`; got != exp {
		t.Fatalf("wrong query results, exp %s, got %s", exp, got)
	}

	// Wait a little bit, to ensure time has moved on. This ensures that if rewriting failed
	// then actual different times would be written on each node.
	time.Sleep(2 * time.Second)

	// Join the second node to the first node, so that it picks up the log entries.
	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Check that row is *exactly* the same on each node. This could only happen if time() was
	// rewritten by the Leader before committing to the Raft log.
	r1, err := node1.QueryNoneConsistency("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query node 1: %s", err.Error())
	}
	r2, err := node2.QueryNoneConsistency("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query node 2: %s", err.Error())
	}
	if r1 != r2 {
		t.Fatalf("rows are different on nodes")
	}
}

// Test_MultiNodeClusterRETURNING tests operation of the RETURNING keyword.
func Test_MultiNodeClusterRETURNING(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c := Cluster{node1, node2}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	_, err = leader.Execute("CREATE TABLE foo (id integer not null primary key, name text)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	res, err := leader.Execute(`INSERT INTO foo(name) VALUES("fiona") RETURNING *`)
	if err != nil {
		t.Fatalf("failed to INSERT record: %s", err.Error())
	}
	if got, exp := res, `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`; got != exp {
		t.Fatalf("wrong execute results for RETURNING, exp %s, got %s", exp, got)
	}
	r, err := leader.Query("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query for count: %s", err.Error())
	}
	if got, exp := r, `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}`; got != exp {
		t.Fatalf("wrong query results, exp %s, got %s", exp, got)
	}

	// Send a few Noops through to ensure SQLite database has been updated on each node.
	for i := 0; i < 3; i++ {
		node1.Noop("some_id")
	}

	// Check that row is *exactly* the same on each node. This is to check that RETURNING
	// went through the Raft log and was applied to each node, even though it involves a
	// query at the low level.
	tFn := func() bool {
		r1, err := node1.QueryNoneConsistency("SELECT * FROM foo")
		if err != nil {
			t.Fatalf("failed to query node 1: %s", err.Error())
		}
		r2, err := node2.QueryNoneConsistency("SELECT * FROM foo")
		if err != nil {
			t.Fatalf("failed to query node 2: %s", err.Error())
		}
		return r1 == r2
	}
	trueOrTimeout(tFn, 10*time.Second)

	// Try a request with multiple statements.
	res, err = leader.ExecuteMulti([]string{
		`INSERT INTO foo(id, name) VALUES(2, "declan") RETURNING *`,
		`INSERT INTO foo(id, name) VALUES(3, "aoife")`})
	if err != nil {
		t.Fatalf("failed to INSERT record: %s", err.Error())
	}
	if got, exp := res, `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"declan"]]},{"last_insert_id":3,"rows_affected":1}]}`; got != exp {
		t.Fatalf("wrong execute-multi results for RETURNING, exp %s, got %s", exp, got)
	}
}

// Test_MultiNodeClusterBootstrap tests formation of a 3-node cluster via bootstrapping,
// and its operation.
func Test_MultiNodeClusterBootstrap(t *testing.T) {
	node1 := mustNewNode("node1", false)
	node1.Store.BootstrapExpect = 3
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	node2.Store.BootstrapExpect = 3
	defer node2.Deprovision()

	node3 := mustNewNode("node3", false)
	node3.Store.BootstrapExpect = 3
	defer node3.Deprovision()

	provider := cluster.NewAddressProviderString(
		[]string{node1.RaftAddr, node2.RaftAddr, node3.RaftAddr})
	node1Bs := cluster.NewBootstrapper(provider, node1.Client)
	node2Bs := cluster.NewBootstrapper(provider, node2.Client)
	node3Bs := cluster.NewBootstrapper(provider, node3.Client)

	// Have all nodes start a bootstrap basically in parallel,
	// ensure only 1 leader actually gets elected.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		done := func() bool {
			addr, _ := node1.Store.LeaderAddr()
			return addr != ""
		}
		node1Bs.Boot(context.Background(), node1.ID, node1.RaftAddr, cluster.Voter, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node2.Store.LeaderAddr()
			return addr != ""
		}
		node2Bs.Boot(context.Background(), node2.ID, node2.RaftAddr, cluster.Voter, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node3.Store.LeaderAddr()
			return addr != ""
		}
		node3Bs.Boot(context.Background(), node3.ID, node3.RaftAddr, cluster.Voter, done, 10*time.Second)
		wg.Done()
	}()
	wg.Wait()

	// Wait for leader election
	_, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for a leader: %s", err.Error())
	}

	c := Cluster{node1, node2, node3}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	// Ensure each node has the same leader!
	leaderAddr, err := leader.WaitForLeader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}
	for i, n := range []*Node{node1, node2, node3} {
		addr, err := n.WaitForLeader()
		if err != nil {
			t.Fatalf("failed waiting for a leader on node %d: %s", i, err.Error())
		}
		if exp, got := leaderAddr, addr; exp != got {
			t.Fatalf("node %d has wrong leader, exp %s, got %s", i, exp, got)
		}
	}

	// Run queries against cluster.
	tests := []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = leader.Execute(tt.stmt)
		} else {
			r, err = leader.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}

	// Kill the leader and wait for the new leader.
	leader.Deprovision()
	c = c.RemoveNode(leader)
	leader, err = c.WaitForNewLeader(leader)
	if err != nil {
		t.Fatalf("failed to find new cluster leader after killing leader: %s", err.Error())
	}

	// Run queries against the now 2-node cluster.
	tests = []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{"error":"table foo already exists"}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("sinead")`,
			expected: `{"results":[{"last_insert_id":2,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"sinead"]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = leader.Execute(tt.stmt)
		} else {
			r, err = leader.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

// Test_MultiNodeClusterBootstrapLaterJoin tests formation of a 3-node cluster and
// then checking a 4th node can join later with the bootstrap parameters.
func Test_MultiNodeClusterBootstrapLaterJoin(t *testing.T) {
	node1 := mustNewNode("node1", false)
	node1.Store.BootstrapExpect = 3
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	node2.Store.BootstrapExpect = 3
	defer node2.Deprovision()

	node3 := mustNewNode("node3", false)
	node3.Store.BootstrapExpect = 3
	defer node3.Deprovision()

	provider := cluster.NewAddressProviderString(
		[]string{node1.RaftAddr, node2.RaftAddr, node3.RaftAddr})
	node1Bs := cluster.NewBootstrapper(provider, node1.Client)
	node1Bs.Interval = time.Second
	node2Bs := cluster.NewBootstrapper(provider, node2.Client)
	node2Bs.Interval = time.Second
	node3Bs := cluster.NewBootstrapper(provider, node3.Client)
	node3Bs.Interval = time.Second

	// Have all nodes start a bootstrap basically in parallel,
	// ensure only 1 leader actually gets elected.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		done := func() bool {
			addr, _ := node1.Store.LeaderAddr()
			return addr != ""
		}
		node1Bs.Boot(context.Background(), node1.ID, node1.RaftAddr, cluster.Voter, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node2.Store.LeaderAddr()
			return addr != ""
		}
		node2Bs.Boot(context.Background(), node2.ID, node2.RaftAddr, cluster.Voter, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node3.Store.LeaderAddr()
			return addr != ""
		}
		node3Bs.Boot(context.Background(), node3.ID, node3.RaftAddr, cluster.Voter, done, 10*time.Second)
		wg.Done()
	}()
	wg.Wait()

	// Check leaders
	node1Leader, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for a leader: %s", err.Error())
	}
	node2Leader, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for a leader: %s", err.Error())
	}
	node3Leader, err := node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for a leader: %s", err.Error())
	}

	if got, exp := node2Leader, node1Leader; got != exp {
		t.Fatalf("leader mismatch between node 1 and node 2, got %s, exp %s", got, exp)
	}
	if got, exp := node3Leader, node1Leader; got != exp {
		t.Fatalf("leader mismatch between node 1 and node 3, got %s, exp %s", got, exp)
	}

	// Ensure a 4th node can join cluster with exactly same launch
	// params. Under the cover it should just do a join.
	node4 := mustNewNode("node4", false)
	node4.Store.BootstrapExpect = 3
	defer node4.Deprovision()
	node4Bs := cluster.NewBootstrapper(provider, node4.Client)
	node4Bs.Interval = time.Second
	done := func() bool {
		addr, _ := node4.Store.LeaderAddr()
		return addr != ""
	}
	if err := node4Bs.Boot(context.Background(), node4.ID, node4.RaftAddr, cluster.Voter, done, 10*time.Second); err != nil {
		t.Fatalf("node 4 failed to boot")
	}
	node4Leader, err := node4.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for a leader: %s", err.Error())
	}
	if got, exp := node4Leader, node1Leader; got != exp {
		t.Fatalf("leader mismatch between node 4 and node 1, got %s, exp %s", got, exp)
	}
}

// Test_MultiNodeClusterBootstrapLaterJoinTLS tests formation of a 3-node cluster which
// uses HTTP and TLS,then checking a 4th node can join later with the bootstrap parameters.
func Test_MultiNodeClusterBootstrapLaterJoinTLS(t *testing.T) {
	node1 := mustNewNodeEncrypted("node1", false, true, true)
	node1.Store.BootstrapExpect = 3
	node1.EnableTLSClient()
	defer node1.Deprovision()

	node2 := mustNewNodeEncrypted("node2", false, true, true)
	node2.Store.BootstrapExpect = 3
	node2.EnableTLSClient()
	defer node2.Deprovision()

	node3 := mustNewNodeEncrypted("node3", false, true, true)
	node3.Store.BootstrapExpect = 3
	node3.EnableTLSClient()
	defer node3.Deprovision()

	provider := cluster.NewAddressProviderString(
		[]string{node1.RaftAddr, node2.RaftAddr, node3.RaftAddr})
	node1Bs := cluster.NewBootstrapper(provider, node1.Client)
	node1Bs.Interval = time.Second
	node2Bs := cluster.NewBootstrapper(provider, node2.Client)
	node2Bs.Interval = time.Second
	node3Bs := cluster.NewBootstrapper(provider, node3.Client)
	node3Bs.Interval = time.Second

	// Have all nodes start a bootstrap basically in parallel,
	// ensure only 1 leader actually gets elected.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		done := func() bool {
			addr, _ := node1.Store.LeaderAddr()
			return addr != ""
		}
		node1Bs.Boot(context.Background(), node1.ID, node1.RaftAddr, cluster.Voter, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node2.Store.LeaderAddr()
			return addr != ""
		}
		node2Bs.Boot(context.Background(), node2.ID, node2.RaftAddr, cluster.Voter, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node3.Store.LeaderAddr()
			return addr != ""
		}
		node3Bs.Boot(context.Background(), node3.ID, node3.RaftAddr, cluster.Voter, done, 10*time.Second)
		wg.Done()
	}()
	wg.Wait()

	// Check leaders
	node1Leader, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for a leader: %s", err.Error())
	}
	node2Leader, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for a leader: %s", err.Error())
	}
	node3Leader, err := node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for a leader: %s", err.Error())
	}

	if got, exp := node2Leader, node1Leader; got != exp {
		t.Fatalf("leader mismatch between node 1 and node 2, got %s, exp %s", got, exp)
	}
	if got, exp := node3Leader, node1Leader; got != exp {
		t.Fatalf("leader mismatch between node 1 and node 3, got %s, exp %s", got, exp)
	}

	// Ensure a 4th node can join cluster with exactly same launch
	// params. Under the covers it should just do a join.
	node4 := mustNewNodeEncrypted("node4", false, true, true)
	node4.Store.BootstrapExpect = 3
	node4.EnableTLSClient()
	defer node3.Deprovision()
	node4Bs := cluster.NewBootstrapper(provider, node3.Client)
	node4Bs.Interval = time.Second
	done := func() bool {
		addr, _ := node4.Store.LeaderAddr()
		return addr != ""
	}
	if err := node4Bs.Boot(context.Background(), node4.ID, node4.RaftAddr, cluster.Voter, done, 10*time.Second); err != nil {
		t.Fatalf("node 4 failed to boot")
	}
	node4Leader, err := node4.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for a leader: %s", err.Error())
	}
	if got, exp := node4Leader, node1Leader; got != exp {
		t.Fatalf("leader mismatch between node 4 and node 1, got %s, exp %s", got, exp)
	}
}

// Test_MultiNodeClusterRaftAdv tests 3-node cluster with advertised Raft addresses usage.
func Test_MultiNodeClusterRaftAdv(t *testing.T) {
	ln1 := mustTCPListener("0.0.0.0:0")
	defer ln1.Close()
	ln2 := mustTCPListener("0.0.0.0:0")
	defer ln2.Close()

	advAddr := mustGetLocalIPv4Address()

	_, port1, err := net.SplitHostPort(ln1.Addr().String())
	if err != nil {
		t.Fatalf("failed to get host and port: %s", err.Error())
	}
	_, port2, err := net.SplitHostPort(ln2.Addr().String())
	if err != nil {
		t.Fatalf("failed to get host and port: %s", err.Error())
	}

	advAddr1, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(advAddr, port1))
	if err != nil {
		t.Fatalf("failed to resolve TCP address: %s", err.Error())
	}
	advAddr2, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(advAddr, port2))
	if err != nil {
		t.Fatalf("failed to resolve TCP address: %s", err.Error())
	}

	mux1, err := tcp.NewMux(ln1, advAddr1)
	if err != nil {
		t.Fatalf("failed to create node-to-node mux: %s", err.Error())
	}
	go mux1.Serve()
	mux2, err := tcp.NewMux(ln2, advAddr2)
	if err != nil {
		t.Fatalf("failed to create node-to-node mux: %s", err.Error())
	}
	go mux2.Serve()

	raftDialer := tcp.NewDialer(cluster.MuxRaftHeader, nil)
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)

	// Start two nodes, and ensure a cluster can be formed.
	node1 := mustNodeEncrypted("node1", mustTempDir("node1"), true, false, mux1, raftDialer, clstrDialer)
	defer node1.Deprovision()
	leader, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader on node1: %s", err.Error())
	}
	if exp, got := advAddr1.String(), leader; exp != got {
		t.Fatalf("node return wrong leader from leader, exp: %s, got %s", exp, got)
	}

	node2 := mustNodeEncrypted("node2", mustTempDir("node2"), false, false, mux2, raftDialer, clstrDialer)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node2 failed to join leader: %s", err.Error())
	}
	leader, err = node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader on node2: %s", err.Error())
	}
	if exp, got := advAddr1.String(), leader; exp != got {
		t.Fatalf("node return wrong leader from follower, exp: %s, got %s", exp, got)
	}
}

// Test_MultiNodeClusterNodes checks nodes/ endpoint under various situations.
func Test_MultiNodeClusterNodes(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c := Cluster{node1, node2}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	node3 := mustNewNode("node3", false)
	defer node3.Deprovision()
	if err := node3.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c = Cluster{node1, node2, node3}
	leader, err = c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	// Get nodes/ status from a node
	nodes, err := node1.Nodes(false)
	if err != nil {
		t.Fatalf("failed to get nodes status: %s", err.Error())
	}
	if len(nodes) != len(c) {
		t.Fatalf("nodes/ output returned wrong number of nodes, got %d, exp %d", len(nodes), len(c))
	}
	ns := nodes.GetNode(leader.ID)
	if ns == nil {
		t.Fatalf("failed to find leader with ID %s in node status", leader.ID)
	}
	if !ns.Leader {
		t.Fatalf("node is not leader")
	}
	if ns.Addr != leader.RaftAddr {
		t.Fatalf("node has wrong Raft address for leader")
	}
	leaderAPIAddr := fmt.Sprintf("http://%s", leader.APIAddr)
	if ns.APIAddr != leaderAPIAddr {
		t.Fatalf("node has wrong API address for leader, got %s, exp %s", ns.APIAddr, leaderAPIAddr)
	}
	if !ns.Reachable {
		t.Fatalf("node is not reachable")
	}

	// Get a follower and confirm nodes/ looks good.
	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to get followers: %s", err.Error())
	}
	if len(followers) != 2 {
		t.Fatalf("got incorrect number of followers: %d", len(followers))
	}
	f := followers[0]
	ns = nodes.GetNode(f.ID)
	if ns == nil {
		t.Fatalf("failed to find follower with ID %s in node status", f.ID)
	}
	if ns.Addr != f.RaftAddr {
		t.Fatalf("node has wrong Raft address for follower")
	}
	if ns.APIAddr != fmt.Sprintf("http://%s", f.APIAddr) {
		t.Fatalf("node has wrong API address for follower")
	}
	if ns.Leader {
		t.Fatalf("node is not a follower")
	}
	if !ns.Reachable {
		t.Fatalf("node is not reachable")
	}
}

// Test_MultiNodeClusterQueuedWrites tests writing to a cluster using
// normal and queued writes.
func Test_MultiNodeClusterQueuedWrites(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	if _, err := node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Join a second and third nodes
	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	node3 := mustNewNode("node3", false)
	defer node3.Deprovision()
	if err := node3.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Write data to the cluster, via various methods and nodes.
	writesPerLoop := 500
	var wg sync.WaitGroup
	numLoops := 5
	wg.Add(numLoops)
	go func() {
		defer wg.Done()
		for i := 0; i < writesPerLoop; i++ {
			if _, err := node1.Execute(`INSERT INTO foo(name) VALUES("fiona")`); err != nil {
				t.Errorf("failed to insert records: %s", err.Error())
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < writesPerLoop; i++ {
			if _, err := node2.Execute(`INSERT INTO foo(name) VALUES("fiona")`); err != nil {
				t.Errorf("failed to insert records: %s", err.Error())
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < writesPerLoop-1; i++ {
			if _, err := node2.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, false); err != nil {
				t.Errorf("failed to insert records: %s\n", err.Error())
			}
		}
		if _, err := node2.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, true); err != nil {
			t.Errorf("failed to insert records: %s\n", err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < writesPerLoop-1; i++ {
			if _, err := node3.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, false); err != nil {
				t.Errorf("failed to insert records: %s\n", err.Error())
			}
		}
		if _, err := node3.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, true); err != nil {
			t.Errorf("failed to insert records: %s\n", err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < writesPerLoop-1; i++ {
			if _, err := node3.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, false); err != nil {
				t.Errorf("failed to insert records: %s\n", err.Error())
			}
		}
		if _, err := node3.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, true); err != nil {
			t.Errorf("failed to insert records: %s", err.Error())
		}
	}()
	wg.Wait()

	exp := fmt.Sprintf(`{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[%d]]}]}`, numLoops*writesPerLoop)
	got, err := node1.Query(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if got != exp {
		t.Fatalf("incorrect count, got %s, exp %s", got, exp)
	}
}

// Test_MultiNodeClusterLargeQueuedWrites tests writing to a cluster using
// many large concurrent Queued Writes operations.
func Test_MultiNodeClusterLargeQueuedWrites(t *testing.T) {
	store.ResetStats()
	db.ResetStats()
	http.ResetStats()
	queue.ResetStats()

	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	if _, err := node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Join a second and third nodes
	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	node3 := mustNewNode("node3", false)
	defer node3.Deprovision()
	if err := node3.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Write data to the cluster, via various nodes.
	nodesUnderTest := []*Node{node3, node1, node2, node1, node2, node3, node1, node3, node2}
	writesPerNode := 1000
	writeBandBase := writesPerNode * 10

	var wg sync.WaitGroup
	wg.Add(len(nodesUnderTest))
	for i, n := range nodesUnderTest {
		go func(ii int, nt *Node) {
			defer wg.Done()
			var j int
			for j = 0; j < writesPerNode-1; j++ {
				stmt := fmt.Sprintf(`INSERT OR IGNORE INTO foo(id, name) VALUES(%d, "fiona")`, j+(ii*writeBandBase))
				if _, err := nt.ExecuteQueued(stmt, false); err != nil {
					t.Logf("failed to insert records: %s", err.Error())
				}
			}
			stmt := fmt.Sprintf(`INSERT OR IGNORE INTO foo(id, name) VALUES(%d, "fiona")`, j+(ii*writeBandBase))
			if _, err := nt.ExecuteQueued(stmt, true); err != nil {
				t.Logf("failed to insert records: %s", err.Error())
			}
		}(i, n)
	}
	wg.Wait()

	exp := fmt.Sprintf(`{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[%d]]}]}`, len(nodesUnderTest)*writesPerNode)
	got, err := node1.Query(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if got != exp {
		t.Fatalf("incorrect count, got %s, exp %s", got, exp)
	}
}

// Test_MultiNodeClusterNodesNonVoter checks nodes/ endpoint with a non-voting node.
func Test_MultiNodeClusterNodesNonVoter(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c := Cluster{node1, node2}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	nonVoter := mustNewNode("nonvoter", false)
	defer nonVoter.Deprovision()
	if err := nonVoter.JoinAsNonVoter(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = nonVoter.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Check that the voter statuses are correct
	checkVoterStatus := func(node *Node, exp bool) {
		v, err := node.IsVoter()
		if err != nil {
			t.Fatalf("failed to get voter status: %s", err.Error())
		}
		if v != exp {
			t.Fatalf("incorrect voter status, got %v, exp %v", v, exp)
		}
	}
	checkVoterStatus(leader, true)
	checkVoterStatus(nonVoter, false)

	// Get the new leader, in case it changed.
	c = Cluster{node1, node2, nonVoter}
	_, err = c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	// Get nodes/ status from a node
	nodes, err := node1.Nodes(false)
	if err != nil {
		t.Fatalf("failed to get nodes status: %s", err.Error())
	}
	if len(nodes) != len(c)-1 {
		t.Fatalf("nodes/ output returned wrong number of nodes, got %d, exp %d", len(nodes), len(c))
	}

	nodes, err = node1.Nodes(true)
	if err != nil {
		t.Fatalf("failed to get nodes status including non-voters: %s", err.Error())
	}
	if len(nodes) != len(c) {
		t.Fatalf("nodes/ output returned wrong number of nodes, got %d, exp %d", len(nodes), len(c))
	}
}

// Test_MultiNodeClusterNodeEncrypted tests formation of a 3-node cluster, and its operation.
// This test enables inter-node encryption, but keeps the unencrypted HTTP API.
func Test_MultiNodeClusterNodeEncrypted(t *testing.T) {
	node1 := mustNewNodeEncrypted("node1", true, false, true)
	node1.EnableTLSClient()
	defer node1.Deprovision()
	if _, err := node1.WaitForLeader(); err != nil {
		t.Fatalf("node never became leader")
	}

	node2 := mustNewNodeEncrypted("node2", false, false, true)
	node2.EnableTLSClient()
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c := Cluster{node1, node2}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	// Check the followers
	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to get followers: %s", err.Error())
	}
	if len(followers) != 1 {
		t.Fatalf("got incorrect number of followers: %d", len(followers))
	}

	node3 := mustNewNodeEncrypted("node3", false, false, true)
	node3.EnableTLSClient()
	defer node3.Deprovision()
	if err := node3.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c = Cluster{node1, node2, node3}
	leader, err = c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	// Check the followers
	followers, err = c.Followers()
	if err != nil {
		t.Fatalf("failed to get followers: %s", err.Error())
	}
	if len(followers) != 2 {
		t.Fatalf("got incorrect number of followers: %d", len(followers))
	}

	// Run queries against cluster.
	tests := []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = leader.Execute(tt.stmt)
		} else {
			r, err = leader.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}

	// Kill the leader and wait for the new leader.
	leader.Deprovision()
	c = c.RemoveNode(leader)
	leader, err = c.WaitForNewLeader(leader)
	if err != nil {
		t.Fatalf("failed to find new cluster leader after killing leader: %s", err.Error())
	}

	// Run queries against the now 2-node cluster.
	tests = []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{"error":"table foo already exists"}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("sinead")`,
			expected: `{"results":[{"last_insert_id":2,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"sinead"]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = leader.Execute(tt.stmt)
		} else {
			r, err = leader.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

// Test_MultiNodeClusterSnapshot tests formation of a 3-node cluster, which involves sharing snapshots.
func Test_MultiNodeClusterSnapshot(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	if _, err := node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Force snapshots and log truncation to occur. Make the number of snapshots high enough
	// that some logs are deleted from the log and are not present in trailing logs either.
	// This way we can be sure that the follower will need to get an actual snapshot from the leader.
	for i := 0; i < 5*int(node1.Store.SnapshotThreshold); i++ {
		_, err := node1.Execute(`INSERT INTO foo(name) VALUES("sinead")`)
		if err != nil {
			t.Fatalf(`failed to write records for Snapshot test: %s`, err.Error())
		}
	}

	expResults := `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[500]]}]}`
	testerFn := func(n *Node) {
		t.Helper()
		c := 0
		for {
			r, err := n.QueryNoneConsistency(`SELECT COUNT(*) FROM foo`)
			if err != nil {
				t.Fatalf("failed to query follower node: %s", err.Error())
			}

			if r != expResults {
				if c < 10 {
					// Wait, and try again.
					sleepForSecond()
					c++
					continue
				}
				t.Fatalf("timed out waiting for snapshot state")
			}
			// The node passed!
			break
		}
	}

	// Join a second node, check it gets the data via a snapshot.
	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	testerFn(node2)

	// Create and add a third node to the cluster.
	node3 := mustNewNode("node3", false)
	defer node3.Deprovision()
	if err := node3.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	c := Cluster{node1, node2, node3}

	// Wait for followers to pick up state.
	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to determine followers: %s", err.Error())
	}

	// Check that all followers have the correct state.
	for _, f := range followers {
		testerFn(f)
	}

	// Kill original node.
	node1.Deprovision()
	c = c.RemoveNode(node1)
	var leader *Node
	leader, err = c.WaitForNewLeader(node1)
	if err != nil {
		t.Fatalf("failed to find new cluster leader after killing leader: %s", err.Error())
	}

	// Test that the new leader still has the full state.
	testerFn(leader)
}

func Test_MultiNodeCluster_Backup(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Create a table and write a record
	if _, err := node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if _, err := node1.Execute(`INSERT INTO foo(name) VALUES("fiona")`); err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	// Get a backup from the leader via the follower.
	c := Cluster{node1, node2}
	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to get followers: %s", err.Error())
	}

	backupFile := mustTempFile()
	defer os.Remove(backupFile)
	if err := followers[0].Backup(backupFile, false, ""); err != nil {
		t.Fatalf("failed to get backup from follower: %s", err.Error())
	}

	// Create brand new node, and restore backup into it.
	newNode := mustNewLeaderNode("newNode")
	defer newNode.Deprovision()
	_, err = newNode.Boot(backupFile)
	if err != nil {
		t.Fatalf("failed to boot using backup: %s", err.Error())
	}

	// Check that the new node has the correct state.
	exp := `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`
	got, err := newNode.Query(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query new node: %s", err.Error())
	}
	if got != exp {
		t.Fatalf("incorrect count, got %s, exp %s", got, exp)
	}
}

func Test_MultiNodeCluster_Backup_SQL(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Create a table and write a record
	if _, err := node1.Execute(`CREATE TABLE foo (name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Get a backup from the leader via the follower.
	c := Cluster{node1, node2}
	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to get followers: %s", err.Error())
	}

	backupFile := mustTempFile()
	defer os.Remove(backupFile)
	if err := followers[0].Backup(backupFile, false, "sql"); err != nil {
		t.Fatalf("failed to get backup from follower: %s", err.Error())
	}

	// Check the backup contents
	sql, err := os.ReadFile(backupFile)
	if err != nil {
		t.Fatalf(`reading backup file failed: %s`, err.Error())
	}
	schema := `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE foo (name text);
COMMIT;
`
	if exp, got := schema, string(sql); exp != got {
		t.Fatalf(`contents of backup file are incorrect exp: %s, got %s`, exp, got)
	}
}

func Test_MultiNodeCluster_FollowerLoad_SQL(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get a follower, make sure Load works via it.
	c := Cluster{node1, node2}
	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to get followers: %s", err.Error())
	}
	if _, err := followers[0].Load(filepath.Join("testdata", "auto-restore.sql")); err != nil {
		t.Fatalf("failed to load via follower: %s", err.Error())
	}

	r, err := node1.QueryStrongConsistency("SELECT * FROM foo WHERE id=2")
	if err != nil {
		t.Fatalf("failed to execute query: %s", err.Error())
	}
	if r != `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"fiona"]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
}

// Test_MultiNodeClusterWithNonVoter tests formation of a 4-node cluster, one of which is
// a non-voter. This test also checks that if the Leader changes the non-voter is still in
// the cluster and gets updates from the new leader.
func Test_MultiNodeClusterWithNonVoter(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c := Cluster{node1, node2}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	node3 := mustNewNode("node3", false)
	defer node3.Deprovision()
	if err := node3.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c = Cluster{node1, node2, node3}
	leader, err = c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	nonVoter := mustNewNode("nonvoter", false)
	defer nonVoter.Deprovision()
	if err := nonVoter.JoinAsNonVoter(leader); err != nil {
		t.Fatalf("non-voting node failed to join leader: %s", err.Error())
	}
	_, err = nonVoter.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	c = Cluster{node1, node2, node3, nonVoter}

	// Run queries against cluster.
	tests := []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = leader.Execute(tt.stmt)
		} else {
			r, err = leader.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}

	// Kill the leader and wait for the new leader.
	leader.Deprovision()
	c = c.RemoveNode(leader)
	leader, err = c.WaitForNewLeader(leader)
	if err != nil {
		t.Fatalf("failed to find new cluster leader after killing leader: %s", err.Error())
	}

	// Run queries against the now 3-node cluster, ensuring the non-voter is still in the cluster
	// and getting updates from the new leader.
	tests = []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{"error":"table foo already exists"}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("sinead")`,
			expected: `{"results":[{"last_insert_id":2,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"sinead"]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = leader.Execute(tt.stmt)
			if err != nil {
				t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
			}
		} else {
			// Ensure leader and non-voter both have latest writes.
			for _, node := range []*Node{leader, nonVoter} {
				testPoll(t, func() (bool, error) {
					r, err = node.QueryNoneConsistency(`SELECT * FROM foo`)
					if err != nil {
						return false, err
					}
					return r == tt.expected, nil
				}, 50*time.Millisecond, 5*time.Second)
			}
		}
	}
}

// Test_MultiNodeCluster_DisconnectedNonVoter tests that "auto" read consistency works
// as expected when a non-voter is disconnected from the cluster.
func Test_MultiNodeCluster_DisconnectedNonVoter(t *testing.T) {
	leader := mustNewLeaderNode("leader")
	defer leader.Deprovision()

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	nonVoter := mustNewNode("nonvoter", false)
	defer nonVoter.Deprovision()
	if err := nonVoter.JoinAsNonVoter(leader); err != nil {
		t.Fatalf("non-voting node failed to join leader: %s", err.Error())
	}
	_, err = nonVoter.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Run queries against cluster.
	tests := []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = leader.Execute(tt.stmt)
		} else {
			r, err = leader.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}

	// Send a few Noops through to ensure SQLite database has been updated on each node.
	for i := 0; i < 3; i++ {
		leader.Noop("some_id")
	}

	// Confirm querying through the follower works fine.
	r, err := node2.Query(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if exp, got := `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`, r; exp != got {
		t.Fatalf("incorrect count, got %s, exp %s", got, exp)
	}
	r, err = node2.QueryNoneConsistency(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if exp, got := `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`, r; exp != got {
		t.Fatalf("incorrect count, got %s, exp %s", got, exp)
	}

	// Kill the leader leaving the cluster without a leader (no quorum), and the non-voting node
	// effectively disconnected.
	leader.Deprovision()
	time.Sleep(3 * time.Second) // Wait for the leader to terminate.

	// Follower node should attempt to forward to leader, but will fail.
	_, err = node2.Query(`SELECT * FROM foo`)
	if err == nil {
		t.Fatalf("expected error querying follower node after Leader deprovision, got nil")
	}
	_, err = node2.QueryAutoConsistency(`SELECT * FROM foo`)
	if err == nil {
		t.Fatalf("expected error querying follower node after Leader deprovision, got nil")
	}
	_, err = node2.QueryNoneConsistency(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("error querying follower node after Leader deprovision with None consistency: %s", err.Error())
	}

	// Non-voting node with None and Auto should work fine.
	testPoll(t, func() (bool, error) {
		r, err := nonVoter.QueryNoneConsistency(`SELECT * FROM foo`)
		if err != nil {
			return false, err
		}
		return r == tests[2].expected, nil
	}, 50*time.Millisecond, 5*time.Second)
	testPoll(t, func() (bool, error) {
		r, err := nonVoter.QueryAutoConsistency(`SELECT * FROM foo`)
		if err != nil {
			return false, err
		}
		return r == tests[2].expected, nil
	}, 50*time.Millisecond, 5*time.Second)

	// Default query level -- which is weak -- should fail.
	_, err = nonVoter.Query(`SELECT * FROM foo`)
	if err == nil {
		t.Fatalf("expected error querying non-voter node with Weak, got nil")
	}
}

// Test_MultiNodeClusterRecoverSingle tests recovery of a single node from a 3-node cluster,
// which no longer has quorum.
func Test_MultiNodeClusterRecoverSingle(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	if _, err := node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if _, err := node1.Execute(`INSERT INTO foo(id, name) VALUES(1, "fiona")`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if rows, _ := node1.Query(`SELECT COUNT(*) FROM foo`); rows != `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}` {
		t.Fatalf("got incorrect results from node: %s", rows)
	}

	// Join a second and third nodes, which will get database state via snapshots.
	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	if rows, _ := node2.Query(`SELECT COUNT(*) FROM foo`); rows != `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}` {
		t.Fatalf("got incorrect results from node: %s", rows)
	}

	node3 := mustNewNode("node3", false)
	defer node3.Deprovision()
	if err := node3.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	if rows, _ := node3.Query(`SELECT COUNT(*) FROM foo`); rows != `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}` {
		t.Fatalf("got incorrect results from node: %s", rows)
	}

	// Shutdown all nodes
	if err := node1.Close(true); err != nil {
		t.Fatalf("failed to close node1: %s", err.Error())
	}
	if err := node2.Close(true); err != nil {
		t.Fatalf("failed to close node2: %s", err.Error())
	}
	if err := node3.Close(true); err != nil {
		t.Fatalf("failed to close node3: %s", err.Error())
	}

	raftDialer := tcp.NewDialer(cluster.MuxRaftHeader, nil)
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)

	// Create a single node using the node's data directory. It should fail because
	// quorum can't be met. This isn't quite right since the Raft address is also
	// changing, but it generally proves it doesn't come up.
	mux0, ln0 := mustNewOpenMux("127.0.0.1:10000")
	failedSingle := mustNodeEncrypted(node1.Store.ID(), node1.Dir, false, false, mux0, raftDialer, clstrDialer)
	_, err = failedSingle.WaitForLeader()
	if err == nil {
		t.Fatalf("no error waiting for leader")
	}
	failedSingle.Close(true)
	ln0.Close()

	// Try again, this time injecting a single-node peers file.
	mux1, ln1 := mustNewOpenMux("127.0.0.1:10001")
	peers := fmt.Sprintf(`[{"id": "%s","address": "%s"}]`, node1.Store.ID(), "127.0.0.1:10001")
	mustWriteFile(node1.PeersPath, peers)

	okSingle := mustNodeEncrypted(node1.Store.ID(), node1.Dir, false, false, mux1, raftDialer, clstrDialer)
	_, err = okSingle.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	if rows, _ := okSingle.Query(`SELECT COUNT(*) FROM foo`); rows != `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}` {
		t.Fatalf("got incorrect results from recovered node: %s", rows)
	}
	okSingle.Close(true)
	ln1.Close()
}

// Test_MultiNodeClusterRecoverFull tests recovery of a full 3-node cluster,
// each node coming up with a different Raft address.
func Test_MultiNodeClusterRecoverFull(t *testing.T) {
	var err error

	raftDialer := tcp.NewDialer(cluster.MuxRaftHeader, nil)
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)

	mux1, ln1 := mustNewOpenMux("127.0.0.1:10001")
	node1 := mustNodeEncrypted("node1", mustTempDir("node1"), true, false, mux1, raftDialer, clstrDialer)
	_, err = node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	mux2, ln2 := mustNewOpenMux("127.0.0.1:10002")
	node2 := mustNodeEncrypted("node2", mustTempDir("node2"), false, false, mux2, raftDialer, clstrDialer)
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	mux3, ln3 := mustNewOpenMux("127.0.0.1:10003")
	node3 := mustNodeEncrypted("node3", mustTempDir("node3"), false, false, mux3, raftDialer, clstrDialer)
	if err := node3.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	if _, err := node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if _, err := node1.Execute(`INSERT INTO foo(id, name) VALUES(1, "fiona")`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	rows, err := node1.Query(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query node: %s", err.Error())
	}
	if got, exp := rows, `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}`; got != exp {
		t.Fatalf("got incorrect results from node exp: %s got: %s", exp, got)
	}

	// Shutdown all nodes
	if err := node1.Close(true); err != nil {
		t.Fatalf("failed to close node1: %s", err.Error())
	}
	ln1.Close()
	if err := node2.Close(true); err != nil {
		t.Fatalf("failed to close node2: %s", err.Error())
	}
	ln2.Close()
	if err := node3.Close(true); err != nil {
		t.Fatalf("failed to close node3: %s", err.Error())
	}
	ln3.Close()

	// Restart cluster, each node with different Raft addresses.
	peers := fmt.Sprintf(`[{"id": "%s","address": "%s"}, {"id": "%s","address": "%s"}, {"id": "%s","address": "%s"}]`,
		"1", "127.0.0.1:11001",
		"2", "127.0.0.1:11002",
		"3", "127.0.0.1:11003",
	)
	mustWriteFile(node1.PeersPath, peers)
	mustWriteFile(node2.PeersPath, peers)
	mustWriteFile(node3.PeersPath, peers)

	mux4, ln4 := mustNewOpenMux("127.0.0.1:11001")
	node4 := mustNodeEncrypted("1", node1.Dir, false, false, mux4, raftDialer, clstrDialer)
	defer node4.Deprovision()
	defer ln4.Close()

	mux5, ln5 := mustNewOpenMux("127.0.0.1:11002")
	node5 := mustNodeEncrypted("2", node2.Dir, false, false, mux5, raftDialer, clstrDialer)
	defer node5.Deprovision()
	defer ln5.Close()

	mux6, ln6 := mustNewOpenMux("127.0.0.1:11003")
	node6 := mustNodeEncrypted("3", node3.Dir, false, false, mux6, raftDialer, clstrDialer)
	defer node6.Deprovision()
	defer ln6.Close()

	for _, node := range []*Node{node4, node5, node6} {
		_, err = node.WaitForLeader()
		if err != nil {
			t.Fatalf("failed waiting for leader on node %s (recovered cluster): %s", node.ID, err.Error())
		}
	}

	rows, err = node4.Query(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query recovered node: %s", err.Error())
	}
	if got, exp := rows, `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}`; got != exp {
		t.Fatalf("got incorrect results from recovered node exp: %s got: %s", exp, got)
	}
}

// Test_MultiNodeClusterReapNodes tests that unreachable nodes are reaped.
func Test_MultiNodeClusterReapNodes(t *testing.T) {
	cfgStoreFn := func(n *Node) {
		n.Store.ReapTimeout = time.Second
		n.Store.ReapReadOnlyTimeout = time.Second
	}

	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()
	cfgStoreFn(node1)
	_, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	cfgStoreFn(node2)
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c := Cluster{node1, node2}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	node3 := mustNewNode("node3", false)
	defer node3.Deprovision()
	cfgStoreFn(node3)
	if err := node3.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c = Cluster{node1, node2, node3}
	leader, err = c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	nonVoter := mustNewNode("nonvoter", false)
	defer nonVoter.Deprovision()
	cfgStoreFn(nonVoter)
	if err := nonVoter.JoinAsNonVoter(leader); err != nil {
		t.Fatalf("non-voting node failed to join leader: %s", err.Error())
	}
	_, err = nonVoter.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Confirm non-voter node is in the cluster config.
	nodes, err := leader.Nodes(true)
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}
	if !nodes.HasAddr(nonVoter.RaftAddr) {
		t.Fatalf("nodes do not contain non-voter node")
	}

	// Kill non-voter node, confirm it's removed.
	nonVoter.Deprovision()
	tFn := func() bool {
		nodes, _ = leader.Nodes(true)
		return !nodes.HasAddr(nonVoter.RaftAddr)
	}
	if !trueOrTimeout(tFn, 20*time.Second) {
		t.Fatalf("timed out waiting for non-voting node to be reaped")
	}

	// Confirm voting node is in the cluster config.
	nodes, err = leader.Nodes(true)
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}
	if !nodes.HasAddr(node3.RaftAddr) {
		t.Fatalf("nodes do not contain non-voter node")
	}

	// Kill voting node, confirm it's removed.
	node3.Deprovision()
	tFn = func() bool {
		nodes, _ = leader.Nodes(true)
		return !nodes.HasAddr(node3.RaftAddr)
	}

	if !trueOrTimeout(tFn, 20*time.Second) {
		t.Fatalf("timed out waiting for voting node to be reaped")
	}
}

// Test_MultiNodeClusterNoReap tests that a node is not reaped before
// its time.
func Test_MultiNodeClusterNoReap(t *testing.T) {
	cfgStoreFn := func(n *Node) {
		n.Store.ReapReadOnlyTimeout = 120 * time.Second
	}

	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()
	cfgStoreFn(node1)
	_, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	nonVoter := mustNewNode("nonvoter", false)
	defer nonVoter.Deprovision()
	cfgStoreFn(nonVoter)
	if err := nonVoter.JoinAsNonVoter(node1); err != nil {
		t.Fatalf("non-voting node failed to join leader: %s", err.Error())
	}
	_, err = nonVoter.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Confirm non-voter node is in the cluster config.
	nodes, err := node1.Nodes(true)
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}
	if !nodes.HasAddr(nonVoter.RaftAddr) {
		t.Fatalf("nodes do not contain non-voter node")
	}

	// Kill non-voter node, confirm it's not removed.
	nonVoter.Deprovision()
	tFn := func() bool {
		nodes, _ = node1.Nodes(true)
		return !nodes.HasAddr(nonVoter.RaftAddr)
	}
	if trueOrTimeout(tFn, 20*time.Second) {
		t.Fatalf("didn't time out waiting for node to be removed")
	}
}

// Test_MultiNodeClusterNoReapZero tests that unreachable nodes are reaped.
func Test_MultiNodeClusterNoReapZero(t *testing.T) {
	cfgStoreFn := func(n *Node) {
		n.Store.ReapTimeout = 0
	}

	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()
	cfgStoreFn(node1)
	_, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	cfgStoreFn(node2)
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c := Cluster{node1, node2}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	node3 := mustNewNode("node3", false)
	defer node3.Deprovision()
	cfgStoreFn(node3)
	if err := node3.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c = Cluster{node1, node2, node3}
	leader, err = c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	// Confirm voting node is in the cluster config.
	nodes, err := leader.Nodes(true)
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}
	if !nodes.HasAddr(node3.RaftAddr) {
		t.Fatalf("nodes do not contain non-voter node")
	}

	// Kill voting node, confirm it's not reaped.
	node3.Deprovision()
	tFn := func() bool {
		nodes, _ = leader.Nodes(true)
		return !nodes.HasAddr(node3.RaftAddr)
	}

	if trueOrTimeout(tFn, 10*time.Second) {
		t.Fatalf("didn't time out waiting for node to be removed")
	}
}

// Test_MultiNodeClusterNoReapReadOnlyZero tests that a node is not incorrectly reaped.
func Test_MultiNodeClusterNoReapReadOnlyZero(t *testing.T) {
	cfgStoreFn := func(n *Node) {
		n.Store.ReapReadOnlyTimeout = 0
	}

	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()
	cfgStoreFn(node1)
	_, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	nonVoter := mustNewNode("2", false)
	defer nonVoter.Deprovision()
	cfgStoreFn(nonVoter)
	if err := nonVoter.JoinAsNonVoter(node1); err != nil {
		t.Fatalf("non-voting node failed to join leader: %s", err.Error())
	}
	_, err = nonVoter.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Confirm non-voter node is in the cluster config.
	nodes, err := node1.Nodes(true)
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}
	if !nodes.HasAddr(nonVoter.RaftAddr) {
		t.Fatalf("nodes do not contain non-voter node")
	}

	// Kill non-voter node, confirm it's not removed.
	nonVoter.Deprovision()
	tFn := func() bool {
		nodes, _ = node1.Nodes(true)
		return !nodes.HasAddr(nonVoter.RaftAddr)
	}
	if trueOrTimeout(tFn, 10*time.Second) {
		t.Fatalf("didn't time out waiting for node to be removed")
	}
}

func Test_MultiNodeCluster_Boot(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	_, err := node1.Boot("testdata/auto-restore.sqlite")
	if err != nil {
		t.Fatalf("failed to boot: %s", err.Error())
	}

	// Join a second node, check it gets the data via a snapshot.
	node2 := mustNewNode("node2", false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Send a few Noops through to ensure SQLite database has been updated on each node.
	for i := 0; i < 5; i++ {
		node1.Noop("some_id")
	}

	// Check the database on each node
	for _, n := range []*Node{node1, node2} {
		rows, err := n.QueryNoneConsistency(`SELECT COUNT(*) FROM foo`)
		if err != nil {
			t.Fatalf("failed to query node: %s", err.Error())
		}
		if got, exp := rows, `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[3]]}]}`; got != exp {
			t.Fatalf("got incorrect results from node exp: %s got: %s", exp, got)
		}
	}

	// One last test -- the leader should refuse any more Boot requests because it
	// is now part of a cluster.
	_, err = node1.Boot("testdata/auto-restore.sqlite")
	if err == nil {
		t.Fatalf("expected error booting")
	}
}

func Test_ClusterLeader_GET(t *testing.T) {
	leader := mustNewLeaderNode("leader1")
	defer leader.Deprovision()

	follower := mustNewNode("follower1", false)
	defer follower.Deprovision()

	if err := follower.Join(leader); err != nil {
		t.Fatalf("failed to join node: %s", err.Error())
	}

	// Test leader endpoint on leader
	leaderInfo, err := leader.Leader()
	if err != nil {
		t.Fatalf("failed to get leader info from leader: %s", err.Error())
	}

	// Parse the JSON response
	var leaderData map[string]string
	if err := json.Unmarshal([]byte(leaderInfo), &leaderData); err != nil {
		t.Fatalf("failed to parse leader response: %s", err.Error())
	}

	// Check that we have addr and api_addr
	if leaderData["addr"] == "" {
		t.Fatalf("leader addr is empty")
	}
	if leaderData["api_addr"] == "" {
		t.Fatalf("leader api_addr is empty")
	}

	// Test leader endpoint on follower - should return same leader info
	followerInfo, err := follower.Leader()
	if err != nil {
		t.Fatalf("failed to get leader info from follower: %s", err.Error())
	}

	var followerData map[string]string
	if err := json.Unmarshal([]byte(followerInfo), &followerData); err != nil {
		t.Fatalf("failed to parse follower leader response: %s", err.Error())
	}

	// Both should report the same leader
	if leaderData["addr"] != followerData["addr"] {
		t.Fatalf("leader and follower report different leader addresses: %s vs %s",
			leaderData["addr"], followerData["addr"])
	}
}

func Test_ClusterLeader_Stepdown(t *testing.T) {
	leader := mustNewLeaderNode("leader1")
	defer leader.Deprovision()

	follower := mustNewNode("follower1", false)
	defer follower.Deprovision()

	if err := follower.Join(leader); err != nil {
		t.Fatalf("failed to join node: %s", err.Error())
	}

	// Wait for cluster to stabilize
	if _, err := leader.WaitForLeader(); err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get leader info before stepdown
	leaderInfoBefore, err := leader.Leader()
	if err != nil {
		t.Fatalf("failed to get leader info before stepdown: %s", err.Error())
	}

	var leaderDataBefore map[string]string
	if err := json.Unmarshal([]byte(leaderInfoBefore), &leaderDataBefore); err != nil {
		t.Fatalf("failed to parse leader response before stepdown: %s", err.Error())
	}

	// Test stepdown on leader with wait=true for maximum testing
	err = leader.Stepdown(true)
	if err != nil {
		t.Fatalf("failed to trigger stepdown on leader: %s", err.Error())
	}

	testPoll(t, func() (bool, error) {
		leaderInfoAfter, err := follower.Leader()
		if err != nil {
			return false, nil
		}
		var leaderDataAfter map[string]string
		if err := json.Unmarshal([]byte(leaderInfoAfter), &leaderDataAfter); err != nil {
			return false, nil
		}
		return leaderDataBefore["addr"] != leaderDataAfter["addr"], nil
	}, time.Second, 10*time.Second)
}

func sleepForSecond() {
	time.Sleep(mustParseDuration("1s"))
}
