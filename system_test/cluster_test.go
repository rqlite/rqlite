package system

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/cluster"
	"github.com/rqlite/rqlite/db"
	"github.com/rqlite/rqlite/http"
	"github.com/rqlite/rqlite/queue"
	"github.com/rqlite/rqlite/rtls"
	"github.com/rqlite/rqlite/store"
	"github.com/rqlite/rqlite/tcp"
)

// Test_JoinLeaderNode tests a join operation between a leader and a new node.
func Test_JoinLeaderNode(t *testing.T) {
	leader := mustNewLeaderNode()
	defer leader.Deprovision()

	node := mustNewNode(false)
	defer node.Deprovision()
	if err := node.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
}

// Test_MultiNodeCluster tests formation of a 3-node cluster, and its operation.
func Test_MultiNodeCluster(t *testing.T) {
	node1 := mustNewLeaderNode()
	defer node1.Deprovision()

	node2 := mustNewNode(false)
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

	node3 := mustNewNode(false)
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
	node1 := mustNewLeaderNode()
	defer node1.Deprovision()

	node2 := mustNewNode(false)
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
	if got, exp := r, `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[1]]}]}`; got != exp {
		t.Fatalf("wrong query results, exp %s, got %s", exp, got)
	}

	// Send a few Noops through to ensure SQLite database has been updated on each node.
	for i := 0; i < 5; i++ {
		node1.Noop("some_id")
	}

	// Check that row is *exactly* the same on each node. This could only happen if RANDOM was
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
		t.Fatalf("node 1 and node 2 do not have the same data (%s %s)", r1, r2)
	}
}

// Test_MultiNodeClusterBootstrap tests formation of a 3-node cluster via bootstraping,
// and its operation.
func Test_MultiNodeClusterBootstrap(t *testing.T) {
	node1 := mustNewNode(false)
	node1.Store.BootstrapExpect = 3
	defer node1.Deprovision()

	node2 := mustNewNode(false)
	node2.Store.BootstrapExpect = 3
	defer node2.Deprovision()

	node3 := mustNewNode(false)
	node3.Store.BootstrapExpect = 3
	defer node3.Deprovision()

	provider := cluster.NewAddressProviderString(
		[]string{node1.APIAddr, node2.APIAddr, node3.APIAddr})
	node1Bs := cluster.NewBootstrapper(provider, nil)
	node2Bs := cluster.NewBootstrapper(provider, nil)
	node3Bs := cluster.NewBootstrapper(provider, nil)

	// Have all nodes start a bootstrap basically in parallel,
	// ensure only 1 leader actually gets elected.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		done := func() bool {
			addr, _ := node1.Store.LeaderAddr()
			return addr != ""
		}
		node1Bs.Boot(node1.ID, node1.RaftAddr, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node2.Store.LeaderAddr()
			return addr != ""
		}
		node2Bs.Boot(node2.ID, node2.RaftAddr, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node3.Store.LeaderAddr()
			return addr != ""
		}
		node3Bs.Boot(node3.ID, node3.RaftAddr, done, 10*time.Second)
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
// then checking a 4th node can join later with the bootstap parameters.
func Test_MultiNodeClusterBootstrapLaterJoin(t *testing.T) {
	node1 := mustNewNode(false)
	node1.Store.BootstrapExpect = 3
	defer node1.Deprovision()

	node2 := mustNewNode(false)
	node2.Store.BootstrapExpect = 3
	defer node2.Deprovision()

	node3 := mustNewNode(false)
	node3.Store.BootstrapExpect = 3
	defer node3.Deprovision()

	provider := cluster.NewAddressProviderString(
		[]string{node1.APIAddr, node2.APIAddr, node3.APIAddr})
	node1Bs := cluster.NewBootstrapper(provider, nil)
	node1Bs.Interval = time.Second
	node2Bs := cluster.NewBootstrapper(provider, nil)
	node2Bs.Interval = time.Second
	node3Bs := cluster.NewBootstrapper(provider, nil)
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
		node1Bs.Boot(node1.ID, node1.RaftAddr, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node2.Store.LeaderAddr()
			return addr != ""
		}
		node2Bs.Boot(node2.ID, node2.RaftAddr, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node3.Store.LeaderAddr()
			return addr != ""
		}
		node3Bs.Boot(node3.ID, node3.RaftAddr, done, 10*time.Second)
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
	node4 := mustNewNode(false)
	node4.Store.BootstrapExpect = 3
	defer node3.Deprovision()
	node4Bs := cluster.NewBootstrapper(provider, nil)
	node4Bs.Interval = time.Second
	done := func() bool {
		addr, _ := node4.Store.LeaderAddr()
		return addr != ""
	}
	if err := node4Bs.Boot(node4.ID, node4.RaftAddr, done, 10*time.Second); err != nil {
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

// Test_MultiNodeClusterBootstrapLaterJoinHTTPS tests formation of a 3-node cluster which
// uses HTTP and TLS,then checking a 4th node can join later with the bootstap parameters.
func Test_MultiNodeClusterBootstrapLaterJoinHTTPS(t *testing.T) {
	node1 := mustNewNodeEncrypted(false, true, true)
	node1.Store.BootstrapExpect = 3
	defer node1.Deprovision()

	node2 := mustNewNodeEncrypted(false, true, true)
	node2.Store.BootstrapExpect = 3
	defer node2.Deprovision()

	node3 := mustNewNodeEncrypted(false, true, true)
	node3.Store.BootstrapExpect = 3
	defer node3.Deprovision()

	tlsConfig, err := rtls.CreateClientConfig("", "", "", true, false)
	if err != nil {
		t.Fatalf("failed to create TLS config: %s", err)
	}

	provider := cluster.NewAddressProviderString(
		[]string{node1.APIAddr, node2.APIAddr, node3.APIAddr})
	node1Bs := cluster.NewBootstrapper(provider, tlsConfig)
	node1Bs.Interval = time.Second
	node2Bs := cluster.NewBootstrapper(provider, tlsConfig)
	node2Bs.Interval = time.Second
	node3Bs := cluster.NewBootstrapper(provider, tlsConfig)
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
		node1Bs.Boot(node1.ID, node1.RaftAddr, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node2.Store.LeaderAddr()
			return addr != ""
		}
		node2Bs.Boot(node2.ID, node2.RaftAddr, done, 10*time.Second)
		wg.Done()
	}()
	go func() {
		done := func() bool {
			addr, _ := node3.Store.LeaderAddr()
			return addr != ""
		}
		node3Bs.Boot(node3.ID, node3.RaftAddr, done, 10*time.Second)
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
	node4 := mustNewNodeEncrypted(false, true, true)
	node4.Store.BootstrapExpect = 3
	defer node3.Deprovision()
	node4Bs := cluster.NewBootstrapper(provider, tlsConfig)
	node4Bs.Interval = time.Second
	done := func() bool {
		addr, _ := node4.Store.LeaderAddr()
		return addr != ""
	}
	if err := node4Bs.Boot(node4.ID, node4.RaftAddr, done, 10*time.Second); err != nil {
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

	// Start two nodes, and ensure a cluster can be formed.
	node1 := mustNodeEncrypted(mustTempDir(), true, false, mux1, "1")
	defer node1.Deprovision()
	leader, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader on node1: %s", err.Error())
	}
	if exp, got := advAddr1.String(), leader; exp != got {
		t.Fatalf("node return wrong leader from leader, exp: %s, got %s", exp, got)
	}

	node2 := mustNodeEncrypted(mustTempDir(), false, false, mux2, "2")
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
	node1 := mustNewLeaderNode()
	defer node1.Deprovision()

	node2 := mustNewNode(false)
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

	node3 := mustNewNode(false)
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
	ns, ok := nodes[leader.ID]
	if !ok {
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
	ns = nodes[f.ID]
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
	node1 := mustNewLeaderNode()
	defer node1.Deprovision()

	if _, err := node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Join a second and third nodes
	node2 := mustNewNode(false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	node3 := mustNewNode(false)
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
	wg.Add(5)
	go func() {
		defer wg.Done()
		for i := 0; i < writesPerLoop; i++ {
			if _, err := node1.Execute(`INSERT INTO foo(name) VALUES("fiona")`); err != nil {
				t.Logf("failed to insert records: %s", err.Error())
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < writesPerLoop; i++ {
			if _, err := node2.Execute(`INSERT INTO foo(name) VALUES("fiona")`); err != nil {
				t.Logf("failed to insert records: %s", err.Error())
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < writesPerLoop-1; i++ {
			if _, err := node2.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, false); err != nil {
				t.Logf("failed to insert records: %s", err.Error())
			}
		}
		if _, err := node2.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, true); err != nil {
			t.Logf("failed to insert records: %s", err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < writesPerLoop-1; i++ {
			if _, err := node3.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, false); err != nil {
				t.Logf("failed to insert records: %s", err.Error())
			}
		}
		if _, err := node3.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, true); err != nil {
			t.Logf("failed to insert records: %s", err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < writesPerLoop-1; i++ {
			if _, err := node3.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, false); err != nil {
				t.Logf("failed to insert records: %s", err.Error())
			}
		}
		if _, err := node3.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, true); err != nil {
			t.Logf("failed to insert records: %s", err.Error())
		}
	}()
	wg.Wait()

	exp := fmt.Sprintf(`{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[%d]]}]}`, 5*writesPerLoop)
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

	node1 := mustNewLeaderNode()
	defer node1.Deprovision()

	if _, err := node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Join a second and third nodes
	node2 := mustNewNode(false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	node3 := mustNewNode(false)
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
	writesPerNode := 10000

	var wg sync.WaitGroup
	wg.Add(len(nodesUnderTest))
	for _, n := range nodesUnderTest {
		go func(nt *Node) {
			defer wg.Done()
			for i := 0; i < writesPerNode-1; i++ {
				if _, err := nt.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, false); err != nil {
					t.Logf("failed to insert records: %s", err.Error())
				}
			}
			if _, err := nt.ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, true); err != nil {
				t.Logf("failed to insert records: %s", err.Error())
			}
		}(n)
	}
	wg.Wait()

	exp := fmt.Sprintf(`{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[%d]]}]}`, len(nodesUnderTest)*writesPerNode)
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
	node1 := mustNewLeaderNode()
	defer node1.Deprovision()

	node2 := mustNewNode(false)
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

	nonVoter := mustNewNode(false)
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
	node1 := mustNewNodeEncrypted(true, false, true)
	defer node1.Deprovision()
	if _, err := node1.WaitForLeader(); err != nil {
		t.Fatalf("node never became leader")
	}

	node2 := mustNewNodeEncrypted(false, false, true)
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

	node3 := mustNewNodeEncrypted(false, false, true)
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
	node1 := mustNewLeaderNode()
	defer node1.Deprovision()

	if _, err := node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Force snapshots and log truncation to occur.
	for i := 0; i < 3*int(node1.Store.SnapshotThreshold); i++ {
		_, err := node1.Execute(`INSERT INTO foo(name) VALUES("sinead")`)
		if err != nil {
			t.Fatalf(`failed to write records for Snapshot test: %s`, err.Error())
		}
	}

	// Join a second and third nodes, which will get database state via snapshots.
	node2 := mustNewNode(false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	node3 := mustNewNode(false)
	defer node3.Deprovision()
	if err := node3.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Create a new cluster.
	c := Cluster{node1, node2, node3}

	// Wait for followers to pick up state.
	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to determine followers: %s", err.Error())
	}

	var n int
	var r string
	for _, f := range followers {
		n = 0
		for {
			r, err = f.QueryNoneConsistency(`SELECT COUNT(*) FROM foo`)
			if err != nil {
				t.Fatalf("failed to query follower node: %s", err.Error())
			}

			if r != `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[300]]}]}` {
				if n < 20 {
					// Wait, and try again.
					time.Sleep(mustParseDuration("1s"))
					n++
					continue
				}
				t.Fatalf("timed out waiting for snapshot state")
			}
			// The node passed!
			break
		}
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
	n = 0
	for {
		var r string
		r, err = leader.Query(`SELECT COUNT(*) FROM foo`)
		if err != nil {
			t.Fatalf("failed to query follower node: %s", err.Error())
		}

		if r != `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[300]]}]}` {
			if n < 10 {
				// Wait, and try again.
				time.Sleep(mustParseDuration("100ms"))
				n++
				continue
			}
			t.Fatalf("timed out waiting for snapshot state")
		}
		// Test passed!
		break
	}
}

// Test_MultiNodeClusterWithNonVoter tests formation of a 4-node cluster, one of which is
// a non-voter
func Test_MultiNodeClusterWithNonVoter(t *testing.T) {
	node1 := mustNewLeaderNode()
	defer node1.Deprovision()

	node2 := mustNewNode(false)
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

	node3 := mustNewNode(false)
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

	nonVoter := mustNewNode(false)
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

	// Run queries against the now 3-node cluster.
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

// Test_MultiNodeClusterRecoverSingle tests recovery of a single node from a 3-node cluster,
// which no longer has quorum.
func Test_MultiNodeClusterRecoverSingle(t *testing.T) {
	node1 := mustNewLeaderNode()
	defer node1.Deprovision()

	if _, err := node1.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if _, err := node1.Execute(`INSERT INTO foo(id, name) VALUES(1, "fiona")`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if rows, _ := node1.Query(`SELECT COUNT(*) FROM foo`); rows != `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[1]]}]}` {
		t.Fatalf("got incorrect results from node: %s", rows)
	}

	// Join a second and third nodes, which will get database state via snapshots.
	node2 := mustNewNode(false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	node3 := mustNewNode(false)
	defer node3.Deprovision()
	if err := node3.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
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

	// Create a single node using the node's data directory. It should fail because
	// quorum can't be met. This isn't quite right since the Raft address is also
	// changing, but it generally proves it doesn't come up.
	mux0, ln0 := mustNewOpenMux("127.0.0.1:10000")
	failedSingle := mustNodeEncrypted(node1.Dir, true, false, mux0, node1.Store.ID())
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

	okSingle := mustNodeEncrypted(node1.Dir, true, false, mux1, node1.Store.ID())
	_, err = okSingle.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}
	if rows, _ := okSingle.Query(`SELECT COUNT(*) FROM foo`); rows != `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[1]]}]}` {
		t.Fatalf("got incorrect results from recovered node: %s", rows)
	}
	okSingle.Close(true)
	ln1.Close()
}

// Test_MultiNodeClusterRecoverFull tests recovery of a full 3-node cluster,
// each node coming up with a different Raft address.
func Test_MultiNodeClusterRecoverFull(t *testing.T) {
	var err error

	mux1, ln1 := mustNewOpenMux("127.0.0.1:10001")
	node1 := mustNodeEncrypted(mustTempDir(), true, false, mux1, "1")
	_, err = node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	mux2, ln2 := mustNewOpenMux("127.0.0.1:10002")
	node2 := mustNodeEncrypted(mustTempDir(), false, false, mux2, "2")
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	mux3, ln3 := mustNewOpenMux("127.0.0.1:10003")
	node3 := mustNodeEncrypted(mustTempDir(), false, false, mux3, "3")
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
	if rows, _ := node1.Query(`SELECT COUNT(*) FROM foo`); rows != `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[1]]}]}` {
		t.Fatalf("got incorrect results from node: %s", rows)
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
	node4 := mustNodeEncrypted(node1.Dir, false, false, mux4, "1")
	defer node4.Deprovision()
	defer ln4.Close()

	mux5, ln5 := mustNewOpenMux("127.0.0.1:11002")
	node5 := mustNodeEncrypted(node2.Dir, false, false, mux5, "2")
	defer node5.Deprovision()
	defer ln5.Close()

	mux6, ln6 := mustNewOpenMux("127.0.0.1:11003")
	node6 := mustNodeEncrypted(node3.Dir, false, false, mux6, "3")
	defer node6.Deprovision()
	defer ln6.Close()

	_, err = node6.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader on recovered cluster: %s", err.Error())
	}

	if rows, _ := node4.Query(`SELECT COUNT(*) FROM foo`); rows != `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[1]]}]}` {
		t.Fatalf("got incorrect results from recovered node: %s", rows)
	}
}

// Test_MultiNodeClusterReapNodes tests that unreachable nodes are reaped.
func Test_MultiNodeClusterReapNodes(t *testing.T) {
	cfgStoreFn := func(n *Node) {
		n.Store.ReapTimeout = time.Second
		n.Store.ReapReadOnlyTimeout = time.Second
	}

	node1 := mustNewLeaderNode()
	defer node1.Deprovision()
	cfgStoreFn(node1)
	_, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	node2 := mustNewNode(false)
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

	node3 := mustNewNode(false)
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

	nonVoter := mustNewNode(false)
	defer nonVoter.Deprovision()
	cfgStoreFn(nonVoter)
	if err := nonVoter.JoinAsNonVoter(leader); err != nil {
		t.Fatalf("non-voting node failed to join leader: %s", err.Error())
	}
	_, err = nonVoter.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Confirm non-voter node is in the the cluster config.
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

	// Confirm voting node is in the the cluster config.
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

	node1 := mustNewLeaderNode()
	defer node1.Deprovision()
	cfgStoreFn(node1)
	_, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	nonVoter := mustNewNode(false)
	defer nonVoter.Deprovision()
	cfgStoreFn(nonVoter)
	if err := nonVoter.JoinAsNonVoter(node1); err != nil {
		t.Fatalf("non-voting node failed to join leader: %s", err.Error())
	}
	_, err = nonVoter.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Confirm non-voter node is in the the cluster config.
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

	node1 := mustNewLeaderNode()
	defer node1.Deprovision()
	cfgStoreFn(node1)
	_, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	node2 := mustNewNode(false)
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

	node3 := mustNewNode(false)
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

	// Confirm voting node is in the the cluster config.
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

	node1 := mustNewLeaderNode()
	defer node1.Deprovision()
	cfgStoreFn(node1)
	_, err := node1.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	nonVoter := mustNewNode(false)
	defer nonVoter.Deprovision()
	cfgStoreFn(nonVoter)
	if err := nonVoter.JoinAsNonVoter(node1); err != nil {
		t.Fatalf("non-voting node failed to join leader: %s", err.Error())
	}
	_, err = nonVoter.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Confirm non-voter node is in the the cluster config.
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
