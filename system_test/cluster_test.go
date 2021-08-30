package system

import (
	"fmt"
	"net"
	"testing"
	"time"

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
	c.RemoveNode(leader)
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

	node3 := mustNewNode(false)
	defer node3.Deprovision()
	if err := node3.JoinAsNonVoter(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c = Cluster{node1, node2, node3}
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
	c.RemoveNode(leader)
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
	c.RemoveNode(node1)
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
	c.RemoveNode(leader)
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
