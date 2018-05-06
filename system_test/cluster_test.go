package system

import (
	"fmt"
	"testing"
)

// Test_JoinLeaderNode tests a join operation between a leader and a new node.
func Test_JoinLeaderNode(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

	// Run queries against cluster.
	tests := []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: fmt.Sprintf(`{"results":[{}],%s}`, rr(leader.ID, 7)),
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: fmt.Sprintf(`{"results":[{"last_insert_id":1,"rows_affected":1}],%s}`, rr(leader.ID, 8)),
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
			expected: fmt.Sprintf(`{"results":[{"error":"table foo already exists"}],%s}`, rr(leader.ID, 10)),
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("sinead")`,
			expected: fmt.Sprintf(`{"results":[{"last_insert_id":2,"rows_affected":1}],%s}`, rr(leader.ID, 11)),
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
