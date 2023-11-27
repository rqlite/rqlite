package http

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rqlite/rqlite/store"
)

func Test_NewNodeFromServer(t *testing.T) {
	server := &store.Server{ID: "1", Addr: "192.168.1.1", Suffrage: "Voter"}
	node := NewNodeFromServer(server)

	if node.ID != server.ID || node.Addr != server.Addr || !node.Voter {
		t.Fatalf("NewNodeFromServer did not correctly initialize Node from Server")
	}
}

func Test_NewNodesFromServers(t *testing.T) {
	servers := []*store.Server{
		{ID: "1", Addr: "192.168.1.1", Suffrage: "Voter"},
		{ID: "2", Addr: "192.168.1.2", Suffrage: "Nonvoter"},
	}
	nodes := NewNodesFromServers(servers)

	if len(nodes) != len(servers) {
		t.Fatalf("NewNodesFromServers did not create the correct number of nodes")
	}
	for i, node := range nodes {
		if node.ID != servers[i].ID || node.Addr != servers[i].Addr {
			t.Fatalf("NewNodesFromServers did not correctly initialize Node %d from Server", i)
		}
	}
}

func Test_NodesVoters(t *testing.T) {
	nodes := Nodes{
		{ID: "1", Voter: true},
		{ID: "2", Voter: false},
	}
	voters := nodes.Voters()

	if len(voters) != 1 || !voters[0].Voter {
		t.Fatalf("Voters method did not correctly filter voter nodes")
	}
}

func Test_NodeTestLeader(t *testing.T) {
	node := &Node{ID: "1", Addr: "leader-raft-addr", APIAddr: "leader-api-addr"}
	mockGA := newMockGetAddresser("leader-api-addr", nil)

	node.Test(mockGA, "leader-raft-addr", 10*time.Second)
	if !node.Reachable || !node.Leader {
		t.Fatalf("Test method did not correctly update node status %s", asJSON(node))
	}
}

func Test_NodeTestNotLeader(t *testing.T) {
	node := &Node{ID: "1", Addr: "follower-raft-addr", APIAddr: "follower-api-addr"}
	mockGA := newMockGetAddresser("follower-api-addr", nil)

	node.Test(mockGA, "leader-raft-addr", 10*time.Second)
	if !node.Reachable || node.Leader {
		t.Fatalf("Test method did not correctly update node status %s", asJSON(node))
	}
}

// mockGetAddresser is a mock implementation of the GetAddresser interface.
type mockGetAddresser struct {
	// Add more fields if needed to simulate different behaviors.
	apiAddr string
	err     error
}

// newMockGetAddresser creates a new instance of mockGetAddresser.
// You can customize the return values for GetNodeAPIAddr by setting apiAddr and err.
func newMockGetAddresser(apiAddr string, err error) *mockGetAddresser {
	return &mockGetAddresser{apiAddr: apiAddr, err: err}
}

// GetNodeAPIAddr is the mock implementation of the GetNodeAPIAddr method.
func (m *mockGetAddresser) GetNodeAPIAddr(addr string, timeout time.Duration) (string, error) {
	return m.apiAddr, m.err
}

func asJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}
