package http

import (
	"time"

	"github.com/rqlite/rqlite/store"
)

// Node represents a single node in the cluster and can include
// information about the node's reachability and leadership status.
// If there was an error communicating with the node, the Error
// field will be populated.
type Node struct {
	ID        string  `json:"id,omitempty"`
	APIAddr   string  `json:"api_addr,omitempty"`
	Addr      string  `json:"addr,omitempty"`
	Voter     bool    `json:"voter"`
	Reachable bool    `json:"reachable"`
	Leader    bool    `json:"leader,omitempty"`
	Time      float64 `json:"time,omitempty"`
	Error     string  `json:"error,omitempty"`
}

// Test tests the node's reachability and leadership status. If an error
// occurs, the Error field will be populated.
func (n *Node) Test(ga GetAddresser, leaderAddr string, timeout time.Duration) {
	start := time.Now()
	apiAddr, err := ga.GetNodeAPIAddr(n.Addr, timeout)
	if err != nil {
		n.Error = err.Error()
		n.Reachable = false
		return
	}
	n.Time = time.Since(start).Seconds()
	n.APIAddr = apiAddr
	n.Reachable = true
	n.Leader = apiAddr == leaderAddr
}

// NewNodesFromServers creates a slice of Nodes from a slice of Servers.
func NewNodesFromServers(servers []*store.Server) ([]*Node, error) {
	nodes := make([]*Node, len(servers))
	for i, s := range servers {
		nodes[i] = NewNodeFromServer(s)
	}
	return nodes, nil
}

// NewNodeFromServer creates a Node from a Server.
func NewNodeFromServer(s *store.Server) *Node {
	return &Node{
		ID:    s.ID,
		Addr:  s.Addr,
		Voter: s.Suffrage == "Voter",
	}
}
