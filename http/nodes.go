package http

import (
	"sort"
	"sync"
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
	Leader    bool    `json:"leader"`
	Time      float64 `json:"time,omitempty"`
	Error     string  `json:"error,omitempty"`
}

// NewNodeFromServer creates a Node from a Server.
func NewNodeFromServer(s *store.Server) *Node {
	return &Node{
		ID:    s.ID,
		Addr:  s.Addr,
		Voter: s.Suffrage == "Voter",
	}
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
	n.Leader = n.Addr == leaderAddr
}

type Nodes []*Node

func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n Nodes) Less(i, j int) bool { return n[i].ID < n[j].ID }

// NewNodesFromServers creates a slice of Nodes from a slice of Servers.
func NewNodesFromServers(servers []*store.Server) Nodes {
	nodes := make([]*Node, len(servers))
	for i, s := range servers {
		nodes[i] = NewNodeFromServer(s)
	}
	sort.Sort(Nodes(nodes))
	return nodes
}

// Voters returns a slice of Nodes that are voters.
func (n Nodes) Voters() Nodes {
	v := make(Nodes, 0)
	for _, node := range n {
		if node.Voter {
			v = append(v, node)
		}
	}
	sort.Sort(v)
	return v
}

// HasAddr returns whether any node in the Nodes slice has the given Raft address.
func (n Nodes) HasAddr(addr string) bool {
	for _, node := range n {
		if node.Addr == addr {
			return true
		}
	}
	return false
}

// Test tests the reachability and leadership status of all nodes. It does this
// in parallel, and blocks until all nodes have been tested.
func (n Nodes) Test(ga GetAddresser, leaderAddr string, timeout time.Duration) {
	var wg sync.WaitGroup
	for _, nn := range n {
		wg.Add(1)
		go func(nnn *Node) {
			defer wg.Done()
			nnn.Test(ga, leaderAddr, timeout)
		}(nn)
	}
	wg.Wait()
}
