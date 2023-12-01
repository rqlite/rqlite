package http

import (
	"bytes"
	"encoding/json"
	"io"
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

// NodesRespEncoder encodes Nodes into JSON with an option for legacy format.
type NodesRespEncoder struct {
	writer io.Writer
	legacy bool
	prefix string
	indent string
}

// NewNodesRespEncoder creates a new NodesRespEncoder instance with the specified
// io.Writer and legacy flag.
func NewNodesRespEncoder(w io.Writer, legacy bool) *NodesRespEncoder {
	return &NodesRespEncoder{
		writer: w,
		legacy: legacy,
	}
}

// SetIndent sets the indentation format for the JSON output.
func (e *NodesRespEncoder) SetIndent(prefix, indent string) {
	e.prefix = prefix
	e.indent = indent
}

// Encode takes a slice of Nodes and encodes it into JSON,
// writing the output to the Encoder's writer.
func (e *NodesRespEncoder) Encode(nodes Nodes) error {
	var data []byte
	var err error

	if e.legacy {
		data, err = e.encodeLegacy(nodes)
	} else {
		data, err = e.encode(nodes)
	}

	if err != nil {
		return err
	}

	if e.indent != "" {
		var buf bytes.Buffer
		err = json.Indent(&buf, data, e.prefix, e.indent)
		if err != nil {
			return err
		}
		data = buf.Bytes()
	}

	_, err = e.writer.Write(data)
	return err
}

// encode encodes the nodes in the standard format.
func (e *NodesRespEncoder) encode(nodes Nodes) ([]byte, error) {
	nodeOutput := &struct {
		Nodes Nodes `json:"nodes"`
	}{
		Nodes: nodes,
	}
	return json.Marshal(nodeOutput)
}

// encodeLegacy encodes the nodes in the legacy format.
func (e *NodesRespEncoder) encodeLegacy(nodes Nodes) ([]byte, error) {
	legacyOutput := make(map[string]*Node)
	for _, node := range nodes {
		legacyOutput[node.ID] = node
	}
	return json.Marshal(legacyOutput)
}

// NodesRespDecoder decodes JSON data into a slice of Nodes.
type NodesRespDecoder struct {
	reader io.Reader
}

// NewNodesRespDecoder creates a new Decoder instance with the specified io.Reader.
func NewNodesRespDecoder(r io.Reader) *NodesRespDecoder {
	return &NodesRespDecoder{reader: r}
}

// Decode reads JSON from its reader and decodes it into the provided Nodes slice.
func (d *NodesRespDecoder) Decode(nodes *Nodes) error {
	// Temporary structure to facilitate decoding.
	var data struct {
		Nodes Nodes `json:"nodes"`
	}

	if err := json.NewDecoder(d.reader).Decode(&data); err != nil {
		return err
	}

	*nodes = data.Nodes
	return nil
}
