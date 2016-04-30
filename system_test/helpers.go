package system

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	httpd "github.com/rqlite/rqlite/http"
	"github.com/rqlite/rqlite/store"
)

// Node represents a node under test.
type Node struct {
	APIAddr  string
	RaftAddr string
	Dir      string
	Store    *store.Store
	Service  *httpd.Service
}

// SameAs returns true if this node is the same as o node.
func (n *Node) SameAs(o *Node) bool {
	return n.RaftAddr == o.RaftAddr
}

// Deprovision shuts down and removes all resources associated with the node.
func (n *Node) Deprovision() {
	n.Store.Close(false)
	n.Service.Close()
	os.RemoveAll(n.Dir)
}

// WaitForLeader blocks for up to 10 seconds until the node detects a leader.
func (n *Node) WaitForLeader() (string, error) {
	return n.Store.WaitForLeader(10 * time.Second)
}

// Execute executes a single statement against the node.
func (n *Node) Execute(stmt string) (string, error) {
	return n.ExecuteMulti([]string{stmt})
}

// ExecuteMulti executes multiple statements against the node.
func (n *Node) ExecuteMulti(stmts []string) (string, error) {
	j, err := json.Marshal(stmts)
	if err != nil {
		return "", err
	}
	return n.postExecute(string(j))
}

// Query runs a single query against the node.
func (n *Node) Query(stmt string) (string, error) {
	v, _ := url.Parse("http://" + n.APIAddr + "/db/query")
	v.RawQuery = url.Values{"q": []string{stmt}}.Encode()

	resp, err := http.Get(v.String())
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// Join instructs this node to join the leader.
func (n *Node) Join(leader *Node) error {
	b, err := json.Marshal(map[string]string{"addr": n.RaftAddr})
	if err != nil {
		return err
	}

	// Attempt to join leader
	resp, err := http.Post("http://"+leader.APIAddr+"/join", "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to join, leader returned: %s", resp.Status)
	}
	defer resp.Body.Close()
	return nil
}

func (n *Node) postExecute(stmt string) (string, error) {
	resp, err := http.Post("http://"+n.APIAddr+"/db/execute", "application/json", strings.NewReader(stmt))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// Cluster represents a cluster of nodes.
type Cluster []*Node

// Leader returns the leader node of a cluster.
func (c Cluster) Leader() (*Node, error) {
	l, err := c[0].WaitForLeader()
	if err != nil {
		return nil, err
	}
	return c.FindNodeByRaftAddr(l)
}

// WaitForNewLeader waits for the leader to change from the node passed in.
func (c Cluster) WaitForNewLeader(old *Node) (*Node, error) {
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			return nil, fmt.Errorf("timed out waiting for new leader")
		case <-ticker.C:
			l, err := c.Leader()
			if err != nil {
				continue
			}
			if !l.SameAs(old) {
				return l, nil
			}
		}
	}
}

// RemoveNode removes the given node from the cluster.
func (c Cluster) RemoveNode(node *Node) {
	for i, n := range c {
		if n.RaftAddr == node.RaftAddr {
			c = append(c[:i], c[i+1:]...)
			return
		}
	}
}

// FindNodeByRaftAddr returns the node with the given Raft address.
func (c Cluster) FindNodeByRaftAddr(addr string) (*Node, error) {
	for _, n := range c {
		if n.RaftAddr == addr {
			return n, nil
		}
	}
	return nil, fmt.Errorf("node not found")
}

// Deprovision deprovisions every node in the cluster.
func (c Cluster) Deprovision() {
	for _, n := range c {
		n.Deprovision()
	}
}

func mustNewNode(enableSingle bool) *Node {
	node := &Node{
		Dir: mustTempDir(),
	}

	dbConf := store.NewDBConfig("", false)
	node.Store = store.New(dbConf, node.Dir, mustMockTransport("localhost:0"))
	if err := node.Store.Open(enableSingle); err != nil {
		node.Deprovision()
		panic(fmt.Sprintf("failed to open store: %s", err.Error()))
	}
	node.RaftAddr = node.Store.Addr().String()

	node.Service = httpd.New("localhost:0", node.Store, nil)
	if err := node.Service.Start(); err != nil {
		node.Deprovision()
		panic(fmt.Sprintf("failed to start HTTP server: %s", err.Error()))
	}
	node.APIAddr = node.Service.Addr().String()

	return node
}

func mustNewLeaderNode() *Node {
	node := mustNewNode(true)
	if _, err := node.WaitForLeader(); err != nil {
		node.Deprovision()
		panic("node never became leader")
	}
	return node
}

type mockTransport struct {
	ln net.Listener
}

func mustMockTransport(addr string) *mockTransport {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic("failed to create new transport")
	}
	return &mockTransport{ln}
}

func (m *mockTransport) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}

func (m *mockTransport) Accept() (net.Conn, error) { return m.ln.Accept() }

func (m *mockTransport) Close() error { return m.ln.Close() }

func (m *mockTransport) Addr() net.Addr { return m.ln.Addr() }

func mustTempDir() string {
	var err error
	path, err := ioutil.TempDir("", "rqlilte-system-test-")
	if err != nil {
		panic("failed to create temp dir")
	}
	return path
}
