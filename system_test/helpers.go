package system

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	httpd "github.com/rqlite/rqlite/http"
	"github.com/rqlite/rqlite/store"
	"github.com/rqlite/rqlite/tcp"
)

// Node represents a node under test.
type Node struct {
	APIAddr  string
	RaftAddr string
	ID       string
	Dir      string
	Store    *store.Store
	Service  *httpd.Service
}

// SameAs returns true if this node is the same as node o.
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

// QueryMulti runs multiple queries against the node.
func (n *Node) QueryMulti(stmts []string) (string, error) {
	j, err := json.Marshal(stmts)
	if err != nil {
		return "", err
	}
	return n.postQuery(string(j))
}

// Join instructs this node to join the leader.
func (n *Node) Join(leader *Node) error {
	resp, err := DoJoinRequest(leader.APIAddr, n.Store.ID(), n.RaftAddr)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to join, leader returned: %s", resp.Status)
	}
	defer resp.Body.Close()
	return nil
}

// Status returns the status and diagnostic output for node.
func (n *Node) Status() (string, error) {
	v, _ := url.Parse("http://" + n.APIAddr + "/status")

	resp, err := http.Get(v.String())
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("status endpoint returned: %s", resp.Status)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// Expvar returns the expvar output for node.
func (n *Node) Expvar() (string, error) {
	v, _ := url.Parse("http://" + n.APIAddr + "/debug/vars")

	resp, err := http.Get(v.String())
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("expvar endpoint returned: %s", resp.Status)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// ConfirmRedirect confirms that the node responds with a redirect to the given host.
func (n *Node) ConfirmRedirect(host string) bool {
	v, _ := url.Parse("http://" + n.APIAddr + "/db/query")
	v.RawQuery = url.Values{"q": []string{`SELECT * FROM foo`}}.Encode()

	resp, err := http.Get(v.String())
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	fmt.Println(resp.StatusCode)
	if resp.StatusCode != http.StatusMovedPermanently {
		return false
	}
	if resp.Header.Get("location") != host {
		return false
	}
	return true
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

func (n *Node) postQuery(stmt string) (string, error) {
	resp, err := http.Post("http://"+n.APIAddr+"/db/query", "application/json", strings.NewReader(stmt))
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

// Followers returns the slice of nodes in the cluster that are followers.
func (c Cluster) Followers() ([]*Node, error) {
	n, err := c[0].WaitForLeader()
	if err != nil {
		return nil, err
	}
	leader, err := c.FindNodeByRaftAddr(n)
	if err != nil {
		return nil, err
	}

	var followers []*Node
	for _, n := range c {
		if n != leader {
			followers = append(followers, n)
		}
	}
	return followers, nil
}

// RemoveNode removes the given node from the list of nodes representing
// a cluster.
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

// Remove tells the cluster at n to remove the node at addr. Assumes n is the leader.
func Remove(n *Node, addr string) error {
	b, err := json.Marshal(map[string]string{"addr": addr})
	if err != nil {
		return err
	}

	// Attempt to remove node from leader.
	resp, err := http.Post("http://"+n.APIAddr+"/remove", "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to remove node, leader returned: %s", resp.Status)
	}
	defer resp.Body.Close()
	return nil
}

// DoJoinRequest sends a join request to nodeAddr, for raftID, reachable at raftAddr.
func DoJoinRequest(nodeAddr, raftID, raftAddr string) (*http.Response, error) {
	b, err := json.Marshal(map[string]string{"id": raftID, "addr": raftAddr})
	if err != nil {
		return nil, err
	}

	resp, err := http.Post("http://"+nodeAddr+"/join", "application-type/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func mustNewNode(enableSingle bool) *Node {
	node := &Node{
		Dir: mustTempDir(),
	}

	dbConf := store.NewDBConfig("", false)
	tn := tcp.NewTransport()
	if err := tn.Open("localhost:0"); err != nil {
		panic(err.Error())
	}
	node.Store = store.New(tn, &store.StoreConfig{
		DBConf: dbConf,
		Dir:    node.Dir,
		ID:     tn.Addr().String(),
	})
	if err := node.Store.Open(enableSingle); err != nil {
		node.Deprovision()
		panic(fmt.Sprintf("failed to open store: %s", err.Error()))
	}
	node.RaftAddr = node.Store.Addr()
	node.ID = node.Store.ID()

	node.Service = httpd.New("localhost:0", node.Store, nil)
	node.Service.Expvar = true
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

func mustTempDir() string {
	var err error
	path, err := ioutil.TempDir("", "rqlilte-system-test-")
	if err != nil {
		panic("failed to create temp dir")
	}
	return path
}

func isJSON(s string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

// rr is a helper function that forms expected Raft responses.
func rr(nodeID string, idx int) string {
	return fmt.Sprintf(`"raft":{"index":%d,"node_id":"%s"}`, idx, nodeID)
}
