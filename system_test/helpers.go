package system

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rqlite/rqlite/cluster"
	"github.com/rqlite/rqlite/command/encoding"
	httpd "github.com/rqlite/rqlite/http"
	"github.com/rqlite/rqlite/store"
	"github.com/rqlite/rqlite/tcp"
	"github.com/rqlite/rqlite/testdata/x509"
)

const (
	// SnapshotInterval is the period between snapshot checks
	SnapshotInterval = time.Second
)

// Node represents a node under test.
type Node struct {
	APIAddr      string
	RaftAddr     string
	ID           string
	Dir          string
	NodeCertPath string
	NodeKeyPath  string
	HTTPCertPath string
	HTTPKeyPath  string
	Store        *store.Store
	Service      *httpd.Service
	Cluster      *cluster.Service
}

// SameAs returns true if this node is the same as node o.
func (n *Node) SameAs(o *Node) bool {
	return n.RaftAddr == o.RaftAddr
}

// Close closes the node.
func (n *Node) Close(graceful bool) error {
	if err := n.Store.Close(graceful); err != nil {
		return err
	}
	n.Service.Close()
	n.Cluster.Close()
	return nil
}

// Deprovision shuts down and removes all resources associated with the node.
func (n *Node) Deprovision() {
	n.Store.Close(true)
	n.Service.Close()
	n.Cluster.Close()
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

// ExecuteParameterized executes a single paramterized query against the ndoe
func (n *Node) ExecuteParameterized(stmt []interface{}) (string, error) {
	m := make([][]interface{}, 1)
	m[0] = stmt

	j, err := json.Marshal(m)
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

// QueryNoneConsistency runs a single query against the node, with no read consistency.
func (n *Node) QueryNoneConsistency(stmt string) (string, error) {
	v, _ := url.Parse("http://" + n.APIAddr + "/db/query")
	v.RawQuery = url.Values{
		"q":     []string{stmt},
		"level": []string{"none"},
	}.Encode()

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

// QueryParameterized run a single paramterized query against the ndoe
func (n *Node) QueryParameterized(stmt []interface{}) (string, error) {
	m := make([][]interface{}, 1)
	m[0] = stmt

	j, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return n.postQuery(string(j))
}

// Noop inserts a noop command into the Store's Raft log.
func (n *Node) Noop(id string) error {
	return n.Store.Noop(id)
}

// Join instructs this node to join the leader.
func (n *Node) Join(leader *Node) error {
	resp, err := DoJoinRequest(leader.APIAddr, n.Store.ID(), n.RaftAddr, true)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to join as voter, leader returned: %s", resp.Status)
	}
	defer resp.Body.Close()
	return nil
}

// JoinAsNonVoter instructs this node to join the leader, but as a non-voting node.
func (n *Node) JoinAsNonVoter(leader *Node) error {
	resp, err := DoJoinRequest(leader.APIAddr, n.Store.ID(), n.RaftAddr, false)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to join as non-voter, leader returned: %s", resp.Status)
	}
	defer resp.Body.Close()
	return nil
}

// NodesStatus is the Go type /nodes endpoint response is marshaled into.
type NodesStatus map[string]struct {
	APIAddr   string `json:"api_addr,omitempty"`
	Addr      string `json:"addr,omitempty"`
	Reachable bool   `json:"reachable,omitempty"`
	Leader    bool   `json:"leader,omitempty"`
}

// Nodes returns the sNodes endpoint output for node.
func (n *Node) Nodes(includeNonVoters bool) (NodesStatus, error) {
	v, _ := url.Parse("http://" + n.APIAddr + "/nodes")
	if includeNonVoters {
		q := v.Query()
		q.Set("nonvoters", "true")
		v.RawQuery = q.Encode()
	}

	resp, err := http.Get(v.String())
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("nodes endpoint returned: %s", resp.Status)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var nstatus NodesStatus
	if err = json.Unmarshal(body, &nstatus); err != nil {
		return nil, err
	}
	return nstatus, nil
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

// PostExecuteStmt performs a HTTP execute request
func PostExecuteStmt(apiAddr string, stmt string) (string, error) {
	return PostExecuteStmtMulti(apiAddr, []string{stmt})
}

// PostExecuteStmtMulti performs a HTTP batch execute request
func PostExecuteStmtMulti(apiAddr string, stmts []string) (string, error) {
	j, err := json.Marshal(stmts)
	if err != nil {
		return "", err
	}

	resp, err := http.Post("http://"+apiAddr+"/db/execute", "application/json", strings.NewReader(string(j)))
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
func DoJoinRequest(nodeAddr, raftID, raftAddr string, voter bool) (*http.Response, error) {
	b, err := json.Marshal(map[string]interface{}{"id": raftID, "addr": raftAddr, "voter": voter})
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
	return mustNewNodeEncrypted(enableSingle, false, false)
}

func mustNewNodeEncrypted(enableSingle, httpEncrypt, nodeEncrypt bool) *Node {
	dir := mustTempDir()
	var mux *tcp.Mux
	if nodeEncrypt {
		mux = mustNewOpenTLSMux(x509.CertFile(dir), x509.KeyFile(dir), "")
	} else {
		mux = mustNewOpenMux("")
	}
	go mux.Serve()

	return mustNodeEncrypted(dir, enableSingle, httpEncrypt, mux, "")
}

func mustNodeEncrypted(dir string, enableSingle, httpEncrypt bool, mux *tcp.Mux, nodeID string) *Node {
	return mustNodeEncryptedOnDisk(dir, enableSingle, httpEncrypt, mux, nodeID, false)
}

func mustNodeEncryptedOnDisk(dir string, enableSingle, httpEncrypt bool, mux *tcp.Mux, nodeID string, onDisk bool) *Node {
	nodeCertPath := x509.CertFile(dir)
	nodeKeyPath := x509.KeyFile(dir)
	httpCertPath := nodeCertPath
	httpKeyPath := nodeKeyPath

	node := &Node{
		Dir:          dir,
		NodeCertPath: nodeCertPath,
		NodeKeyPath:  nodeKeyPath,
		HTTPCertPath: httpCertPath,
		HTTPKeyPath:  httpKeyPath,
	}

	dbConf := store.NewDBConfig(!onDisk)

	raftTn := mux.Listen(cluster.MuxRaftHeader)
	id := nodeID
	if id == "" {
		id = raftTn.Addr().String()
	}
	node.Store = store.New(raftTn, &store.StoreConfig{
		DBConf: dbConf,
		Dir:    node.Dir,
		ID:     id,
	})
	node.Store.SnapshotThreshold = 100
	node.Store.SnapshotInterval = SnapshotInterval

	if err := node.Store.Open(enableSingle); err != nil {
		node.Deprovision()
		panic(fmt.Sprintf("failed to open store: %s", err.Error()))
	}
	node.RaftAddr = node.Store.Addr()
	node.ID = node.Store.ID()

	clstr := cluster.New(mux.Listen(cluster.MuxClusterHeader), node.Store)
	if err := clstr.Open(); err != nil {
		panic("failed to open Cluster service)")
	}
	node.Cluster = clstr

	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, false, true)
	clstrClient := cluster.NewClient(clstrDialer)
	node.Service = httpd.New("localhost:0", node.Store, clstrClient, nil)
	node.Service.Expvar = true
	if httpEncrypt {
		node.Service.CertFile = node.HTTPCertPath
		node.Service.KeyFile = node.HTTPKeyPath
	}

	if err := node.Service.Start(); err != nil {
		node.Deprovision()
		panic(fmt.Sprintf("failed to start HTTP server: %s", err.Error()))
	}
	node.APIAddr = node.Service.Addr().String()

	// Finally, set API address in Cluster service
	clstr.SetAPIAddr(node.APIAddr)

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

func mustNewOpenMux(addr string) *tcp.Mux {
	if addr == "" {
		addr = "localhost:0"
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on %s: %s", addr, err.Error()))
	}

	var mux *tcp.Mux
	mux, err = tcp.NewMux(ln, nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create node-to-node mux: %s", err.Error()))
	}

	go mux.Serve()
	return mux
}

func mustNewOpenTLSMux(certFile, keyPath, addr string) *tcp.Mux {
	if addr == "" {
		addr = "localhost:0"
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on %s: %s", addr, err.Error()))
	}

	var mux *tcp.Mux
	mux, err = tcp.NewTLSMux(ln, nil, certFile, keyPath, "")
	if err != nil {
		panic(fmt.Sprintf("failed to create node-to-node mux: %s", err.Error()))
	}
	mux.InsecureSkipVerify = true

	go mux.Serve()
	return mux
}

func mustTCPListener(bind string) net.Listener {
	l, err := net.Listen("tcp", bind)
	if err != nil {
		panic(err)
	}
	return l
}

func mustGetLocalIPv4Address() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(fmt.Sprintf("failed to get interface addresses: %s", err.Error()))
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	panic("couldn't find a local IPv4 address")
}

func mustParseDuration(d string) time.Duration {
	if dur, err := time.ParseDuration(d); err != nil {
		panic("failed to parse duration")
	} else {
		return dur
	}
}

func asJSON(v interface{}) string {
	b, err := encoding.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}

func isJSON(s string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

/* MIT License
 *
 * Copyright (c) 2017 Roland Singer [roland.singer@desertbit.com]
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

// copyFile copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file. The file mode will be copied from the source and
// the copied data is synced/flushed to stable storage.
func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		if e := out.Close(); e != nil {
			err = e
		}
	}()

	_, err = io.Copy(out, in)
	if err != nil {
		return
	}

	err = out.Sync()
	if err != nil {
		return
	}

	si, err := os.Stat(src)
	if err != nil {
		return
	}
	err = os.Chmod(dst, si.Mode())
	if err != nil {
		return
	}

	return
}

// copyDir recursively copies a directory tree, attempting to preserve permissions.
// Source directory must exist, destination directory must *not* exist.
// Symlinks are ignored and skipped.
func copyDir(src string, dst string) (err error) {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !si.IsDir() {
		return fmt.Errorf("source is not a directory")
	}

	_, err = os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	if err == nil {
		return fmt.Errorf("destination already exists")
	}

	err = os.MkdirAll(dst, si.Mode())
	if err != nil {
		return
	}

	entries, err := ioutil.ReadDir(src)
	if err != nil {
		return
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			err = copyDir(srcPath, dstPath)
			if err != nil {
				return
			}
		} else {
			// Skip symlinks.
			if entry.Mode()&os.ModeSymlink != 0 {
				continue
			}

			err = copyFile(srcPath, dstPath)
			if err != nil {
				return
			}
		}
	}

	return
}
