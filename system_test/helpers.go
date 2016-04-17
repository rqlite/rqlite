package system

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	sql "github.com/otoolep/rqlite/db"
	httpd "github.com/otoolep/rqlite/http"
	"github.com/otoolep/rqlite/store"
)

// Node represents a node under test.
type Node struct {
	Dir     string
	Store   *store.Store
	Service *httpd.Service
}

func (n *Node) APIAddr() string {
	return n.Service.Addr().String()
}

func (n *Node) RaftAddr() string {
	return n.Store.Addr().String()
}

// Deprovisions removes all resources associated with the node.
func (n *Node) Deprovision() {
	n.Store.Close()
	n.Service.Close()
	os.RemoveAll(n.Dir)
}

// WaitForLeader blocks for up to 10 seconds until the node detects a leader.
func (n *Node) WaitForLeader() error {
	_, err := n.Store.WaitForLeader(10 * time.Second)
	return err
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
	v, _ := url.Parse("http://" + n.APIAddr() + "/db/query")
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

func (n *Node) postExecute(stmt string) (string, error) {
	resp, err := http.Post("http://"+n.APIAddr()+"/db/execute", "application/json", strings.NewReader(stmt))
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

func mustNewNode(joinAddr string) *Node {
	node := &Node{
		Dir: mustTempDir(),
	}

	dbConf := sql.NewConfig()
	node.Store = store.New(dbConf, node.Dir, "localhost:0")
	if err := node.Store.Open(joinAddr == ""); err != nil {
		node.Deprovision()
		panic(fmt.Sprintf("failed to open store: %s", err.Error()))
	}

	node.Service = httpd.New("localhost:0", node.Store, nil)
	if err := node.Service.Start(); err != nil {
		node.Deprovision()
		panic(fmt.Sprintf("failed to start HTTP server: %s", err.Error()))
	}

	return node
}

func mustNewLeaderNode() *Node {
	node := mustNewNode("")
	if err := node.WaitForLeader(); err != nil {
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
