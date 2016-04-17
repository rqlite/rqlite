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

type Node struct {
	Addr    string
	Dir     string
	Store   *store.Store
	Service *httpd.Service
}

func (n *Node) Deprovision() {
	n.Store.Close()
	n.Service.Close()
	os.RemoveAll(n.Dir)
}

func (n *Node) WaitForLeader() error {
	_, err := n.Store.WaitForLeader(10 * time.Second)
	return err
}

func (n *Node) Execute(stmt string) (string, error) {
	j, err := json.Marshal([]string{stmt})
	if err != nil {
		return "", err
	}
	stmt = string(j)

	resp, err := http.Post("http://"+n.Addr+"/db/execute", "application/json", strings.NewReader(stmt))
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

func (n *Node) Query(stmt string) (string, error) {
	v, _ := url.Parse("http://" + n.Addr + "/db/query")
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
	node.Addr = node.Service.Addr().String()

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
