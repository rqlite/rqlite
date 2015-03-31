package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/otoolep/rqlite/server"
)

var spinUpDelay = time.Duration(2 * time.Second)

type testServer struct {
	host   string
	port   int
	server *server.Server
}

func (t *testServer) URL() string {
	return fmt.Sprintf("http://%s:%d/db", t.host, t.port)
}

func doPost(t *testing.T, url, body string) {
	resp, err := http.Post(url, "text/plain", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("HTTP request failed: %s", err.Error())
	}
	if resp.StatusCode != 200 {
		t.Fatalf("bad status code returned: %d", resp.StatusCode)
	}
}

func doGet(t *testing.T, URL, query string) string {
	v, _ := url.Parse(URL)
	v.RawQuery = url.Values{"q": []string{query}}.Encode()

	resp, err := http.Get(v.String())
	if err != nil {
		t.Fatalf("Failed to execute query '%s': %s", query, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Couldn't read body of response: %s", err.Error())
	}
	return string(body)
}

// createCluster creates a cluster, numNodes in size, using path for storage
// for all nodes. The first node in the returned slice of nodes will be the
// cluster leader.
func createCluster(t *testing.T, numNodes int, host string, basePort int, path string) []*testServer {
	var nodes []*testServer

	// Create first differently, since it is the leader.
	nodePath := filepath.Join(path, "0")
	mustMkDirAll(nodePath)
	s := server.NewServer(nodePath, "db.sqlite", 100000, host, basePort)
	go func() {
		t.Fatal(s.ListenAndServe(""))
	}()
	nodes = append(nodes, &testServer{host: host, port: basePort, server: s})
	time.Sleep(spinUpDelay)

	// Create remaining nodes in cluster.
	for i := 1; i < numNodes; i++ {
		port := basePort + i
		nodePath := filepath.Join(path, strconv.Itoa(i))
		mustMkDirAll(nodePath)

		s := server.NewServer(nodePath, "db.sqlite", 100000, host, port)
		go func() {
			t.Fatal(s.ListenAndServe(host + ":" + strconv.Itoa(basePort)))
		}()
		nodes = append(nodes, &testServer{host: host, port: port, server: s})
		time.Sleep(spinUpDelay)
	}

	return nodes
}

// writeCluster POSTs the body against every node in the given cluster. Testing fails
// if the POST does not succeed (HTTP 200)
func writeCluster(t *testing.T, nodes []*testServer, body string) {
	for _, n := range nodes {
		doPost(t, n.URL(), body)
	}
}

// queryCluster performs the given query against each node in the given cluster.
// Testing fails if the response returned from any node does not match the expected
// string.
func queryCluster(t *testing.T, nodes []*testServer, query, expected string) {

}

func runTests(t *testing.T, nodes []*testServer) {
	if testing.Short() {
		t.Skip("skipping for short testing")
	}
	tests := []struct {
		name     string // Name of test, printed during testing.
		write    string // The query to POST.
		query    string // The query to GET. Ignored if post is set.
		expected string // Expected response, as a string. Ignored if not set.
	}{
		{
			name:  "create table",
			write: "CREATE TABLE foo (id integer not null primary key, name text)",
		},
		{
			name:  "select from empty table",
			query: "SELECT * FROM foo",
		},
		{
			name:  "insert one record",
			query: `INSERT INTO foo(name) VALUES("fiona")`,
		},
		{
			name:  "select after 1 record inserted",
			query: "SELECT * FROM foo",
		},
	}

	for _, tt := range tests {
		if tt.write != "" {
			t.Logf("Executing '%s'", tt.write)
			writeCluster(t, nodes[:1], tt.write)
		} else {
			t.Logf("Executing '%s'", tt.query)
			queryCluster(t, nodes, tt.query, tt.expected)
		}
	}
}

func TestOneNode_Test(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	nodes := createCluster(t, 1, "localhost", 8000, path)
	t.Logf("1 node cluster created in %s", path)

	runTests(t, nodes)
}

func TestThreeNode_Test(t *testing.T) {
	t.Skip()
	path := tempfile()
	defer os.RemoveAll(path)

	nodes := createCluster(t, 3, "localhost", 8100, path)
	t.Logf("3 node cluster created in %s", path)

	runTests(t, nodes)
}

// tempfile returns a temporary path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "rqlite_")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

// mustMkDirAll makes the requested directory, including parents, or panics.
func mustMkDirAll(path string) {
	if err := os.MkdirAll(path, 0755); err != nil {
		panic(fmt.Sprintf("unable to create directory at %s", path))
	}
}
