package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	. "gopkg.in/check.v1"
)

const (
	host      = "localhost"
	port      = 4001
	snapAfter = 1000
	dbfile    = "rqlite-test"
)

type SingleServerSuite struct{}

var _ = Suite(&SingleServerSuite{})

func getEndpoint(endpoint string) (*http.Response, error) {
	url := fmt.Sprintf("http://%s:%d%s", host, port, endpoint)
	return http.Get(url)
}

func getEndpointBody(endpoint string, body string) (*http.Response, error) {
	var jsonStr = []byte(body)
	url := fmt.Sprintf("http://%s:%d%s", host, port, endpoint)
	req, err := http.NewRequest("GET", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	return client.Do(req)
}

func postEndpoint(endpoint string, body string) (*http.Response, error) {
	var jsonStr = []byte(body)
	url := fmt.Sprintf("http://%s:%d%s", host, port, endpoint)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	return client.Do(req)
}

func isJSONBody(res *http.Response) bool {
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return false
	}

	var o interface{}
	err = json.Unmarshal(b, &o)
	if err != nil {
		return false
	}

	return true
}

func (s *SingleServerSuite) Test_SingleServer(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	server := NewServer(dir, dbfile, snapAfter, host, port)
	c.Assert(server, NotNil)
	go func() { server.ListenAndServe("") }()

	// Wait to ensure server is up. This is not ideal, and the server should
	// really use a channel to flag it is ready.
	time.Sleep(1 * time.Second)

	// Sanity-check admin API endpoints
	var res *http.Response
	res, err = getEndpoint("/statistics")
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)
	c.Assert(isJSONBody(res), Equals, true)

	res, err = getEndpoint("/diagnostics")
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)
	c.Assert(isJSONBody(res), Equals, true)

	res, err = getEndpoint("/raft")
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)
	c.Assert(isJSONBody(res), Equals, true)

	// Create a database.
	res, err = postEndpoint("/db", "CREATE TABLE foo (id integer not null primary key, name text)")
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)

	// Data write.
	res, err = postEndpoint("/db", "INSERT INTO foo(name) VALUES(\"fiona\")")
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)

	// Data read
	res, err = getEndpointBody("/db", "SELECT * from foo")
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)
	c.Assert(isJSONBody(res), Equals, true)
}
