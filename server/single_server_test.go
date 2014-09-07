package server

import (
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

func getEndpoint(endpoint string) (res *http.Response, err error) {
	url := fmt.Sprintf("http://%s:%d%s", host, port, endpoint)
	return http.Get(url)
}

func isJsonBody(res *http.Response) bool {
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
	c.Assert(isJsonBody(res), Equals, true)

	res, err = getEndpoint("/diagnostics")
	c.Assert(err, IsNil)
	c.Assert(isJsonBody(res), Equals, true)

	res, err = getEndpoint("/raft")
	c.Assert(err, IsNil)
	c.Assert(isJsonBody(res), Equals, true)
}
