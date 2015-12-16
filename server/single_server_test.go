package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
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

func getEndpointQuery(endpoint string, query string) (*http.Response, error) {
	q := url.Values{"q": []string{query}}
	v, _ := url.Parse(fmt.Sprintf("http://%s:%d%s", host, port, endpoint))
	v.RawQuery = q.Encode()

	req, err := http.Get(v.String())
	if err != nil {
		panic(err)
	}
	return req, nil
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

	// Data write New API
	res, err = postEndpoint("/db/bulk", "{\"data\":[{\"table\":\"foo\",\"payload\":[{\"name\":\"testdata5\",\"id\":\"1501\"},{\"name\":\"testdata5\",\"id\":\"1601\"},{\"name\":\"testdata5\",\"id\":\"1701\"}]}]}")
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)

	// Data read
	res, err = getEndpointQuery("/db", "SELECT * from foo")
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)
	c.Assert(isJSONBody(res), Equals, true)

	// Data read New API
	res, err = getEndpoint("/db/foo")
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, 200)
	c.Assert(isJSONBody(res), Equals, true)
}
