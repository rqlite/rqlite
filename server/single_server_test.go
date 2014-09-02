package server

import (
	"io/ioutil"
	"os"

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

func (s *SingleServerSuite) Test_SingleServer(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	server := NewServer(dir, dbfile, snapAfter, host, port)
	c.Assert(server, NotNil)
	go func() { server.ListenAndServe("") }()
}
