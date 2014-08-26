package db

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type DbSuite struct{}

var _ = Suite(&DbSuite{})

/*
 * Lowest-layer database tests
 */

func (s *DbSuite) Test_DbFileCreation(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)

	db := New(path.Join(dir, "test_db"))
	c.Assert(db, NotNil)
	err = db.Close()
	c.Assert(err, IsNil)
}

func (s *DbSuite) Test_DbTableCreation(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)
	db := New(path.Join(dir, "test_db"))
	defer db.Close()

	err = db.Execute("create table foo (id integer not null primary key, name text)")
	c.Assert(err, IsNil)

	r, err := db.Query("select * from foo")
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 0)
}

func (s *DbSuite) Test_DbTableSimpleStatements(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)
	db := New(path.Join(dir, "test_db"))
	defer db.Close()

	err = db.Execute("create table foo (id integer not null primary key, name text)")
	c.Assert(err, IsNil)

	err = db.Execute("INSERT INTO foo(name) VALUES(\"fiona\")")
	c.Assert(err, IsNil)
	r, err := db.Query("select * from foo")
	c.Assert(len(r), Equals, 1)
	c.Assert(r[0]["id"], Equals, "1")
	c.Assert(r[0]["name"], Equals, "fiona")

	err = db.Execute("INSERT INTO foo(name) VALUES(\"dana\")")
	c.Assert(err, IsNil)
	r, err = db.Query("select * from foo")
	c.Assert(len(r), Equals, 2)
	c.Assert(r[1]["id"], Equals, "2")
	c.Assert(r[1]["name"], Equals, "dana")

	err = db.Execute("UPDATE foo SET Name='Who knows?' WHERE Id=1")
	c.Assert(err, IsNil)
	r, err = db.Query("select * from foo")
	c.Assert(len(r), Equals, 2)
	c.Assert(r[0]["id"], Equals, "1")
	c.Assert(r[0]["name"], Equals, "Who knows?")

	err = db.Execute("DELETE FROM foo WHERE Id=2")
	c.Assert(err, IsNil)
	r, err = db.Query("select * from foo")
	c.Assert(len(r), Equals, 1)
	c.Assert(r[0]["id"], Equals, "1")
	c.Assert(r[0]["name"], Equals, "Who knows?")

	err = db.Execute("DELETE FROM foo WHERE Id=1")
	c.Assert(err, IsNil)

	_ = db.Execute("INSERT INTO foo(name) VALUES(\"philip\")")
	_ = db.Execute("INSERT INTO foo(name) VALUES(\"philip\")")
	_ = db.Execute("INSERT INTO foo(name) VALUES(\"philip\")")
	r, err = db.Query("select name from foo")
	c.Assert(len(r), Equals, 3)
	for i := range r {
		c.Assert(r[i]["name"], Equals, "philip")
	}
}
