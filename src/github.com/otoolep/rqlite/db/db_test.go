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

func (s *DbSuite) Test_TableCreation(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)
	db := New(path.Join(dir, "test_db"))
	defer db.Close()

	err = db.Execute("create table foo (id integer not null primary key, name text)")
	c.Assert(err, IsNil)

	r, err := db.Query("SELECT * FROM foo")
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 0)
}

func (s *DbSuite) Test_SimpleStatements(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)
	db := New(path.Join(dir, "test_db"))
	defer db.Close()

	err = db.Execute("create table foo (id integer not null primary key, name text)")
	c.Assert(err, IsNil)

	err = db.Execute("INSERT INTO foo(name) VALUES(\"fiona\")")
	c.Assert(err, IsNil)
	r, err := db.Query("SELECT * FROM foo")
	c.Assert(len(r), Equals, 1)
	c.Assert(r[0]["id"], Equals, "1")
	c.Assert(r[0]["name"], Equals, "fiona")

	err = db.Execute("INSERT INTO foo(name) VALUES(\"dana\")")
	c.Assert(err, IsNil)
	r, err = db.Query("SELECT * FROM foo")
	c.Assert(len(r), Equals, 2)
	c.Assert(r[1]["id"], Equals, "2")
	c.Assert(r[1]["name"], Equals, "dana")

	err = db.Execute("UPDATE foo SET Name='Who knows?' WHERE Id=1")
	c.Assert(err, IsNil)
	r, err = db.Query("SELECT * FROM foo")
	c.Assert(len(r), Equals, 2)
	c.Assert(r[0]["id"], Equals, "1")
	c.Assert(r[0]["name"], Equals, "Who knows?")

	err = db.Execute("DELETE FROM foo WHERE Id=2")
	c.Assert(err, IsNil)
	r, err = db.Query("SELECT * FROM foo")
	c.Assert(len(r), Equals, 1)
	c.Assert(r[0]["id"], Equals, "1")
	c.Assert(r[0]["name"], Equals, "Who knows?")

	err = db.Execute("DELETE FROM foo WHERE Id=1")
	c.Assert(err, IsNil)

	for i := 0; i < 10; i++ {
		_ = db.Execute("INSERT INTO foo(name) VALUES(\"philip\")")
	}
	r, err = db.Query("SELECT name FROM foo")
	c.Assert(len(r), Equals, 10)
	for i := range r {
		c.Assert(r[i]["name"], Equals, "philip")
	}
}

func (s *DbSuite) Test_FailingSimpleStatements(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)
	db := New(path.Join(dir, "test_db"))
	defer db.Close()

	err = db.Execute("INSERT INTO foo(name) VALUES(\"fiona\")")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "no such table: foo")

	err = db.Execute("create table foo (id integer not null primary key, name text)")
	c.Assert(err, IsNil)
	err = db.Execute("create table foo (id integer not null primary key, name text)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "table foo already exists")

	err = db.Execute("INSERT INTO foo(id, name) VALUES(11, \"fiona\")")
	c.Assert(err, IsNil)
	err = db.Execute("INSERT INTO foo(id, name) VALUES(11, \"fiona\")")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "UNIQUE constraint failed: foo.id")

	err = db.Execute("SELECT * FROM bar")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "no such table: bar")

	err = db.Execute("utter nonsense")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "near \"utter\": syntax error")
}

func (s *DbSuite) Test_SimpleTransactions(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)
	db := New(path.Join(dir, "test_db"))
	defer db.Close()

	err = db.Execute("create table foo (id integer not null primary key, name text)")
	c.Assert(err, IsNil)

	err = db.StartTransaction()
	c.Assert(err, IsNil)
	for i := 0; i < 10; i++ {
		_ = db.Execute("INSERT INTO foo(name) VALUES(\"philip\")")
	}
	err = db.CommitTransaction()
	c.Assert(err, IsNil)

	r, err := db.Query("SELECT name FROM foo")
	c.Assert(len(r), Equals, 10)
	for i := range r {
		c.Assert(r[i]["name"], Equals, "philip")
	}

	err = db.StartTransaction()
	c.Assert(err, IsNil)
	for i := 0; i < 10; i++ {
		_ = db.Execute("INSERT INTO foo(name) VALUES(\"philip\")")
	}
	err = db.RollbackTransaction()
	c.Assert(err, IsNil)

	r, err = db.Query("SELECT name FROM foo")
	c.Assert(len(r), Equals, 10)
	for i := range r {
		c.Assert(r[i]["name"], Equals, "philip")
	}
}
