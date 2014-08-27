package command

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/otoolep/rqlite/db"
	. "gopkg.in/check.v1"
)

// Mocked out database provider.
type DbProvider struct {
	s interface{}
}

func NewDbProvider(d *db.DB) *DbProvider {
	return &DbProvider{
		s: d,
	}
	return nil
}

func (d *DbProvider) Context() interface{} {
	return d.s
}

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type CommandSuite struct{}

var _ = Suite(&CommandSuite{})

/*
 * Command-layer tests
 */

func (s *CommandSuite) Test_SimpleWrite(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)
	c.Assert(err, IsNil)

	d := db.New(path.Join(dir, "test_db"))
	c.Assert(d, NotNil)

	provider := NewDbProvider(d)
	command := NewWriteCommand("create table foo (id integer not null primary key, name text)")

	_, err = command.Apply(provider)
	c.Assert(err, IsNil)
}

func (s *CommandSuite) Test_SuccessfulTransaction(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)
	c.Assert(err, IsNil)

	d := db.New(path.Join(dir, "test_db"))
	c.Assert(d, NotNil)

	stmts := []string{
		"create table foo (id integer not null primary key, name text)",
	}

	provider := NewDbProvider(d)
	command := NewTransactionWriteCommandSet(stmts)
	c.Assert(command, NotNil)
	_, err = command.Apply(provider)
	c.Assert(err, IsNil)

	stmts = []string{
		"INSERT INTO foo(id, name) VALUES(11, \"philip\")",
		"INSERT INTO foo(id, name) VALUES(12, \"dana\")",
		"INSERT INTO foo(id, name) VALUES(13, \"fiona\")",
	}
	command = NewTransactionWriteCommandSet(stmts)
	c.Assert(command, NotNil)
	_, err = command.Apply(provider)
	c.Assert(err, IsNil)

	// Database should have 3 records.
	r, err := d.Query("SELECT name FROM foo")
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 3)
}

func (s *CommandSuite) Test_FailedTransaction(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)
	c.Assert(err, IsNil)

	d := db.New(path.Join(dir, "test_db"))
	c.Assert(d, NotNil)

	stmts := []string{
		"create table foo (id integer not null primary key, name text)",
	}

	provider := NewDbProvider(d)
	command := NewTransactionWriteCommandSet(stmts)
	c.Assert(command, NotNil)
	_, err = command.Apply(provider)
	c.Assert(err, IsNil)

	stmts = []string{
		"INSERT INTO foo(id, name) VALUES(11, \"philip\")",
		"INSERT INTO foo(id, name) VALUES(11, \"dana\")",
	}
	command = NewTransactionWriteCommandSet(stmts)
	c.Assert(command, NotNil)
	_, err = command.Apply(provider)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "UNIQUE constraint failed: foo.id")

	// Database should have no records.
	r, err := d.Query("SELECT name FROM foo")
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 0)
}
