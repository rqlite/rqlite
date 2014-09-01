package server

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type SnapshotSuite struct{}

var _ = Suite(&SnapshotSuite{})

func (s *SnapshotSuite) Test_Snapshot(c *C) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	path1 := path.Join(dir, "test_db1")
	path2 := path.Join(dir, "test_db2")
	defer os.RemoveAll(dir)

	// Create a small database.
	dbc, err := sql.Open("sqlite3", path1)
	c.Assert(err, IsNil)
	_, err = dbc.Exec("create table foo (id integer not null primary key, name text)")
	c.Assert(err, IsNil)
	_, err = dbc.Exec("INSERT INTO foo(name) VALUES(\"fiona\")")
	dbc.Close()

	// Snapshot it.
	snapper1 := NewDbStateMachine(path1)
	c.Assert(snapper1, NotNil)
	snap, err := snapper1.Save()
	c.Assert(err, IsNil)

	// Save it to a different location.
	snapper2 := NewDbStateMachine(path2)
	c.Assert(snapper2, NotNil)
	err = snapper2.Recovery(snap)
	c.Assert(err, IsNil)

	// Confirm two files are byte-for-byte identical.
	b1, err := ioutil.ReadFile(path1)
	c.Assert(err, IsNil)
	b2, err := ioutil.ReadFile(path2)
	c.Assert(err, IsNil)
	c.Assert(reflect.DeepEqual(b1, b2), Equals, true)

	// Open database using snapshot copy.
	dbc, err = sql.Open("sqlite3", path2)
	c.Assert(err, IsNil)
	rows, err := dbc.Query("SELECT name FROM foo")
	c.Assert(err, IsNil)

	var nrows int
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		c.Assert(err, IsNil)
		c.Assert(name, Equals, "fiona")
		nrows++
	}
	c.Assert(nrows, Equals, 1)
	dbc.Close()
}
