package store

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/testdata/chinook"
)

func Test_SingleNodeInMemExecuteQuery(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}
	c := mustNewConnection(s)
	defer c.Close()
	re, err := c.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if exp, got := `[{},{"last_insert_id":1,"rows_affected":1}]`, asJSON(re.Results); exp != got {
		t.Fatalf("unexpected results for execute\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := uint64(4), re.Raft.Index; exp != got {
		t.Fatalf("unexpected Raft index received\nexp: %d\ngot: %d", exp, got)
	}

	// Ensure Raft index is non-zero only for Strong queries (which are the only ones
	// that go through the log).
	for lvl, rr := range map[ConsistencyLevel]*RaftResponse{
		None:   nil,
		Weak:   nil,
		Strong: {Index: 5},
	} {

		rq, err := c.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, lvl})
		if err != nil {
			t.Fatalf("failed to query single node: %s", err.Error())
		}
		if exp, got := `["id","name"]`, asJSON(rq.Rows[0].Columns); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[1,"fiona"]]`, asJSON(rq.Rows[0].Values); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
		if rr != nil && rr.Index != rq.Raft.Index {
			t.Fatalf("unexpected Raft index received\nexp: %d\ngot: %d", rr.Index, rq.Raft.Index)
		}
	}
}

// Test_SingleNodeInMemExecuteQueryFail ensures database level errors are presented by the store.
func Test_SingleNodeInMemExecuteQueryFail(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	queries := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}
	c := mustNewConnection(s)
	defer c.Close()
	r, err := c.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if exp, got := "no such table: foo", r.Results[0].Error; exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeFileExecuteQuery(t *testing.T) {
	t.Parallel()

	s := mustNewStore(false)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}
	c := mustNewConnection(s)
	defer c.Close()
	_, err := c.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	r, err := c.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = c.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Weak})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = c.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeExecuteQueryTx(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}
	c := mustNewConnection(s)
	defer c.Close()
	_, err := c.Execute(&ExecuteRequest{queries, false, true})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	r, err := c.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, true, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = c.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, true, Weak})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = c.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	_, err = c.Execute(&ExecuteRequest{queries, false, true})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
}

func Test_SingleNodeSingleCommandTrigger(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	dump := `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE foo (id integer primary key asc, name text);
INSERT INTO "foo" VALUES(1,'bob');
INSERT INTO "foo" VALUES(2,'alice');
INSERT INTO "foo" VALUES(3,'eve');
CREATE TABLE bar (nameid integer, age integer);
INSERT INTO "bar" VALUES(1,44);
INSERT INTO "bar" VALUES(2,46);
INSERT INTO "bar" VALUES(3,8);
CREATE VIEW foobar as select name as Person, Age as age from foo inner join bar on foo.id == bar.nameid;
CREATE TRIGGER new_foobar instead of insert on foobar begin insert into foo (name) values (new.Person); insert into bar (nameid, age) values ((select id from foo where name == new.Person), new.Age); end;
COMMIT;
`

	c := mustNewConnection(s)
	defer c.Close()

	_, err := c.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load dump with trigger: %s", err.Error())
	}

	// Check that the VIEW and TRIGGER are OK by using both.
	r, err := c.Execute(&ExecuteRequest{[]string{`INSERT INTO foobar VALUES('jason', 16)`}, false, true})
	if err != nil {
		t.Fatalf("failed to insert into view on single node: %s", err.Error())
	}
	if exp, got := int64(3), r.Results[0].LastInsertID; exp != got {
		t.Fatalf("unexpected results for query\nexp: %d\ngot: %d", exp, got)
	}
}

func Test_SingleNodeLoadNoStatements(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	dump := `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
COMMIT;
`

	c := mustNewConnection(s)
	defer c.Close()
	_, err := c.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load dump with no commands: %s", err.Error())
	}
}

func Test_SingleNodeLoadEmpty(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	dump := ``
	c := mustNewConnection(s)
	defer c.Close()
	_, err := c.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load empty dump: %s", err.Error())
	}
}

func Test_SingleNodeLoadAbortOnError(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	dump := `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT);
COMMIT;
`

	c := mustNewConnection(s)
	defer c.Close()

	r, err := c.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load commands: %s", err.Error())
	}
	if r.Results[0].Error != "" {
		t.Fatalf("error received creating table: %s", r.Results[0].Error)
	}

	r, err = c.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load commands: %s", err.Error())
	}
	if r.Results[0].Error != "table foo already exists" {
		t.Fatalf("received wrong error message: %s", r.Results[0].Error)
	}

	r, err = c.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load commands: %s", err.Error())
	}
	if r.Results[0].Error != "cannot start a transaction within a transaction" {
		t.Fatalf("received wrong error message: %s", r.Results[0].Error)
	}

	r, err = c.ExecuteOrAbort(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load commands: %s", err.Error())
	}
	if r.Results[0].Error != "cannot start a transaction within a transaction" {
		t.Fatalf("received wrong error message: %s", r.Results[0].Error)
	}

	r, err = c.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load commands: %s", err.Error())
	}
	if r.Results[0].Error != "table foo already exists" {
		t.Fatalf("received wrong error message: %s", r.Results[0].Error)
	}
}

func Test_SingleNodeLoadChinook(t *testing.T) {
	t.Parallel()

	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	c := mustNewConnection(s)
	defer c.Close()

	_, err := c.Execute(&ExecuteRequest{[]string{chinook.DB}, false, false})
	if err != nil {
		t.Fatalf("failed to load chinook dump: %s", err.Error())
	}

	// Check that data were loaded correctly.

	r, err := c.Query(&QueryRequest{[]string{`SELECT count(*) FROM track`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[3503]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = c.Query(&QueryRequest{[]string{`SELECT count(*) FROM album`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[347]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = c.Query(&QueryRequest{[]string{`SELECT count(*) FROM artist`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[275]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_MultiNodeExecuteQuery(t *testing.T) {
	t.Parallel()

	s0 := mustNewStore(true)
	defer os.RemoveAll(s0.Path())
	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s0.Close(true)
	s0.WaitForLeader(10 * time.Second)

	s1 := mustNewStore(true)
	defer os.RemoveAll(s1.Path())
	if err := s1.Open(false); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	// Join the second node to the first.
	if err := s0.Join(s1.ID(), s1.Addr(), nil); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}
	c0 := mustNewConnection(s0)
	defer c0.Close()

	_, err := c0.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	r, err := c0.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Wait until the 3 log entries have been applied to the follower,
	// and then query.
	c1 := mustNewConnection(s1)
	defer c1.Close()

	if err := s1.WaitForAppliedIndex(3, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}
	r, err = c1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Weak})
	if err == nil {
		t.Fatalf("successfully queried non-leader node")
	}
	r, err = c1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Strong})
	if err == nil {
		t.Fatalf("successfully queried non-leader node")
	}
	r, err = c1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Kill the leader and check that query can still be satisfied via None consistency.
	s0.Close(true)
	r, err = c1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Strong})
	if err == nil {
		t.Fatalf("successfully queried non-leader node: %s", err.Error())
	}
	r, err = c1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query node with None consistency: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func mustNewConnection(s *Store) *Connection {
	c, err := s.Connect()
	if err != nil {
		panic(fmt.Sprintf("failed to connect to store: %s", err.Error()))
	}
	return c
}
