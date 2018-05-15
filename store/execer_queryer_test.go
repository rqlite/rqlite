package store

import (
	"os"
	"testing"
	"time"
)

type testF func(t *testing.T, eq ExecerQueryer)

func TestStoreOnDisk(t *testing.T) {
	t.Parallel()
	for _, f := range testfunctions {
		func() {
			s := mustNewStore(false)
			if err := s.Open(true); err != nil {
				t.Fatalf("failed to open single-node store: %s", err.Error())
			}
			defer s.Close(true)
			defer os.RemoveAll(s.Path())
			s.WaitForLeader(10 * time.Second)
			f(t, s)
		}()
	}
}

func TestStoreInMem(t *testing.T) {
	t.Parallel()
	for _, f := range testfunctions {
		func() {
			s := mustNewStore(true)
			if err := s.Open(true); err != nil {
				t.Fatalf("failed to open single-node store: %s", err.Error())
			}
			defer s.Close(true)
			defer os.RemoveAll(s.Path())
			s.WaitForLeader(10 * time.Second)
			f(t, s)
		}()
	}
}

func TestStoreConnectionOnDisk(t *testing.T) {
	t.Parallel()
	for _, f := range testfunctions {
		func() {
			s := mustNewStore(false)
			if err := s.Open(true); err != nil {
				t.Fatalf("failed to open single-node store: %s", err.Error())
			}
			defer s.Close(true)
			defer os.RemoveAll(s.Path())
			s.WaitForLeader(10 * time.Second)
			c := mustNewConnection(s)
			defer c.Close()
			f(t, c)
		}()
	}
}

func TestStoreConnectionInMem(t *testing.T) {
	t.Parallel()
	for _, f := range testfunctions {
		func() {
			s := mustNewStore(true)
			if err := s.Open(true); err != nil {
				t.Fatalf("failed to open single-node store: %s", err.Error())
			}
			defer s.Close(true)
			defer os.RemoveAll(s.Path())
			s.WaitForLeader(10 * time.Second)
			c := mustNewConnection(s)
			defer c.Close()
			f(t, c)
		}()
	}
}

var testfunctions []testF = []testF{
	testSimpleExecuteQuery,
	testSimpleExecuteQueryFail,
	testSingleCommandTrigger,
	testSimpleLoadNoStatements,
	testSimpleLoadEmpty,
	testLoadAbortOnError,
}

func testSimpleExecuteQuery(t *testing.T, eq ExecerQueryer) {
	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}
	re, err := eq.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if exp, got := `[{},{"last_insert_id":1,"rows_affected":1}]`, asJSON(re.Results); exp != got {
		t.Fatalf("unexpected results for execute\nexp: %s\ngot: %s", exp, got)
	}
	raftIndex := re.Raft.Index

	// Ensure Raft index is non-zero only for Strong queries (which are the only ones
	// that go through the log).
	for lvl, rr := range map[ConsistencyLevel]*RaftResponse{
		None:   nil,
		Weak:   nil,
		Strong: {Index: raftIndex + 1},
	} {

		rq, err := eq.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, lvl})
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

func testSimpleExecuteQueryFail(t *testing.T, eq ExecerQueryer) {
	queries := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}
	r, err := eq.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if exp, got := "no such table: foo", r.Results[0].Error; exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleExecuteQueryTx(t *testing.T, eq ExecerQueryer) {
	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}
	_, err := eq.Execute(&ExecuteRequest{queries, false, true})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	r, err := eq.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, true, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = eq.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, true, Weak})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = eq.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r.Rows[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r.Rows[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	_, err = eq.Execute(&ExecuteRequest{queries, false, true})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
}

func testSingleCommandTrigger(t *testing.T, eq ExecerQueryer) {
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
	_, err := eq.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load dump with trigger: %s", err.Error())
	}

	// Check that the VIEW and TRIGGER are OK by using both.
	r, err := eq.Execute(&ExecuteRequest{[]string{`INSERT INTO foobar VALUES('jason', 16)`}, false, true})
	if err != nil {
		t.Fatalf("failed to insert into view on single node: %s", err.Error())
	}
	if exp, got := int64(3), r.Results[0].LastInsertID; exp != got {
		t.Fatalf("unexpected results for query\nexp: %d\ngot: %d", exp, got)
	}
}

func testSimpleLoadNoStatements(t *testing.T, eq ExecerQueryer) {
	dump := `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
COMMIT;
`
	_, err := eq.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load dump with no commands: %s", err.Error())
	}
}

func testSimpleLoadEmpty(t *testing.T, eq ExecerQueryer) {
	_, err := eq.Execute(&ExecuteRequest{[]string{``}, false, false})
	if err != nil {
		t.Fatalf("failed to load dump with no commands: %s", err.Error())
	}
}

func testLoadAbortOnError(t *testing.T, eq ExecerQueryer) {
	dump := `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT);
COMMIT;
`

	r, err := eq.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load commands: %s", err.Error())
	}
	if r.Results[0].Error != "" {
		t.Fatalf("error received creating table: %s", r.Results[0].Error)
	}

	r, err = eq.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load commands: %s", err.Error())
	}
	if r.Results[0].Error != "table foo already exists" {
		t.Fatalf("received wrong error message: %s", r.Results[0].Error)
	}

	r, err = eq.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load commands: %s", err.Error())
	}
	if r.Results[0].Error != "cannot start a transaction within a transaction" {
		t.Fatalf("received wrong error message: %s", r.Results[0].Error)
	}

	r, err = eq.ExecuteOrAbort(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load commands: %s", err.Error())
	}
	if r.Results[0].Error != "cannot start a transaction within a transaction" {
		t.Fatalf("received wrong error message: %s", r.Results[0].Error)
	}

	r, err = eq.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load commands: %s", err.Error())
	}
	if r.Results[0].Error != "table foo already exists" {
		t.Fatalf("received wrong error message: %s", r.Results[0].Error)
	}
}
