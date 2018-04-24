package store

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/rqlite/rqlite/testdata/chinook"
)

func Test_OpenStoreSingleNode(t *testing.T) {
	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}

	s.WaitForLeader(10 * time.Second)
	if got, exp := s.LeaderAddr(), s.Addr(); got != exp {
		t.Fatalf("wrong leader address returned, got: %s, exp %s", got, exp)
	}
	id, err := s.LeaderID()
	if err != nil {
		t.Fatalf("failed to retrieve leader ID: %s", err.Error())
	}
	if got, exp := id, s.raftID; got != exp {
		t.Fatalf("wrong leader ID returned, got: %s, exp %s", got, exp)
	}
}

func Test_OpenStoreCloseSingleNode(t *testing.T) {
	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	s.WaitForLeader(10 * time.Second)
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}
}

func Test_SingleNodeInMemExecuteQuery(t *testing.T) {
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
	_, err := s.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	r, err := s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_SingleNodeInMemExecuteQueryFail ensures database level errors are presented by the store.
func Test_SingleNodeInMemExecuteQueryFail(t *testing.T) {
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
	r, err := s.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if exp, got := "no such table: foo", r[0].Error; exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeFileExecuteQuery(t *testing.T) {
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
	_, err := s.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	r, err := s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeExecuteQueryTx(t *testing.T) {
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
	_, err := s.Execute(&ExecuteRequest{queries, false, true})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	r, err := s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, true, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, true, Weak})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	r, err = s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	_, err = s.Execute(&ExecuteRequest{queries, false, true})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
}

func Test_SingleNodeLoad(t *testing.T) {
	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	dump := `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE foo (id integer not null primary key, name text);
INSERT INTO "foo" VALUES(1,'fiona');
COMMIT;
`
	_, err := s.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load simple dump: %s", err.Error())
	}

	// Check that data were loaded correctly.
	r, err := s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeSingleCommandTrigger(t *testing.T) {
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
	_, err := s.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load dump with trigger: %s", err.Error())
	}

	// Check that the VIEW and TRIGGER are OK by using both.
	r, err := s.Execute(&ExecuteRequest{[]string{`INSERT INTO foobar VALUES('jason', 16)`}, false, true})
	if err != nil {
		t.Fatalf("failed to insert into view on single node: %s", err.Error())
	}
	if exp, got := int64(3), r[0].LastInsertID; exp != got {
		t.Fatalf("unexpected results for query\nexp: %d\ngot: %d", exp, got)
	}
}

func Test_SingleNodeLoadNoStatements(t *testing.T) {
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
	_, err := s.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load dump with no commands: %s", err.Error())
	}
}

func Test_SingleNodeLoadEmpty(t *testing.T) {
	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	dump := ``
	_, err := s.Execute(&ExecuteRequest{[]string{dump}, false, false})
	if err != nil {
		t.Fatalf("failed to load empty dump: %s", err.Error())
	}
}

func Test_SingleNodeLoadChinook(t *testing.T) {
	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	_, err := s.Execute(&ExecuteRequest{[]string{chinook.DB}, false, false})
	if err != nil {
		t.Fatalf("failed to load chinook dump: %s", err.Error())
	}

	// Check that data were loaded correctly.

	r, err := s.Query(&QueryRequest{[]string{`SELECT count(*) FROM track`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[3503]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = s.Query(&QueryRequest{[]string{`SELECT count(*) FROM album`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[347]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = s.Query(&QueryRequest{[]string{`SELECT count(*) FROM artist`}, false, true, Strong})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[275]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

}

func Test_MultiNodeJoinRemove(t *testing.T) {
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

	// Get sorted list of cluster nodes.
	storeNodes := []string{s0.ID(), s1.ID()}
	sort.StringSlice(storeNodes).Sort()

	// Join the second node to the first.
	if err := s0.Join(s1.ID(), s1.Addr(), nil); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	s1.WaitForLeader(10 * time.Second)

	// Check leader state on follower.
	if got, exp := s1.LeaderAddr(), s0.Addr(); got != exp {
		t.Fatalf("wrong leader address returned, got: %s, exp %s", got, exp)
	}
	id, err := s1.LeaderID()
	if err != nil {
		t.Fatalf("failed to retrieve leader ID: %s", err.Error())
	}
	if got, exp := id, s0.raftID; got != exp {
		t.Fatalf("wrong leader ID returned, got: %s, exp %s", got, exp)
	}

	nodes, err := s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}

	if len(nodes) != len(storeNodes) {
		t.Fatalf("size of cluster is not correct")
	}
	if storeNodes[0] != nodes[0].ID || storeNodes[1] != nodes[1].ID {
		t.Fatalf("cluster does not have correct nodes")
	}

	// Remove a node.
	if err := s0.Remove(s1.ID()); err != nil {
		t.Fatalf("failed to remove %s from cluster: %s", s1.ID(), err.Error())
	}

	nodes, err = s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes post remove: %s", err.Error())
	}
	if len(nodes) != 1 {
		t.Fatalf("size of cluster is not correct post remove")
	}
	if s0.ID() != nodes[0].ID {
		t.Fatalf("cluster does not have correct nodes post remove")
	}
}

func Test_MultiNodeExecuteQuery(t *testing.T) {
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
	_, err := s0.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	r, err := s0.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Wait until the 3 log entries have been applied to the follower,
	// and then query.
	if err := s1.WaitForAppliedIndex(3, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}
	r, err = s1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Weak})
	if err == nil {
		t.Fatalf("successfully queried non-leader node")
	}
	r, err = s1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Strong})
	if err == nil {
		t.Fatalf("successfully queried non-leader node")
	}
	r, err = s1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Kill the leader and check that query can still be satisfied via None consistency.
	s0.Close(true)
	r, err = s1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, Strong})
	if err == nil {
		t.Fatalf("successfully queried non-leader node: %s", err.Error())
	}
	r, err = s1.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query node with None consistency: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeSnapshotOnDisk(t *testing.T) {
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
	_, err := s.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, err = s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}

	// Snap the node and write to disk.
	f, err := s.Snapshot()
	if err != nil {
		t.Fatalf("failed to snapshot node: %s", err.Error())
	}

	snapDir := mustTempDir()
	defer os.RemoveAll(snapDir)
	snapFile, err := os.Create(filepath.Join(snapDir, "snapshot"))
	if err != nil {
		t.Fatalf("failed to create snapshot file: %s", err.Error())
	}
	sink := &mockSnapshotSink{snapFile}
	if err := f.Persist(sink); err != nil {
		t.Fatalf("failed to persist snapshot to disk: %s", err.Error())
	}

	// Check restoration.
	snapFile, err = os.Open(filepath.Join(snapDir, "snapshot"))
	if err != nil {
		t.Fatalf("failed to open snapshot file: %s", err.Error())
	}
	if err := s.Restore(snapFile); err != nil {
		t.Fatalf("failed to restore snapshot from disk: %s", err.Error())
	}

	// Ensure database is back in the correct state.
	r, err := s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeSnapshotInMem(t *testing.T) {
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
	_, err := s.Execute(&ExecuteRequest{queries, false, false})
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, err = s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}

	// Snap the node and write to disk.
	f, err := s.Snapshot()
	if err != nil {
		t.Fatalf("failed to snapshot node: %s", err.Error())
	}

	snapDir := mustTempDir()
	defer os.RemoveAll(snapDir)
	snapFile, err := os.Create(filepath.Join(snapDir, "snapshot"))
	if err != nil {
		t.Fatalf("failed to create snapshot file: %s", err.Error())
	}
	sink := &mockSnapshotSink{snapFile}
	if err := f.Persist(sink); err != nil {
		t.Fatalf("failed to persist snapshot to disk: %s", err.Error())
	}

	// Check restoration.
	snapFile, err = os.Open(filepath.Join(snapDir, "snapshot"))
	if err != nil {
		t.Fatalf("failed to open snapshot file: %s", err.Error())
	}
	if err := s.Restore(snapFile); err != nil {
		t.Fatalf("failed to restore snapshot from disk: %s", err.Error())
	}

	// Ensure database is back in the correct state.
	r, err := s.Query(&QueryRequest{[]string{`SELECT * FROM foo`}, false, false, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_StoreLogTruncationMultinode(t *testing.T) {
	s0 := mustNewStore(true)
	defer os.RemoveAll(s0.Path())
	s0.SnapshotThreshold = 4
	s0.SnapshotInterval = 100 * time.Millisecond

	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	s0.WaitForLeader(10 * time.Second)
	nSnaps := stats.Get(numSnaphots).String()

	// Write more than s.SnapshotThreshold statements.
	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(3, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(5, "fiona")`,
	}
	for i := range queries {
		_, err := s0.Execute(&ExecuteRequest{[]string{queries[i]}, false, false})
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}

	// Wait for the snapshot to happen and log to be truncated.
	for {
		time.Sleep(1000 * time.Millisecond)
		if stats.Get(numSnaphots).String() != nSnaps {
			// It's changed, so a snap and truncate has happened.
			break
		}
	}

	// Fire up new node and ensure it picks up all changes. This will
	// involve getting a snapshot and truncated log.
	s1 := mustNewStore(true)
	if err := s1.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	// Join the second node to the first.
	if err := s0.Join(s1.ID(), s1.Addr(), nil); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	s1.WaitForLeader(10 * time.Second)
	// Wait until the log entries have been applied to the follower,
	// and then query.
	if err := s1.WaitForAppliedIndex(10, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}
	r, err := s1.Query(&QueryRequest{[]string{`SELECT count(*) FROM foo`}, false, true, None})
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[5]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_MetadataMultinode(t *testing.T) {
	s0 := mustNewStore(true)
	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	s0.WaitForLeader(10 * time.Second)
	s1 := mustNewStore(true)
	if err := s1.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)
	s1.WaitForLeader(10 * time.Second)

	if s0.Metadata(s0.raftID, "foo") != "" {
		t.Fatal("nonexistent metadata foo found")
	}
	if s0.Metadata("nonsense", "foo") != "" {
		t.Fatal("nonexistent metadata foo found for nonexistent node")
	}

	if err := s0.SetMetadata(map[string]string{"foo": "bar"}); err != nil {
		t.Fatalf("failed to set metadata: %s", err.Error())
	}
	if s0.Metadata(s0.raftID, "foo") != "bar" {
		t.Fatal("key foo not found")
	}
	if s0.Metadata("nonsense", "foo") != "" {
		t.Fatal("nonexistent metadata foo found for nonexistent node")
	}

	// Join the second node to the first.
	meta := map[string]string{"baz": "qux"}
	if err := s0.Join(s1.ID(), s1.Addr(), meta); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	s1.WaitForLeader(10 * time.Second)
	// Wait until the log entries have been applied to the follower,
	// and then query.
	if err := s1.WaitForAppliedIndex(5, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}

	if s1.Metadata(s0.raftID, "foo") != "bar" {
		t.Fatal("key foo not found for s0")
	}
	if s0.Metadata(s1.raftID, "baz") != "qux" {
		t.Fatal("key baz not found for s1")
	}

	// Remove a node.
	if err := s0.Remove(s1.ID()); err != nil {
		t.Fatalf("failed to remove %s from cluster: %s", s1.ID(), err.Error())
	}
	if s1.Metadata(s0.raftID, "foo") != "bar" {
		t.Fatal("key foo not found for s0")
	}
	if s0.Metadata(s1.raftID, "baz") != "" {
		t.Fatal("key baz found for removed node s1")
	}
}

func Test_IsLeader(t *testing.T) {
	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	if !s.IsLeader() {
		t.Fatalf("single node is not leader!")
	}
}

func Test_State(t *testing.T) {
	s := mustNewStore(true)
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	state := s.State()
	if state != Leader {
		t.Fatalf("single node returned incorrect state (not Leader): %v", s)
	}
}

func mustNewStore(inmem bool) *Store {
	path := mustTempDir()
	defer os.RemoveAll(path)

	cfg := NewDBConfig("", inmem)
	s := New(mustMockLister("localhost:0"), &StoreConfig{
		DBConf: cfg,
		Dir:    path,
		ID:     path, // Could be any unique string.
	})
	if s == nil {
		panic("failed to create new store")
	}
	return s
}

type mockSnapshotSink struct {
	*os.File
}

func (m *mockSnapshotSink) ID() string {
	return "1"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

type mockTransport struct {
	ln net.Listener
}

type mockListener struct {
	ln net.Listener
}

func mustMockLister(addr string) Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic("failed to create new listner")
	}
	return &mockListener{ln}
}

func (m *mockListener) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}

func (m *mockListener) Accept() (net.Conn, error) { return m.ln.Accept() }

func (m *mockListener) Close() error { return m.ln.Close() }

func (m *mockListener) Addr() net.Addr { return m.ln.Addr() }

func mustTempDir() string {
	var err error
	path, err := ioutil.TempDir("", "rqlilte-test-")
	if err != nil {
		panic("failed to create temp dir")
	}
	return path
}

func asJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic("failed to JSON marshal value")
	}
	return string(b)
}
