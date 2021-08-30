package system

import (
	"testing"
	"time"

	"github.com/rqlite/rqlite/cluster"
	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/tcp"
)

const fiveSec = 5 * time.Second

// Test_StoreClientSideBySide operates on the same store directly, and via
// RPC, and ensures results are the same for basically the same operation.
func Test_StoreClientSideBySide(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()
	leaderAddr, err := node.Store.LeaderAddr()
	if err != nil {
		t.Fatalf("failed to get leader Raft address: %s", err.Error())
	}

	client := cluster.NewClient(mustNewDialer(cluster.MuxClusterHeader, false, false))

	res, err := node.Store.Execute(executeRequestFromString("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"))
	if err != nil {
		t.Fatalf("failed to execute on local: %s", err.Error())
	}
	if exp, got := "[{}]", asJSON(res); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}
	res, err = client.Execute(executeRequestFromString("CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"), leaderAddr, fiveSec)
	if err != nil {
		t.Fatalf("failed to execute via remote: %s", err.Error())
	}
	if exp, got := "[{}]", asJSON(res); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}

	res, err = node.Store.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`))
	if err != nil {
		t.Fatalf("failed to execute on local: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}
	res, err = client.Execute(executeRequestFromString(`INSERT INTO bar(name) VALUES("fiona")`), leaderAddr, fiveSec)
	if err != nil {
		t.Fatalf("failed to execute via remote: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}

	rows, err := node.Store.Query(queryRequestFromString(`SELECT * FROM foo`))
	if err != nil {
		t.Fatalf("failed to query on local: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}
	rows, err = node.Store.Query(queryRequestFromString(`SELECT * FROM bar`))
	if err != nil {
		t.Fatalf("failed to query on local: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}

	rows, err = client.Query(queryRequestFromString(`SELECT * FROM foo`), leaderAddr, fiveSec)
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}
	rows, err = client.Query(queryRequestFromString(`SELECT * FROM bar`), leaderAddr, fiveSec)
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}

	rows, err = node.Store.Query(queryRequestFromString(`SELECT * FROM qux`))
	if err != nil {
		t.Fatalf("failed to query on local: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: qux"}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}
	rows, err = client.Query(queryRequestFromString(`SELECT * FROM qux`), leaderAddr, fiveSec)
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: qux"}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}
}

// Test_MultiNodeCluster tests formation of a 3-node cluster and query
// against all nodes to test requests are forwarded to leader transparently.
func Test_MultiNodeClusterRequestForwardOK(t *testing.T) {
	node1 := mustNewLeaderNode()
	defer node1.Deprovision()

	node2 := mustNewNode(false)
	defer node2.Deprovision()
	if err := node2.Join(node1); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err := node2.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c := Cluster{node1, node2}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	node3 := mustNewNode(false)
	defer node3.Deprovision()
	if err := node3.Join(leader); err != nil {
		t.Fatalf("node failed to join leader: %s", err.Error())
	}
	_, err = node3.WaitForLeader()
	if err != nil {
		t.Fatalf("failed waiting for leader: %s", err.Error())
	}

	// Get the new leader, in case it changed.
	c = Cluster{node1, node2, node3}
	leader, err = c.Leader()
	if err != nil {
		t.Fatalf("failed to find cluster leader: %s", err.Error())
	}

	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to get followers: %s", err.Error())
	}
	if len(followers) != 2 {
		t.Fatalf("got incorrect number of followers: %d", len(followers))
	}

	res, err := followers[0].Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `{"results":[{}]}`, res; exp != got {
		t.Fatalf("got incorrect response from follower exp: %s, got: %s", exp, got)
	}

	res, err = followers[1].Execute(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `{"results":[{"last_insert_id":1,"rows_affected":1}]}`, res; exp != got {
		t.Fatalf("got incorrect response from follower exp: %s, got: %s", exp, got)
	}

	res, err = leader.Execute(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `{"results":[{"last_insert_id":2,"rows_affected":1}]}`, res; exp != got {
		t.Fatalf("got incorrect response from follower exp: %s, got: %s", exp, got)
	}

	rows, err := followers[0].Query(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[2]]}]}`, rows; exp != got {
		t.Fatalf("got incorrect response from follower exp: %s, got: %s", exp, got)
	}
}

func executeRequestFromString(s string) *command.ExecuteRequest {
	return executeRequestFromStrings([]string{s})
}

// queryRequestFromStrings converts a slice of strings into a command.ExecuteRequest
func executeRequestFromStrings(s []string) *command.ExecuteRequest {
	stmts := make([]*command.Statement, len(s))
	for i := range s {
		stmts[i] = &command.Statement{
			Sql: s[i],
		}
	}
	return &command.ExecuteRequest{
		Request: &command.Request{
			Statements:  stmts,
			Transaction: false,
		},
		Timings: false,
	}
}

func queryRequestFromString(s string) *command.QueryRequest {
	return queryRequestFromStrings([]string{s})
}

// queryRequestFromStrings converts a slice of strings into a command.QueryRequest
func queryRequestFromStrings(s []string) *command.QueryRequest {
	stmts := make([]*command.Statement, len(s))
	for i := range s {
		stmts[i] = &command.Statement{
			Sql: s[i],
		}
	}
	return &command.QueryRequest{
		Request: &command.Request{
			Statements:  stmts,
			Transaction: false,
		},
		Timings: false,
	}
}

func mustNewDialer(header byte, remoteEncrypted, skipVerify bool) *tcp.Dialer {
	return tcp.NewDialer(header, remoteEncrypted, skipVerify)
}
