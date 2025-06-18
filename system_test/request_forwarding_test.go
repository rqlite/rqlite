package system

import (
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/cluster"
	clstrPB "github.com/rqlite/rqlite/v8/cluster/proto"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/rtls"
	"github.com/rqlite/rqlite/v8/tcp"
)

const (
	shortWait = 5 * time.Second
	noRetries = 0
)

var (
	NO_CREDS = (*clstrPB.Credentials)(nil)
)

// Test_StoreClientSideBySide operates on the same store directly, and via
// RPC, and ensures results are the same for basically the same operation.
func Test_StoreClientSideBySide(t *testing.T) {

	node := mustNewLeaderNode("leader1")
	defer node.Deprovision()
	leaderAddr, err := node.Store.LeaderAddr()
	if err != nil {
		t.Fatalf("failed to get leader Raft address: %s", err.Error())
	}

	client := cluster.NewClient(mustNewDialer(cluster.MuxClusterHeader, false, false), 30*time.Second)

	res, _, err := node.Store.Execute(executeRequestFromString("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"))
	if err != nil {
		t.Fatalf("failed to execute on local: %s", err.Error())
	}
	if exp, got := "[{}]", asJSON(res); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	res, idx, err := client.Execute(executeRequestFromString("CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"), leaderAddr, NO_CREDS, shortWait, noRetries)
	if err != nil {
		t.Fatalf("failed to execute via remote: %s", err.Error())
	}
	if exp, got := "[{}]", asJSON(res); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	if idx == 0 {
		t.Fatalf("expected non-zero index, got %d", idx)
	}

	// ==============================================================================
	res, _, err = node.Store.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`))
	if err != nil {
		t.Fatalf("failed to execute on local: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	res, idx, err = client.Execute(executeRequestFromString(`INSERT INTO bar(name) VALUES("fiona")`), leaderAddr, NO_CREDS, shortWait, noRetries)
	if err != nil {
		t.Fatalf("failed to execute via remote: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	if idx == 0 {
		t.Fatalf("expected non-zero index, got %d", idx)
	}

	// ==============================================================================
	rows, err := node.Store.Query(queryRequestFromString(`SELECT * FROM foo`))
	if err != nil {
		t.Fatalf("failed to query on local: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	results, _, err := node.Store.Request(executeQueryRequestFromString(`SELECT * FROM foo`))
	if err != nil {
		t.Fatalf("failed to request on local: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(results); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	rows, err = client.Query(queryRequestFromString(`SELECT * FROM foo`), leaderAddr, NO_CREDS, shortWait)
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	results, idx, err = client.Request(executeQueryRequestFromString(`SELECT * FROM foo`), leaderAddr, NO_CREDS, shortWait, 0)
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(results); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	if idx != 0 {
		t.Fatalf("expected zero index due to only read, got %d", idx)
	}

	// ==============================================================================
	rows, err = node.Store.Query(queryRequestFromString(`SELECT * FROM bar`))
	if err != nil {
		t.Fatalf("failed to query on local: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	results, _, err = node.Store.Request(executeQueryRequestFromString(`SELECT * FROM bar`))
	if err != nil {
		t.Fatalf("failed to request on local: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(results); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	rows, err = client.Query(queryRequestFromString(`SELECT * FROM bar`), leaderAddr, NO_CREDS, shortWait)
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	results, idx, err = client.Request(executeQueryRequestFromString(`SELECT * FROM bar`), leaderAddr, NO_CREDS, shortWait, noRetries)
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(results); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	if idx != 0 {
		t.Fatalf("expected zero Raft index due to read, got %d", idx)
	}

	// ==============================================================================
	rows, err = node.Store.Query(queryRequestFromString(`SELECT * FROM qux`))
	if err != nil {
		t.Fatalf("failed to query on local: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: qux"}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	results, _, err = node.Store.Request(executeQueryRequestFromString(`SELECT * FROM qux`))
	if err != nil {
		t.Fatalf("failed to request on local: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: qux"}]`, asJSON(results); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	// Statements causing errors are considered read-write by SQLite, so
	// don't bother with index check. It's not super-meaningful.

	rows, err = client.Query(queryRequestFromString(`SELECT * FROM qux`), leaderAddr, NO_CREDS, shortWait)
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: qux"}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
	results, _, err = client.Request(executeQueryRequestFromString(`SELECT * FROM qux`), leaderAddr, NO_CREDS, shortWait, noRetries)
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: qux"}]`, asJSON(results); exp != got {
		t.Fatalf("unexpected results, exp %s, got %s", exp, got)
	}
}

// Test_MultiNodeCluster tests formation of a 3-node cluster and query
// against all nodes to test requests are forwarded to leader transparently.
func Test_MultiNodeClusterRequestForwardOK(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
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

	node3 := mustNewNode("node3", false)
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

	res, err = followers[1].Request(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `{"results":[{"last_insert_id":2,"rows_affected":1}]}`, res; exp != got {
		t.Fatalf("got incorrect response from follower exp: %s, got: %s", exp, got)
	}

	res, err = leader.Execute(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `{"results":[{"last_insert_id":3,"rows_affected":1}]}`, res; exp != got {
		t.Fatalf("got incorrect response from follower exp: %s, got: %s", exp, got)
	}

	rows, err := followers[0].Query(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[3]]}]}`, rows; exp != got {
		t.Fatalf("got incorrect response from follower exp: %s, got: %s", exp, got)
	}
}

// Test_MultiNodeClusterQueuedRequestForwardOK tests that queued writes are forwarded
// correctly.
func Test_MultiNodeClusterQueuedRequestForwardOK(t *testing.T) {
	node1 := mustNewLeaderNode("leader1")
	defer node1.Deprovision()

	node2 := mustNewNode("node2", false)
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

	// Create table and confirm its existence.
	res, err := leader.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `{"results":[{}]}`, res; exp != got {
		t.Fatalf("got incorrect response from follower exp: %s, got: %s", exp, got)
	}
	rows, err := leader.Query(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query for count: %s", err.Error())
	}
	if exp, got := `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[0]]}]}`, rows; exp != got {
		t.Fatalf("got incorrect response from follower exp: %s, got: %s", exp, got)
	}

	// Write a request to a follower's queue, checking it's eventually sent to the leader.
	followers, err := c.Followers()
	if err != nil {
		t.Fatalf("failed to get followers: %s", err.Error())
	}
	if len(followers) != 1 {
		t.Fatalf("got incorrect number of followers: %d", len(followers))
	}
	_, err = followers[0].ExecuteQueued(`INSERT INTO foo(name) VALUES("fiona")`, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	timer := time.NewTimer(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			r, err := leader.Query(`SELECT COUNT(*) FROM foo`)
			if err != nil {
				t.Fatalf("failed to query for count: %s", err.Error())
			}
			if r == `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}` {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for queued writes")
		}
	}
}

func executeRequestFromString(s string) *proto.ExecuteRequest {
	return executeRequestFromStrings([]string{s})
}

// queryRequestFromStrings converts a slice of strings into a command.ExecuteRequest
func executeRequestFromStrings(s []string) *proto.ExecuteRequest {
	stmts := make([]*proto.Statement, len(s))
	for i := range s {
		stmts[i] = &proto.Statement{
			Sql: s[i],
		}
	}
	return &proto.ExecuteRequest{
		Request: &proto.Request{
			Statements:  stmts,
			Transaction: false,
		},
		Timings: false,
	}
}

func queryRequestFromString(s string) *proto.QueryRequest {
	return queryRequestFromStrings([]string{s})
}

// queryRequestFromStrings converts a slice of strings into a command.QueryRequest
func queryRequestFromStrings(s []string) *proto.QueryRequest {
	stmts := make([]*proto.Statement, len(s))
	for i := range s {
		stmts[i] = &proto.Statement{
			Sql: s[i],
		}
	}
	return &proto.QueryRequest{
		Request: &proto.Request{
			Statements:  stmts,
			Transaction: false,
		},
		Timings: false,
	}
}

func executeQueryRequestFromString(s string) *proto.ExecuteQueryRequest {
	return executeQueryRequestFromStrings([]string{s})
}

// executeQueryRequestFromStrings converts a slice of strings into a command.ExecuteQueryRequest
func executeQueryRequestFromStrings(s []string) *proto.ExecuteQueryRequest {
	stmts := make([]*proto.Statement, len(s))
	for i := range s {
		stmts[i] = &proto.Statement{
			Sql: s[i],
		}
	}
	return &proto.ExecuteQueryRequest{
		Request: &proto.Request{
			Statements:  stmts,
			Transaction: false,
		},
		Timings: false,
	}
}

func mustNewDialer(header byte, remoteEncrypted, skipVerify bool) *tcp.Dialer {
	var tlsConfig *tls.Config
	var err error
	if remoteEncrypted {
		tlsConfig, err = rtls.CreateClientConfig("", "", rtls.NoCACert, rtls.NoServerName, skipVerify)
		if err != nil {
			panic(fmt.Sprintf("failed to create client TLS config: %s", err))
		}
	}
	return tcp.NewDialer(header, tlsConfig)
}
