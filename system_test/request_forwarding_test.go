package system

import (
	"fmt"
	"testing"

	"github.com/rqlite/rqlite/cluster"
	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/command/encoding"
	"github.com/rqlite/rqlite/tcp"
)

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
	res, err = client.Execute(leaderAddr, executeRequestFromString("CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"))
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
	res, err = client.Execute(leaderAddr, executeRequestFromString(`INSERT INTO bar(name) VALUES("fiona")`))
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

	rows, err = client.Query(leaderAddr, queryRequestFromString(`SELECT * FROM foo`))
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
	}
	rows, err = client.Query(leaderAddr, queryRequestFromString(`SELECT * FROM bar`))
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
	rows, err = client.Query(leaderAddr, queryRequestFromString(`SELECT * FROM qux`))
	if err != nil {
		t.Fatalf("failed to query via remote: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: qux"}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results, expt %s, got %s", exp, got)
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

func asJSON(v interface{}) string {
	b, err := encoding.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}
