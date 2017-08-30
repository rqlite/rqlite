/*
Package system runs system-level testing of rqlite. This includes testing of single nodes, and multi-node clusters.
*/
package system

import (
	"testing"
)

func Test_SingleNode(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	tests := []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO bar(name) VALUES("fiona")`,
			expected: `{"results":[{"error":"no such table: bar"}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT blah blah`,
			expected: `{"results":[{"error":"near \"blah\": syntax error"}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`,
			execute:  false,
		},
		{
			stmt:     `DROP TABLE bar`,
			expected: `{"results":[{"error":"no such table: bar"}]}`,
			execute:  true,
		},
		{
			stmt:     `DROP TABLE foo`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = node.Execute(tt.stmt)
		} else {
			r, err = node.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

func Test_SingleNodeMulti(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	tests := []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     `CREATE TABLE bar (id integer not null primary key, sequence integer)`,
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("declan")`,
			expected: `{"results":[{"last_insert_id":2,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `INSERT INTO bar(sequence) VALUES(5)`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = node.Execute(tt.stmt)
		} else {
			r, err = node.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}

	r, err := node.QueryMulti([]string{"SELECT * FROM foo", "SELECT * FROM bar"})
	if err != nil {
		t.Fatalf("failed to run multiple queries: %s", err.Error())
	}
	if r != `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"declan"]]},{"columns":["id","sequence"],"types":["integer","integer"],"values":[[1,5]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
}

func Test_SingleNodeCoverage(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	// Access endpoints to ensure the code is covered.
	var err error
	var str string

	str, err = node.Status()
	if err != nil {
		t.Fatalf("failed to access status endpoint: %s", err.Error())
	}
	if !isJSON(str) {
		t.Fatalf("output from status endpoint is not valid JSON: %s", str)
	}

	str, err = node.Expvar()
	if err != nil {
		t.Fatalf("failed to access expvar endpoint: %s", err.Error())
	}
	if !isJSON(str) {
		t.Fatalf("output from expvar endpoint is not valid JSON: %s", str)
	}
}
