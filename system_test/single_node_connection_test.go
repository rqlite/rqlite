/*
Package system runs system-level testing of rqlite. This includes testing of single nodes, and multi-node clusters.
*/
package system

import (
	"fmt"
	"testing"
)

func Test_SingleNodeConnection(t *testing.T) {
	t.Parallel()

	node := mustNewLeaderNode()
	defer node.Deprovision()
	c, err := node.Connect()
	if err != nil {
		t.Fatalf("failed to create connection: %s", err.Error())
	}
	ri := 3 // It just is now.
	raftIdx := func() int {
		ri++
		return ri
	}

	tests := []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: fmt.Sprintf(`{"results":[{}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: fmt.Sprintf(`{"results":[{"last_insert_id":1,"rows_affected":1}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
		{
			stmt:     `INSERT INTO bar(name) VALUES("fiona")`,
			expected: fmt.Sprintf(`{"results":[{"error":"no such table: bar"}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
		{
			stmt:     `INSERT blah blah`,
			expected: fmt.Sprintf(`{"results":[{"error":"near \"blah\": syntax error"}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`,
			execute:  false,
		},
		{
			stmt:     `DROP TABLE bar`,
			expected: fmt.Sprintf(`{"results":[{"error":"no such table: bar"}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
		{
			stmt:     `DROP TABLE foo`,
			expected: fmt.Sprintf(`{"results":[{"last_insert_id":1,"rows_affected":1}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = c.Execute(tt.stmt)
		} else {
			r, err = c.Query(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

func Test_SingleNodeMultiConnection(t *testing.T) {
	t.Parallel()

	node := mustNewLeaderNode()
	defer node.Deprovision()
	c, err := node.Connect()
	if err != nil {
		t.Fatalf("failed to create connection: %s", err.Error())
	}
	ri := 3 // It just is now.
	raftIdx := func() int {
		ri++
		return ri
	}

	tests := []struct {
		stmt     string
		expected string
		execute  bool
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: fmt.Sprintf(`{"results":[{}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
		{
			stmt:     `CREATE TABLE bar (id integer not null primary key, sequence integer)`,
			expected: fmt.Sprintf(`{"results":[{}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: fmt.Sprintf(`{"results":[{"last_insert_id":1,"rows_affected":1}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("declan")`,
			expected: fmt.Sprintf(`{"results":[{"last_insert_id":2,"rows_affected":1}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
		{
			stmt:     `INSERT INTO bar(sequence) VALUES(5)`,
			expected: fmt.Sprintf(`{"results":[{"last_insert_id":1,"rows_affected":1}],%s}`, rr(node.ID, raftIdx())),
			execute:  true,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = c.Execute(tt.stmt)
		} else {
			r, err = c.Query(tt.stmt)
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
