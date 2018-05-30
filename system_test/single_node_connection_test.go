package system

import (
	"fmt"
	"strings"
	"testing"
	"time"
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

	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %s", err.Error())
	}
	r, _ := c.Query(`SELECT * FROM foo`)
	if !strings.Contains(r, "connection not found") {
		t.Fatalf("test received wrong result\ngot: %s", r)
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

func Test_SingleNodeConnectionIsolation(t *testing.T) {
	t.Parallel()

	node := mustNewLeaderNode()
	defer node.Deprovision()
	first, err := node.Connect()
	if err != nil {
		t.Fatalf("failed to create first connection: %s", err.Error())
	}
	second, err := node.Connect()
	if err != nil {
		t.Fatalf("failed to create second connection: %s", err.Error())
	}

	first.Execute(`BEGIN`)
	first.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	first.Execute(`INSERT INTO foo(name) VALUES("fiona")`)

	r, _ := second.Query(`SELECT * FROM foo`)
	if got, exp := r, `{"results":[{"error":"no such table: foo"}]}`; got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}

	first.Execute(`COMMIT`)

	r, _ = second.Query(`SELECT * FROM foo`)
	if got, exp := r, `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`; got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}
	second.Execute(`BEGIN`)
	second.Execute(`INSERT INTO foo(name) VALUES("fiona")`)
	r, _ = second.Query(`SELECT COUNT(id) FROM foo`)
	if got, exp := r, `{"results":[{"columns":["COUNT(id)"],"types":[""],"values":[[2]]}]}`; got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}

	r, _ = first.Query(`SELECT COUNT(id) FROM foo`)
	if got, exp := r, `{"results":[{"columns":["COUNT(id)"],"types":[""],"values":[[1]]}]}`; got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}

	second.Execute(`ROLLBACK`)
	r, _ = second.Query(`SELECT COUNT(id) FROM foo`)
	if got, exp := r, `{"results":[{"columns":["COUNT(id)"],"types":[""],"values":[[1]]}]}`; got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}

	r, _ = first.Query(`SELECT COUNT(id) FROM foo`)
	if got, exp := r, `{"results":[{"columns":["COUNT(id)"],"types":[""],"values":[[1]]}]}`; got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}

	first.Execute(`BEGIN`)
	first.Execute(`INSERT INTO foo(name) VALUES("fiona")`)

	second.Execute(`BEGIN`)
	r, _ = second.Execute(`INSERT INTO foo(name) VALUES("fiona")`)
	if got, exp := r, fmt.Sprintf(`{"results":[{"error":"database is locked"}],%s}`, rr(node.ID, 15)); got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}
	second.Execute(`ROLLBACK`)

	first.Execute(`ROLLBACK`)
	first.Execute(`BEGIN IMMEDIATE`)

	second.Execute(`BEGIN`)
	if got, exp := r, fmt.Sprintf(`{"results":[{"error":"database is locked"}],%s}`, rr(node.ID, 15)); got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}
}

func Test_SingleNodeConnectionIsolationClose(t *testing.T) {
	t.Parallel()

	node := mustNewLeaderNode()
	defer node.Deprovision()
	first, err := node.Connect()
	if err != nil {
		t.Fatalf("failed to create first connection: %s", err.Error())
	}
	second, err := node.Connect()
	if err != nil {
		t.Fatalf("failed to create second connection: %s", err.Error())
	}

	first.Execute(`BEGIN`)
	first.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	first.Execute(`INSERT INTO foo(name) VALUES("fiona")`)
	first.Close()

	r, _ := second.Query(`SELECT COUNT(id) FROM foo`)
	if got, exp := r, `{"results":[{"error":"no such table: foo"}]}`; got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}
}

func Test_SingleNodeConnectionIsolationTimeouts(t *testing.T) {
	t.Parallel()

	txTimeout := 2 * time.Second
	node := mustNewLeaderNode()
	defer node.Deprovision()
	first, err := node.ConnectWithTimeouts(0, txTimeout)
	if err != nil {
		t.Fatalf("failed to create first connection: %s", err.Error())
	}
	second, err := node.Connect()
	if err != nil {
		t.Fatalf("failed to create second connection: %s", err.Error())
	}

	node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	r, _ := first.Query(`SELECT * FROM foo`)
	if got, exp := r, `{"results":[{"columns":["id","name"],"types":["integer","text"]}]}`; got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}
	r, _ = second.Query(`SELECT * FROM foo`)
	if got, exp := r, `{"results":[{"columns":["id","name"],"types":["integer","text"]}]}`; got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}

	first.Execute(`BEGIN IMMEDIATE`)
	first.Execute(`INSERT INTO foo(name) VALUES("fiona")`)

	r, _ = second.Query(`SELECT * FROM foo`)
	if got, exp := r, `{"results":[{"columns":["id","name"],"types":["integer","text"]}]}`; got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}

	time.Sleep(3 * txTimeout)

	r, _ = second.Execute(`BEGIN`)
	if got, exp := r, fmt.Sprintf(`{"results":[{}],%s}`, rr(node.ID, 9)); got != exp {
		t.Fatalf("test received wrong result\ngot: %s\nexp: %s\n", got, exp)
	}
	second.Execute(`ROLLBACK`)
}

func Test_SingleNodeConnectionCloseTimeouts(t *testing.T) {
	t.Parallel()

	idleTimeout := 2 * time.Second
	node := mustNewLeaderNode()
	defer node.Deprovision()
	first, err := node.ConnectWithTimeouts(idleTimeout, 0)
	if err != nil {
		t.Fatalf("failed to create first connection: %s", err.Error())
	}

	first.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	first.Execute(`INSERT INTO foo(name) VALUES("fiona")`)

	time.Sleep(3 * idleTimeout)

	r, _ := first.Query(`SELECT * FROM foo`)
	if !strings.Contains(r, "connection not found") {
		t.Fatalf("test received wrong result\ngot: %s", r)
	}
	r, _ = first.Execute(`INSERT INTO foo(name) VALUES("fiona")`)
	if !strings.Contains(r, "connection not found") {
		t.Fatalf("test received wrong result\ngot: %s", r)
	}
}
