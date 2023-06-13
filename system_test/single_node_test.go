/*
Package system runs system-level testing of rqlite. This includes testing of single nodes, and multi-node clusters.
*/
package system

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/cluster"
	httpd "github.com/rqlite/rqlite/http"
	"github.com/rqlite/rqlite/store"
	"github.com/rqlite/rqlite/tcp"
)

func Test_SingleNodeBasicEndpoint(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	// Ensure accessing endpoints in basic manner works
	_, err := node.Status()
	if err != nil {
		t.Fatalf(`failed to retrieve status for in-memory: %s`, err)
	}

	dir := mustTempDir()
	mux, ln := mustNewOpenMux("")
	defer ln.Close()
	node = mustNodeEncryptedOnDisk(dir, true, false, mux, "", false)
	if _, err := node.WaitForLeader(); err != nil {
		t.Fatalf("node never became leader")
	}
	_, err = node.Status()
	if err != nil {
		t.Fatalf(`failed to retrieve status for on-disk: %s`, err)
	}

	ready, err := node.Ready()
	if err != nil {
		t.Fatalf(`failed to retrieve readiness: %s`, err)
	}
	if !ready {
		t.Fatalf("node is not ready")
	}

	// Test that registering a ready channel affects readiness as expected.
	ch := make(chan struct{})
	node.Store.RegisterReadyChannel(ch)
	ready, err = node.Ready()
	if err != nil {
		t.Fatalf(`failed to retrieve readiness: %s`, err)
	}
	if ready {
		t.Fatalf("node is ready when registered ready channel not yet closed")
	}
	close(ch)
	testPoll(t, node.Ready, 100*time.Millisecond, 5*time.Second)
}

func Test_SingleNodeNotReadyLive(t *testing.T) {
	node := mustNewNode(false)
	defer node.Deprovision()
	ready, err := node.Ready()
	if err != nil {
		t.Fatalf(`failed to retrieve readiness: %s`, err)
	}
	if ready {
		t.Fatalf("node is ready when it should not be")
	}

	liveness, err := node.Liveness()
	if err != nil {
		t.Fatalf(`failed to retrieve liveness: %s`, err)
	}
	if !liveness {
		t.Fatalf("node is not live when it should not be")
	}

	// Confirm that hitting various endpoints with a non-ready node
	// is OK.
	_, err = node.Nodes(false)
	if err != nil {
		t.Fatalf(`failed to retrieve Nodes: %s`, err)
	}
	_, err = node.Status()
	if err != nil {
		t.Fatalf(`failed to retrieve status: %s`, err)
	}
}

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

func Test_SingleNodeRequest(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	tests := []struct {
		stmt     string
		expected string
	}{
		{
			stmt:     `CREATE TABLE foo (id integer not null primary key, name text)`,
			expected: `{"results":[{}]}`,
		},
		{
			stmt:     `INSERT INTO foo(name) VALUES("fiona")`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
		},
		{
			stmt:     `INSERT INTO bar(name) VALUES("fiona")`,
			expected: `{"results":[{"error":"no such table: bar"}]}`,
		},
		{
			stmt:     `INSERT blah blah`,
			expected: `{"results":[{"error":"near \"blah\": syntax error"}]}`,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}`,
		},
		{
			stmt:     `DROP TABLE bar`,
			expected: `{"results":[{"error":"no such table: bar"}]}`,
		},
		{
			stmt:     `DROP TABLE foo`,
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
		},
	}

	for i, tt := range tests {
		r, err := node.Request(tt.stmt)
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

func Test_SingleNodeConcurrentRequests(t *testing.T) {
	var err error
	node := mustNewLeaderNode()
	node.Store.SetRequestCompression(1024, 1024) // Ensure no compression
	defer node.Deprovision()

	_, err = node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := PostExecuteStmt(node.APIAddr, `INSERT INTO foo(name) VALUES("fiona")`)
			if err != nil {
				t.Logf("failed to insert record: %s %s", err.Error(), resp)
			}
		}()
	}

	wg.Wait()
	r, err := node.Query("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to count records: %s", err.Error())
	}
	if r != `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[200]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
}

func Test_SingleNodeConcurrentRequestsCompressed(t *testing.T) {
	var err error
	node := mustNewLeaderNode()
	node.Store.SetRequestCompression(0, 0) // Ensure compression
	defer node.Deprovision()

	_, err = node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := PostExecuteStmt(node.APIAddr, `INSERT INTO foo(name) VALUES("fiona")`)
			if err != nil {
				t.Logf("failed to insert record: %s %s", err.Error(), resp)
			}
		}()
	}

	wg.Wait()
	r, err := node.Query("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to count records: %s", err.Error())
	}
	if r != `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[200]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
}

func Test_SingleNodeParameterized(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	tests := []struct {
		stmt     []interface{}
		expected string
		execute  bool
	}{
		{
			stmt:     []interface{}{"CREATE TABLE foo (id integer not null primary key, name text, age integer)"},
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     []interface{}{"INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     []interface{}{"SELECT * FROM foo WHERE NAME=?", "fiona"},
			expected: `{"results":[{"columns":["id","name","age"],"types":["integer","text","integer"],"values":[[1,"fiona",20]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = node.ExecuteParameterized(tt.stmt)
		} else {
			r, err = node.QueryParameterized(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

func Test_SingleNodeRequestParameterized(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	tests := []struct {
		stmt     []interface{}
		expected string
	}{
		{
			stmt:     []interface{}{"CREATE TABLE foo (id integer not null primary key, name text, age integer)"},
			expected: `{"results":[{}]}`,
		},
		{
			stmt:     []interface{}{"INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
		},
		{
			stmt:     []interface{}{"SELECT * FROM foo WHERE NAME=?", "fiona"},
			expected: `{"results":[{"columns":["id","name","age"],"types":["integer","text","integer"],"values":[[1,"fiona",20]]}]}`,
		},
	}

	for i, tt := range tests {
		r, err := node.RequestMultiParameterized(tt.stmt)
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

func Test_SingleNodeParameterizedNull(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	tests := []struct {
		stmt     []interface{}
		expected string
		execute  bool
	}{
		{
			stmt:     []interface{}{"CREATE TABLE foo (id integer not null primary key, name text, age integer)"},
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     []interface{}{"INSERT INTO foo(name, age) VALUES(?, ?)", "declan", nil},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     []interface{}{"SELECT * FROM foo WHERE NAME=?", "declan"},
			expected: `{"results":[{"columns":["id","name","age"],"types":["integer","text","integer"],"values":[[1,"declan",null]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = node.ExecuteParameterized(tt.stmt)
		} else {
			r, err = node.QueryParameterized(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

func Test_SingleNodeParameterizedNamed(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	tests := []struct {
		stmt     []interface{}
		expected string
		execute  bool
	}{
		{
			stmt:     []interface{}{"CREATE TABLE foo (id integer not null primary key, name text, age integer)"},
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     []interface{}{"INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     []interface{}{"SELECT * FROM foo WHERE NAME=:name", map[string]interface{}{"name": "fiona"}},
			expected: `{"results":[{"columns":["id","name","age"],"types":["integer","text","integer"],"values":[[1,"fiona",20]]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = node.ExecuteParameterized(tt.stmt)
		} else {
			r, err = node.QueryParameterized(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

// Test_SingleNodeParameterizedNamedConstraints tests that named parameters can be used with constraints
// See https://github.com/rqlite/rqlite/issues/1177
func Test_SingleNodeParameterizedNamedConstraints(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	_, err := node.ExecuteParameterized([]interface{}{"CREATE TABLE [TestTable] ([Id] int primary key, [Col1] int not null, [Col2] varchar(500), [Col3] int not null, [Col4] varchar(500))"})
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	for i := 0; i < 100; i++ {
		r, err := node.ExecuteParameterized([]interface{}{"INSERT into TestTable (Col1, Col2, Col3, Col4) values (:Val1, :Val2, :Val3, :Val4)", map[string]interface{}{"Val1": 1, "Val2": "foo", "Val3": 2, "Val4": nil}})
		if err != nil {
			t.Fatalf("failed to insert record on loop %d: %s", i, err.Error())
		}
		if r != fmt.Sprintf(`{"results":[{"last_insert_id":%d,"rows_affected":1}]}`, i+1) {
			t.Fatalf("test received wrong result on loop %d, got %s", i, r)
		}
	}
}

func Test_SingleNodeRewriteRandom(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	_, err := node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf(`CREATE TABLE failed: %s`, err.Error())
	}

	resp, err := node.Execute(`INSERT INTO foo(id, name) VALUES(RANDOM(), "fiona")`)
	if err != nil {
		t.Fatalf(`queued write failed: %s`, err.Error())
	}

	match := regexp.MustCompile(`{"results":[{"last_insert_id":\-?[0-9]+,"rows_affected":1}]}`)
	if !match.MatchString(resp) {
		t.Fatalf("test received wrong result got %s", resp)
	}
}

func Test_SingleNodeQueued(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	_, err := node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf(`CREATE TABLE failed: %s`, err.Error())
	}

	qWrites := []string{
		`INSERT INTO foo(name) VALUES("fiona")`,
		`INSERT INTO foo(name) VALUES("fiona")`,
		`INSERT INTO foo(name) VALUES("fiona")`,
	}
	resp, err := node.ExecuteQueuedMulti(qWrites, false)
	if err != nil {
		t.Fatalf(`queued write failed: %s`, err.Error())
	}
	if !QueuedResponseRegex.MatchString(resp) {
		t.Fatalf("queued response is not valid: %s", resp)
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	timer := time.NewTimer(5 * time.Second)
LOOP:
	for {
		select {
		case <-ticker.C:
			r, err := node.Query(`SELECT COUNT(*) FROM foo`)
			if err != nil {
				t.Fatalf(`query failed: %s`, err.Error())
			}
			if r == `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[3]]}]}` {
				break LOOP
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for queued writes")

		}
	}

	// Waiting for a queue write to complete means we should get the correct
	// results immediately.
	resp, err = node.ExecuteQueuedMulti(qWrites, true)
	if err != nil {
		t.Fatalf(`queued write failed: %s`, err.Error())
	}
	if !QueuedResponseRegex.MatchString(resp) {
		t.Fatalf("queued response is not valid: %s", resp)
	}
	r, err := node.Query(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf(`query failed: %s`, err.Error())
	}
	if got, exp := r, `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[6]]}]}`; got != exp {
		t.Fatalf("incorrect results, exp: %s, got: %s", exp, got)
	}
}

// Test_SingleNodeQueuedBadStmt tests that a single bad SQL statement has the right outcome.
func Test_SingleNodeQueuedBadStmt(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()
	node.Service.DefaultQueueTx = false

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	_, err := node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf(`CREATE TABLE failed: %s`, err.Error())
	}

	qWrites := []string{
		`INSERT INTO foo(name) VALUES("fiona")`,
		`INSERT INTO nonsense`,
		`INSERT INTO foo(name) VALUES("fiona")`,
	}
	resp, err := node.ExecuteQueuedMulti(qWrites, false)
	if err != nil {
		t.Fatalf(`queued write failed: %s`, err.Error())
	}
	if !QueuedResponseRegex.MatchString(resp) {
		t.Fatalf("queued response is not valid: %s", resp)
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

LOOP1:
	for {
		select {
		case <-ticker.C:
			r, err := node.Query(`SELECT COUNT(*) FROM foo`)
			if err != nil {
				t.Fatalf(`query failed: %s`, err.Error())
			}
			if r == `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[2]]}]}` {
				break LOOP1
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for queued writes")

		}
	}

	// Enable transactions, so that the next request shouldn't change the data
	// due to the single bad statement.
	node.Service.DefaultQueueTx = true
	resp, err = node.ExecuteQueuedMulti(qWrites, false)
	if err != nil {
		t.Fatalf(`queued write failed: %s`, err.Error())
	}
	if !QueuedResponseRegex.MatchString(resp) {
		t.Fatalf("queued response is not valid: %s", resp)
	}

	timer.Reset(5 * time.Second)
LOOP2:
	for {
		select {
		case <-ticker.C:
			r, err := node.Query(`SELECT COUNT(*) FROM foo`)
			if err != nil {
				t.Fatalf(`query failed: %s`, err.Error())
			}
			if r == `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[2]]}]}` {
				break LOOP2
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for queued writes")

		}
	}
}

func Test_SingleNodeQueuedEmptyNil(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	_, err := node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf(`CREATE TABLE failed: %s`, err.Error())
	}

	qWrites := []string{
		`INSERT INTO foo(name) VALUES("fiona")`,
		`INSERT INTO foo(name) VALUES("fiona")`,
		`INSERT INTO foo(name) VALUES("fiona")`,
	}
	resp, err := node.ExecuteQueuedMulti(qWrites, false)
	if err != nil {
		t.Fatalf(`queued write failed: %s`, err.Error())
	}
	if !QueuedResponseRegex.MatchString(resp) {
		t.Fatalf("queued response is not valid: %s", resp)
	}

	// Waiting for a queue write to complete means we should get the correct
	// results immediately. But don't add any statements.
	resp, err = node.ExecuteQueuedMulti([]string{}, true)
	if err != nil {
		t.Fatalf(`queued empty write failed: %s`, err.Error())
	}
	if !QueuedResponseRegex.MatchString(resp) {
		t.Fatalf("queued response is not valid: %s", resp)
	}
	r, err := node.Query(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf(`query failed: %s`, err.Error())
	}
	if got, exp := r, `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[3]]}]}`; got != exp {
		t.Fatalf("incorrect results, exp: %s, got: %s", exp, got)
	}

	resp, err = node.ExecuteQueuedMulti(qWrites, false)
	if err != nil {
		t.Fatalf(`queued write failed: %s`, err.Error())
	}
	if !QueuedResponseRegex.MatchString(resp) {
		t.Fatalf("queued response is not valid: %s", resp)
	}

	// Waiting for a queue write to complete means we should get the correct
	// results immediately. But don't add any statements.
	resp, err = node.ExecuteQueuedMulti(nil, true)
	if err != nil {
		t.Fatalf(`queued nil write failed: %s`, err.Error())
	}
	if !QueuedResponseRegex.MatchString(resp) {
		t.Fatalf("queued response is not valid: %s", resp)
	}
	r, err = node.Query(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf(`query failed: %s`, err.Error())
	}
	if got, exp := r, `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[6]]}]}`; got != exp {
		t.Fatalf("incorrect results, exp: %s, got: %s", exp, got)
	}
}

// Test_SingleNodeSQLInjection demonstrates that using the non-parameterized API is vulnerable to
// SQL injection attacks.
func Test_SingleNodeSQLInjection(t *testing.T) {
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
			stmt:     `CREATE TABLE bar (id integer not null primary key, name text)`,
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"]}]}`,
			execute:  false,
		},
		{
			stmt:     fmt.Sprintf(`INSERT INTO foo(name) VALUES(%s)`, `"alice");DROP TABLE foo;INSERT INTO bar(name) VALUES("bob"`),
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     `SELECT * FROM foo`,
			expected: `{"results":[{"error":"no such table: foo"}]}`,
			execute:  false,
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

// Test_SingleNodeNoSQLInjection demonstrates that using the parameterized API protects
// against SQL injection attacks.
func Test_SingleNodeNoSQLInjection(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	tests := []struct {
		stmt     []interface{}
		expected string
		execute  bool
	}{
		{
			stmt:     []interface{}{"CREATE TABLE foo (id integer not null primary key, name text)"},
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     []interface{}{`SELECT * FROM foo WHERE name="baz"`},
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"]}]}`,
			execute:  false,
		},
		{
			stmt:     []interface{}{`SELECT * FROM foo WHERE name=?`, `"baz";DROP TABLE FOO`},
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"]}]}`,
			execute:  false,
		},
		{
			stmt:     []interface{}{`SELECT * FROM foo`},
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"]}]}`,
			execute:  false,
		},
	}

	for i, tt := range tests {
		var r string
		var err error
		if tt.execute {
			r, err = node.ExecuteParameterized(tt.stmt)
		} else {
			r, err = node.QueryParameterized(tt.stmt)
		}
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

// Test_SingleNodeUpgrades upgrade from a data created by earlier releases.
func Test_SingleNodeUpgrades(t *testing.T) {
	versions := []string{
		"v6.0.0-data",
		"v7.0.0-data",
		"v7.9.2-data",
		"v7.20.3-data-with-snapshots",
	}

	upgradeFrom := func(dir string) {
		// Deprovision of a node deletes the node's dir, so make a copy first.
		srcdir := filepath.Join("testdata", dir)
		destdir := mustTempDir()
		if err := os.Remove(destdir); err != nil {
			t.Fatalf("failed to remove dest dir: %s", err)
		}
		if err := copyDir(srcdir, destdir); err != nil {
			t.Fatalf("failed to copy node test directory: %s", err)
		}

		mux, ln := mustNewOpenMux("")
		defer ln.Close()

		node := mustNodeEncrypted(destdir, true, false, mux, "node1")
		defer node.Deprovision()
		if _, err := node.WaitForLeader(); err != nil {
			t.Fatalf("node never became leader with %s data:", dir)
		}

		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		testSuccess := make(chan struct{})

		for {
			select {
			case <-testSuccess:
				return
			case <-timer.C:
				t.Fatalf(`timeout waiting for correct results with %s data`, dir)
			case <-ticker.C:
				r, err := node.QueryNoneConsistency(`SELECT COUNT(*) FROM foo`)
				if err != nil {
					t.Fatalf("query failed with %s data: %s", dir, err)
				}
				expected := `{"results":[{"columns":["COUNT(*)"],"types":[""],"values":[[20]]}]}`
				if r == expected {
					close(testSuccess)
				}
			}
		}
	}

	for _, version := range versions {
		t.Run(version, func(t *testing.T) {
			upgradeFrom(version)
		})
	}
}

func Test_SingleNodeNodes(t *testing.T) {
	node := mustNewLeaderNode()
	defer node.Deprovision()

	// Access endpoints to ensure the code is covered.
	nodes, err := node.Nodes(false)
	if err != nil {
		t.Fatalf("failed to access nodes endpoint: %s", err.Error())
	}

	if len(nodes) != 1 {
		t.Fatalf("wrong number of nodes in response")
	}
	n, ok := nodes[node.ID]
	if !ok {
		t.Fatalf("node not found by ID in response")
	}
	if n.Addr != node.RaftAddr {
		t.Fatalf("node has wrong Raft address")
	}
	if got, exp := n.APIAddr, fmt.Sprintf("http://%s", node.APIAddr); exp != got {
		t.Fatalf("node has wrong API address, exp: %s got: %s", exp, got)
	}
	if !n.Leader {
		t.Fatalf("node is not leader")
	}
	if !n.Reachable {
		t.Fatalf("node is not reachable")
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

// Test_SingleNodeReopen tests that a node can be re-opened OK.
func Test_SingleNodeReopen(t *testing.T) {
	onDisk := false

	for {
		t.Logf("running test %s, on-disk=%v", t.Name(), onDisk)

		dir := mustTempDir()
		mux, ln := mustNewOpenMux("")
		defer ln.Close()
		node := mustNodeEncrypted(dir, true, false, mux, "")

		if _, err := node.WaitForLeader(); err != nil {
			t.Fatalf("node never became leader")
		}

		if err := node.Close(true); err != nil {
			t.Fatalf("failed to close node")
		}

		if err := node.Store.Open(); err != nil {
			t.Fatalf("failed to re-open store: %s", err)
		}
		if err := node.Store.Bootstrap(store.NewServer(node.Store.ID(), node.Store.Addr(), true)); err != nil {
			t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
		}
		if err := node.Service.Start(); err != nil {
			t.Fatalf("failed to restart service: %s", err)
		}

		if _, err := node.WaitForLeader(); err != nil {
			t.Fatalf("node never became leader")
		}

		node.Deprovision()
		// Switch to other mode for another test.
		onDisk = !onDisk
		if onDisk == false {
			break
		}
	}
}

// Test_SingleNodeReopen tests that a node can be re-opened OK, with
// a non-database command in the log.
func Test_SingleNodeNoopReopen(t *testing.T) {
	onDisk := false

	for {
		t.Logf("running test %s, on-disk=%v", t.Name(), onDisk)

		dir := mustTempDir()
		mux, ln := mustNewOpenMux("")
		defer ln.Close()
		node := mustNodeEncryptedOnDisk(dir, true, false, mux, "", false)

		if _, err := node.WaitForLeader(); err != nil {
			t.Fatalf("node never became leader")
		}

		if err := node.Noop("#1"); err != nil {
			t.Fatalf("failed to write noop command: %s", err)
		}

		if err := node.Close(true); err != nil {
			t.Fatalf("failed to close node")
		}

		if err := node.Store.Open(); err != nil {
			t.Fatalf("failed to re-open store: %s", err)
		}
		if err := node.Store.Bootstrap(store.NewServer(node.Store.ID(), node.Store.Addr(), true)); err != nil {
			t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
		}
		if err := node.Service.Start(); err != nil {
			t.Fatalf("failed to restart service: %s", err)
		}
		// This testing tells service to restart with localhost:0
		// again, so explicitly set the API address again.
		node.APIAddr = node.Service.Addr().String()

		if _, err := node.WaitForLeader(); err != nil {
			t.Fatalf("node never became leader")
		}

		// Ensure node is fully functional after restart.
		tests := []struct {
			stmt     []interface{}
			expected string
			execute  bool
		}{
			{
				stmt:     []interface{}{"CREATE TABLE foo (id integer not null primary key, name text, age integer)"},
				expected: `{"results":[{}]}`,
				execute:  true,
			},
			{
				stmt:     []interface{}{"INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20},
				expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
				execute:  true,
			},
			{
				stmt:     []interface{}{"SELECT * FROM foo WHERE NAME=?", "fiona"},
				expected: `{"results":[{"columns":["id","name","age"],"types":["integer","text","integer"],"values":[[1,"fiona",20]]}]}`,
				execute:  false,
			},
		}

		for i, tt := range tests {
			var r string
			var err error
			if tt.execute {
				r, err = node.ExecuteParameterized(tt.stmt)
			} else {
				r, err = node.QueryParameterized(tt.stmt)
			}
			if err != nil {
				t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
			}
			if r != tt.expected {
				t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
			}
		}

		node.Deprovision()
		// Switch to other mode for another test.
		onDisk = !onDisk
		if onDisk == false {
			break
		}
	}
}

// Test_SingleNodeReopen tests that a node can be re-opened OK, with
// a snapshot present which was triggered by non-database commands.
// This tests that the code can handle a snapshot that doesn't
// contain database data. This shouldn't happen in real systems
func Test_SingleNodeNoopSnapReopen(t *testing.T) {
	onDisk := false

	for {
		t.Logf("running test %s, on-disk=%v", t.Name(), onDisk)

		dir := mustTempDir()
		mux, ln := mustNewOpenMux("")
		defer ln.Close()
		node := mustNodeEncryptedOnDisk(dir, true, false, mux, "", onDisk)

		if _, err := node.WaitForLeader(); err != nil {
			t.Fatalf("node never became leader")
		}

		for i := 0; i < 150; i++ {
			if err := node.Noop(fmt.Sprintf("%d", i)); err != nil {
				t.Fatalf("failed to write noop command: %s", err)
			}
		}

		// Wait for a snapshot to happen.
		time.Sleep(5 * time.Second)

		if err := node.Close(true); err != nil {
			t.Fatalf("failed to close node")
		}

		if err := node.Store.Open(); err != nil {
			t.Fatalf("failed to re-open store: %s", err)
		}
		if err := node.Store.Bootstrap(store.NewServer(node.Store.ID(), node.Store.Addr(), true)); err != nil {
			t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
		}
		if err := node.Service.Start(); err != nil {
			t.Fatalf("failed to restart service: %s", err)
		}
		// This testing tells service to restart with localhost:0
		// again, so explicitly set the API address again.
		node.APIAddr = node.Service.Addr().String()

		if _, err := node.WaitForLeader(); err != nil {
			t.Fatalf("node never became leader")
		}

		// Ensure node is fully functional after restart.
		tests := []struct {
			stmt     []interface{}
			expected string
			execute  bool
		}{
			{
				stmt:     []interface{}{"CREATE TABLE foo (id integer not null primary key, name text, age integer)"},
				expected: `{"results":[{}]}`,
				execute:  true,
			},
			{
				stmt:     []interface{}{"INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20},
				expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
				execute:  true,
			},
			{
				stmt:     []interface{}{"SELECT * FROM foo WHERE NAME=?", "fiona"},
				expected: `{"results":[{"columns":["id","name","age"],"types":["integer","text","integer"],"values":[[1,"fiona",20]]}]}`,
				execute:  false,
			},
		}

		for i, tt := range tests {
			var r string
			var err error
			if tt.execute {
				r, err = node.ExecuteParameterized(tt.stmt)
			} else {
				r, err = node.QueryParameterized(tt.stmt)
			}
			if err != nil {
				t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
			}
			if r != tt.expected {
				t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
			}
		}

		node.Deprovision()
		onDisk = !onDisk
		if onDisk == false {
			break
		}
	}
}

// Test_SingleNodeNoopSnapLogsReopen tests that a node can be re-opened OK,
// with a snapshot present which was triggered by non-database commands.
// This tests that the code can handle a snapshot that doesn't
// contain database data. This shouldn't happen in real systems
func Test_SingleNodeNoopSnapLogsReopen(t *testing.T) {
	onDisk := false
	var raftAddr string

	for {
		t.Logf("running test %s, on-disk=%v", t.Name(), onDisk)

		dir := mustTempDir()
		mux, ln := mustNewOpenMux("")
		defer ln.Close()
		node := mustNodeEncryptedOnDisk(dir, true, false, mux, "", onDisk)
		raftAddr = node.RaftAddr
		t.Logf("node listening for Raft on %s", raftAddr)

		if _, err := node.WaitForLeader(); err != nil {
			t.Fatalf("node never became leader")
		}

		for i := 0; i < 150; i++ {
			if err := node.Noop(fmt.Sprintf("%d", i)); err != nil {
				t.Fatalf("failed to write noop command: %s", err)
			}
		}

		// Wait for a snapshot to happen, and then write some more commands.
		time.Sleep(5 * time.Second)
		for i := 0; i < 5; i++ {
			if err := node.Noop(fmt.Sprintf("%d", i)); err != nil {
				t.Fatalf("failed to write noop command: %s", err)
			}
		}

		if err := node.Close(true); err != nil {
			t.Fatalf("failed to close node")
		}

		if err := node.Store.Open(); err != nil {
			t.Fatalf("failed to re-open store: %s", err)
		}
		if err := node.Service.Start(); err != nil {
			t.Fatalf("failed to restart service: %s", err)
		}
		// This testing tells service to restart with localhost:0
		// again, so explicitly set the API address again.
		node.APIAddr = node.Service.Addr().String()

		if _, err := node.WaitForLeader(); err != nil {
			t.Fatalf("node never became leader")
		}

		// Ensure node is fully functional after restart.
		tests := []struct {
			stmt     []interface{}
			expected string
			execute  bool
		}{
			{
				stmt:     []interface{}{"CREATE TABLE foo (id integer not null primary key, name text, age integer)"},
				expected: `{"results":[{}]}`,
				execute:  true,
			},
			{
				stmt:     []interface{}{"INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20},
				expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
				execute:  true,
			},
			{
				stmt:     []interface{}{"SELECT * FROM foo WHERE NAME=?", "fiona"},
				expected: `{"results":[{"columns":["id","name","age"],"types":["integer","text","integer"],"values":[[1,"fiona",20]]}]}`,
				execute:  false,
			},
		}

		for i, tt := range tests {
			var r string
			var err error
			if tt.execute {
				r, err = node.ExecuteParameterized(tt.stmt)
			} else {
				r, err = node.QueryParameterized(tt.stmt)
			}
			if err != nil {
				t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
			}
			if r != tt.expected {
				t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
			}
		}

		node.Deprovision()
		// Switch to other mode for another test.
		onDisk = !onDisk
		if onDisk == false {
			break
		}
	}
}

func Test_SingleNodeAutoRestore(t *testing.T) {
	dir := mustTempDir()
	node := &Node{
		Dir:       dir,
		PeersPath: filepath.Join(dir, "raft/peers.json"),
	}
	defer node.Deprovision()

	mux, _ := mustNewOpenMux("")
	go mux.Serve()

	raftTn := mux.Listen(cluster.MuxRaftHeader)
	node.Store = store.New(raftTn, &store.Config{
		DBConf: store.NewDBConfig(true),
		Dir:    node.Dir,
		ID:     raftTn.Addr().String(),
	})

	restoreFile := mustTempFile()
	copyFile(filepath.Join("testdata", "auto-restore.sqlite"), restoreFile)
	if err := node.Store.SetRestorePath(restoreFile); err != nil {
		t.Fatalf("failed to set restore path: %s", err.Error())
	}

	if err := node.Store.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err.Error())
	}
	if err := node.Store.Bootstrap(store.NewServer(node.Store.ID(), node.Store.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap store: %s", err.Error())
	}
	node.RaftAddr = node.Store.Addr()
	node.ID = node.Store.ID()

	clstr := cluster.New(mux.Listen(cluster.MuxClusterHeader), node.Store, node.Store, mustNewMockCredentialStore())
	if err := clstr.Open(); err != nil {
		t.Fatalf("failed to open Cluster service: %s", err.Error())
	}
	node.Cluster = clstr

	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)
	clstrClient := cluster.NewClient(clstrDialer, 30*time.Second)
	node.Service = httpd.New("localhost:0", node.Store, clstrClient, nil)
	node.Service.Expvar = true

	if err := node.Service.Start(); err != nil {
		t.Fatalf("failed to start HTTP server: %s", err.Error())
	}
	node.APIAddr = node.Service.Addr().String()

	// Finally, set API address in Cluster service
	clstr.SetAPIAddr(node.APIAddr)

	if _, err := node.WaitForLeader(); err != nil {
		t.Fatalf("node never became leader")
	}
	testPoll(t, node.Ready, 100*time.Millisecond, 5*time.Second)

	r, err := node.Query("SELECT * FROM foo WHERE id=2")
	if err != nil {
		t.Fatalf("failed to execute query: %s", err.Error())
	}
	if r != `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"fiona"]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
}
