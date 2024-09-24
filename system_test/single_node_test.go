/*
Package system runs system-level testing of rqlite. This includes testing of single nodes, and multi-node clusters.
*/
package system

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/cluster"
	httpd "github.com/rqlite/rqlite/v8/http"
	"github.com/rqlite/rqlite/v8/random"
	"github.com/rqlite/rqlite/v8/store"
	"github.com/rqlite/rqlite/v8/tcp"
)

func Test_SingleNodeBasicEndpoint(t *testing.T) {
	node := mustNewLeaderNode("node1")
	defer node.Deprovision()

	// Ensure accessing endpoints in basic manner works
	_, err := node.Status()
	if err != nil {
		t.Fatalf(`failed to retrieve status: %s`, err)
	}

	dir := mustTempDir("node1")
	mux, ln := mustNewOpenMux("")
	defer ln.Close()
	raftDialer := tcp.NewDialer(cluster.MuxRaftHeader, nil)
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)
	node = mustNodeEncrypted("node1", dir, true, false, mux, raftDialer, clstrDialer)
	if _, err := node.WaitForLeader(); err != nil {
		t.Fatalf("node never became leader")
	}
	_, err = node.Status()
	if err != nil {
		t.Fatalf(`failed to retrieve status for: %s`, err)
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
	node := mustNewNode("node1", false)
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
	node := mustNewLeaderNode("node1")
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

func Test_SingleNode_DisallowedPragmas(t *testing.T) {
	node := mustNewLeaderNode("node1")
	defer node.Deprovision()

	tests := []struct {
		stmt     string
		expected string
	}{
		{
			stmt:     `PRAGMA JOURNAL_MODE=DELETE`,
			expected: `{"results":[],"error":"disallowed pragma"}`,
		},
		{
			stmt:     `PRAGMA wal_autocheckpoint = 1000`,
			expected: `{"results":[],"error":"disallowed pragma"}`,
		},
		{
			stmt:     `PRAGMA wal_autocheckpoint = 1000`,
			expected: `{"results":[],"error":"disallowed pragma"}`,
		},
		{
			stmt:     `PRAGMA synchronous = NORMAL`,
			expected: `{"results":[],"error":"disallowed pragma"}`,
		},
		{
			stmt:     `  PRAGMA synchronous = NORMAL`,
			expected: `{"results":[],"error":"disallowed pragma"}`,
		},
		{
			stmt:     `PRAGMA      synchronous = NORMAL`,
			expected: `{"results":[],"error":"disallowed pragma"}`,
		},
	}

	for i, tt := range tests {
		var r string
		var err error

		r, err = node.Execute(tt.stmt)
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}

		r, err = node.Query(tt.stmt)
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}

		r, err = node.Request(tt.stmt)
		if err != nil {
			t.Fatalf(`test %d failed "%s": %s`, i, tt.stmt, err.Error())
		}
		if r != tt.expected {
			t.Fatalf(`test %d received wrong result "%s" got: %s exp: %s`, i, tt.stmt, r, tt.expected)
		}
	}
}

func Test_SingleNodeRequest(t *testing.T) {
	node := mustNewLeaderNode("leader1")
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
	node := mustNewLeaderNode("leader1")
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
	node := mustNewLeaderNode("leader1")
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
	if r != `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[200]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
}

func Test_SingleNodeConcurrentRequestsCompressed(t *testing.T) {
	var err error
	node := mustNewLeaderNode("leader1")
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
	if r != `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[200]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
}

func Test_SingleNodeBlob(t *testing.T) {
	node := mustNewLeaderNode("leader1")
	defer node.Deprovision()

	_, err := node.Execute(`CREATE TABLE foo (id integer not null primary key, data blob)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	_, err = node.Execute(`INSERT INTO foo(data) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	_, err = node.Execute(`INSERT INTO foo(data) VALUES(x'deadbeef')`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := node.Query("SELECT data FROM foo")
	if err != nil {
		t.Fatalf("failed to query records: %s", err.Error())
	}
	if r != `{"results":[{"columns":["data"],"types":["blob"],"values":[["fiona"],["3q2+7w=="]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
	r, err = node.QueryWithBlobArray("SELECT data FROM foo")
	if err != nil {
		t.Fatalf("failed to query records: %s", err.Error())
	}
	if r != `{"results":[{"columns":["data"],"types":["blob"],"values":[["fiona"],[[222,173,190,239]]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
}

func Test_SingleNodeParameterized(t *testing.T) {
	node := mustNewLeaderNode("leader1")
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

func Test_SingleNodeBlob_Parameterized(t *testing.T) {
	node := mustNewLeaderNode("leader1")
	defer node.Deprovision()

	tests := []struct {
		stmt     []interface{}
		expected string
		execute  bool
	}{
		{
			stmt:     []interface{}{"CREATE TABLE foo (id integer not null primary key, data blob) STRICT"},
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			stmt:     []interface{}{"INSERT INTO foo(id, data) VALUES(?, ?)", 1, `x'deadbeef'`},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     []interface{}{"INSERT INTO foo(id, data) VALUES(?, ?)", 2, []int{222, 173, 190, 239}},
			expected: `{"results":[{"last_insert_id":2,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			stmt:     []interface{}{"SELECT * FROM foo"},
			expected: `{"results":[{"columns":["id","data"],"types":["integer","blob"],"values":[[1,"3q2+7w=="],[2,"3q2+7w=="]]}]}`,
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
	node := mustNewLeaderNode("leader1")
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
	node := mustNewLeaderNode("leader1")
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
	node := mustNewLeaderNode("leader1")
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
	node := mustNewLeaderNode("leader1")
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

func Test_SingleNodeQueryTimeout(t *testing.T) {
	node := mustNewLeaderNode("leader1")
	defer node.Deprovision()

	sql := `CREATE TABLE IF NOT EXISTS test_table (
		key1      VARCHAR(64) PRIMARY KEY,
		key_id    VARCHAR(64) NOT NULL,
		key2      VARCHAR(64) NOT NULL,
		key3      VARCHAR(64) NOT NULL,
		key4      VARCHAR(64) NOT NULL,
		key5      VARCHAR(64) NOT NULL,
		key6      VARCHAR(64) NOT NULL,
		data      BLOB        NOT NULL
	)`
	if _, err := node.Execute(sql); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Bulk insert rows (for speed and to avoid snapshotting)
	sqls := make([]string, 5000)
	for i := 0; i < cap(sqls); i++ {
		args := []any{
			random.String(),
			fmt.Sprint(i),
			random.String(),
			random.String(),
			random.String(),
			random.String(),
			random.String(),
			random.String(),
		}
		sql := fmt.Sprintf(`INSERT INTO test_table
			(key1, key_id, key2, key3, key4, key5, key6, data)
			VALUES
			(%q, %q, %q, %q, %q, %q, %q, %q);`, args...)
		sqls[i] = sql
	}
	if _, err := node.ExecuteMulti(sqls); err != nil {
		t.Fatalf("failed to insert data: %s", err.Error())
	}
	r, err := node.Query(`SELECT COUNT(*) FROM test_table`)
	if err != nil {
		t.Fatalf("failed to count records: %s", err.Error())
	}
	exp := fmt.Sprintf(`{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[%d]]}]}`, len(sqls))
	if r != exp {
		t.Fatalf("test received wrong result\nexp: %s\ngot: %s\n", exp, r)
	}

	q := `SELECT key1, key_id, key2, key3, key4, key5, key6, data
	FROM test_table
	ORDER BY key2 ASC`
	r, err = node.QueryWithTimeout(q, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to query with timeout: %s", err.Error())
	}
	if !strings.Contains(r, `"error":"query timeout"`) {
		// This test is brittle, but it's the best we can do, as we can't be sure
		// how much of the query will actually be executed. We just know it should
		// time out at some point.
		t.Fatalf("query ran to completion, but should have timed out")
	}
}

func Test_SingleNodeRewriteRandom(t *testing.T) {
	node := mustNewLeaderNode("leader1")
	defer node.Deprovision()

	_, err := node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf(`CREATE TABLE failed: %s`, err.Error())
	}

	resp, err := node.Execute(`INSERT INTO foo(id, name) VALUES(RANDOM(), "fiona")`)
	if err != nil {
		t.Fatalf(`write failed: %s`, err.Error())
	}

	match := regexp.MustCompile(`{"results":[{"last_insert_id":\-?[0-9]+,"rows_affected":1}]}`)
	if !match.MatchString(resp) {
		t.Fatalf("test received wrong result got %s", resp)
	}
}

func Test_SingleNode_RETURNING(t *testing.T) {
	node := mustNewLeaderNode("leader1")
	defer node.Deprovision()

	_, err := node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf(`CREATE TABLE failed: %s`, err.Error())
	}

	res, err := node.Execute(`INSERT INTO foo(id, name) VALUES(1, "fiona")`)
	if err != nil {
		t.Fatalf(`queued write failed: %s`, err.Error())
	}
	if got, exp := res, `{"results":[{"last_insert_id":1,"rows_affected":1}]}`; got != exp {
		t.Fatalf("wrong execute results, exp %s, got %s", exp, got)
	}
	res, err = node.Execute(`INSERT INTO foo(id, name) VALUES(2, "declan") RETURNING *`)
	if err != nil {
		t.Fatalf(`write failed: %s`, err.Error())
	}
	if got, exp := res, `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"declan"]]}]}`; got != exp {
		t.Fatalf("wrong execute results for RETURNING, exp %s, got %s", exp, got)
	}
	res, err = node.Execute(`INSERT INTO foo(id, name) VALUES(3, "aoife")`)
	if err != nil {
		t.Fatalf(`write failed: %s`, err.Error())
	}
	if got, exp := res, `{"results":[{"last_insert_id":3,"rows_affected":1}]}`; got != exp {
		t.Fatalf("wrong execute results, exp %s, got %s", exp, got)
	}

	// Try a request with multiple statements.
	res, err = node.ExecuteMulti([]string{
		`INSERT INTO foo(id, name) VALUES(4, "alice") RETURNING *`,
		`INSERT INTO foo(id, name) VALUES(5, "bob")`})
	if err != nil {
		t.Fatalf("failed to INSERT record: %s", err.Error())
	}
	if got, exp := res, `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[4,"alice"]]},{"last_insert_id":5,"rows_affected":1}]}`; got != exp {
		t.Fatalf("wrong execute-multi results for RETURNING, exp %s, got %s", exp, got)
	}
}

func Test_SingleNodeQueued(t *testing.T) {
	node := mustNewLeaderNode("leader1")
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
			if r == `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[3]]}]}` {
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
	if got, exp := r, `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[6]]}]}`; got != exp {
		t.Fatalf("incorrect results, exp: %s, got: %s", exp, got)
	}
}

// Test_SingleNodeQueuedBadStmt tests that a single bad SQL statement has the right outcome.
func Test_SingleNodeQueuedBadStmt(t *testing.T) {
	node := mustNewLeaderNode("leader1")
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
			if r == `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2]]}]}` {
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
			if r == `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2]]}]}` {
				break LOOP2
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for queued writes")

		}
	}
}

func Test_SingleNodeQueuedEmptyNil(t *testing.T) {
	node := mustNewLeaderNode("leader1")
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
	if got, exp := r, `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[3]]}]}`; got != exp {
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
	if got, exp := r, `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[6]]}]}`; got != exp {
		t.Fatalf("incorrect results, exp: %s, got: %s", exp, got)
	}
}

// Test_SingleNodeSQLInjection demonstrates that using the non-parameterized API is vulnerable to
// SQL injection attacks.
func Test_SingleNodeSQLInjection(t *testing.T) {
	node := mustNewLeaderNode("leader1")
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
	node := mustNewLeaderNode("leader1")
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

// Test_SingleNodeUpgrades_NoSnapshots upgrade from a data created by earlier releases, but which
// do not have snapshots.
func Test_SingleNodeUpgrades_NoSnapshots(t *testing.T) {
	versions := []string{
		"v7.0.0-data",
		"v7.9.2-data",
	}

	upgradeFrom := func(dir string) {
		// Deprovision of a node deletes the node's dir, so make a copy first.
		srcdir := filepath.Join("testdata", dir)
		destdir := mustTempDir("")
		if err := os.Remove(destdir); err != nil {
			t.Fatalf("failed to remove dest dir: %s", err)
		}
		if err := copyDir(srcdir, destdir); err != nil {
			t.Fatalf("failed to copy node test directory: %s", err)
		}

		mux, ln := mustNewOpenMux("")
		defer ln.Close()
		raftDialer := tcp.NewDialer(cluster.MuxRaftHeader, nil)
		clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)
		node := mustNodeEncrypted("node1", destdir, false, false, mux, raftDialer, clstrDialer)
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
				expected := `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[20]]}]}`
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

// Test_SingleNodeUpgrades_Snapshots upgrade from a data created by earlier releases, but which
// do have snapshots.
func Test_SingleNodeUpgrades_Snapshots(t *testing.T) {
	versions := []string{
		"v7.20.3-data-with-snapshots",
	}

	upgradeFrom := func(dir string) {
		// Deprovision of a node deletes the node's dir, so make a copy first.
		srcdir := filepath.Join("testdata", dir)
		destdir := mustTempDir("")
		if err := os.Remove(destdir); err != nil {
			t.Fatalf("failed to remove dest dir: %s", err)
		}
		if err := copyDir(srcdir, destdir); err != nil {
			t.Fatalf("failed to copy node test directory: %s", err)
		}

		mux, ln := mustNewOpenMux("")
		defer ln.Close()
		raftDialer := tcp.NewDialer(cluster.MuxRaftHeader, nil)
		clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)
		node := mustNodeEncrypted("node1", destdir, false, false, mux, raftDialer, clstrDialer)
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
				expected := `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[20]]}]}`
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

func Test_SingleNodeBackup(t *testing.T) {
	node := mustNewLeaderNode("leader1")
	defer node.Deprovision()

	// create a table and insert a row
	_, err := node.Execute(`CREATE TABLE foo (id integer not null primary key, name text)`)
	if err != nil {
		t.Fatalf(`CREATE TABLE failed: %s`, err.Error())
	}
	_, err = node.Execute(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf(`INSERT failed: %s`, err.Error())
	}

	backup := mustTempFile()
	defer os.Remove(backup)
	if err := node.Backup(backup, false); err != nil {
		t.Fatalf(`backup failed: %s`, err.Error())
	}

	// Create new node, and restore from backup.
	newNode := mustNewLeaderNode("leader2")
	defer newNode.Deprovision()
	_, err = newNode.Boot(backup)
	if err != nil {
		t.Fatalf(`boot failed: %s`, err.Error())
	}
	r, err := newNode.Query(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf(`query failed: %s`, err.Error())
	}
	if r != `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}

	// Get a compressed backup, test it again.
	if err := node.Backup(backup, true); err != nil {
		t.Fatalf(`backup failed: %s`, err.Error())
	}

	// decompress backup and check it.
	decompressedBackup := mustTempFile()
	defer os.Remove(decompressedBackup)

	f, err := os.Open(backup)
	if err != nil {
		t.Fatalf(`failed to open backup: %s`, err.Error())
	}
	defer f.Close()
	gzr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf(`failed to create gzip reader: %s`, err.Error())
	}
	defer gzr.Close()
	w, err := os.Create(decompressedBackup)
	if err != nil {
		t.Fatalf(`failed to create decompressed backup: %s`, err.Error())
	}
	defer w.Close()
	if _, err := io.Copy(w, gzr); err != nil {
		t.Fatalf(`failed to decompress backup: %s`, err.Error())
	}

	// Create new node, and restore from backup.
	newNode2 := mustNewLeaderNode("leader3")
	defer newNode2.Deprovision()
	_, err = newNode2.Boot(decompressedBackup)
	if err != nil {
		t.Fatalf(`boot failed: %s`, err.Error())
	}
	r, err = newNode2.Query(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf(`query failed: %s`, err.Error())
	}
	if r != `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
}

func Test_SingleNodeNodes(t *testing.T) {
	node := mustNewLeaderNode("leader1")
	defer node.Deprovision()

	// Access endpoints to ensure the code is covered.
	nodes, err := node.Nodes(false)
	if err != nil {
		t.Fatalf("failed to access nodes endpoint: %s", err.Error())
	}

	if len(nodes) != 1 {
		t.Fatalf("wrong number of nodes in response")
	}
	n := nodes[0]
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
	node := mustNewLeaderNode("leader1")
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
	dir := mustTempDir("node1")
	mux, ln := mustNewOpenMux("")
	defer ln.Close()
	raftDialer := tcp.NewDialer(cluster.MuxRaftHeader, nil)
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)
	node := mustNodeEncrypted("node1", dir, true, false, mux, raftDialer, clstrDialer)

	if _, err := node.WaitForLeader(); err != nil {
		t.Fatalf("node never became leader")
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
	if _, err := node.WaitForLeader(); err != nil {
		t.Fatalf("node never became leader")
	}

	node.Deprovision()
}

// Test_SingleNodeNoopReopen tests that a node can be re-opened OK, with
// a non-database command in the log.
func Test_SingleNodeNoopReopen(t *testing.T) {
	dir := mustTempDir("node1")
	mux, ln := mustNewOpenMux("")
	defer ln.Close()
	raftDialer := tcp.NewDialer(cluster.MuxRaftHeader, nil)
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)
	node := mustNodeEncrypted("node1", dir, true, false, mux, raftDialer, clstrDialer)

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
}

// Test_SingleNodeNoopSnapReopen tests that a node can be re-opened OK, with
// a snapshot present which was triggered by non-database commands.
// This tests that the code can handle a snapshot that doesn't
// contain database data. This shouldn't happen in real systems
func Test_SingleNodeNoopSnapReopen(t *testing.T) {
	dir := mustTempDir("node1")
	mux, ln := mustNewOpenMux("")
	defer ln.Close()
	raftDialer := tcp.NewDialer(cluster.MuxRaftHeader, nil)
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)
	node := mustNodeEncrypted("node1", dir, true, false, mux, raftDialer, clstrDialer)

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
}

// Test_SingleNodeNoopSnapLogsReopen tests that a node can be re-opened OK,
// with a snapshot present which was triggered by non-database commands.
// This tests that the code can handle a snapshot that doesn't
// contain database data. This shouldn't happen in real systems
func Test_SingleNodeNoopSnapLogsReopen(t *testing.T) {
	dir := mustTempDir("node1")
	mux, ln := mustNewOpenMux("")
	defer ln.Close()
	raftDialer := tcp.NewDialer(cluster.MuxRaftHeader, nil)
	clstrDialer := tcp.NewDialer(cluster.MuxClusterHeader, nil)
	node := mustNodeEncrypted("node1", dir, true, false, mux, raftDialer, clstrDialer)

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
}

func Test_SingleNodeAutoRestore(t *testing.T) {
	dir := mustTempDir("node1")
	node := &Node{
		Dir:       dir,
		PeersPath: filepath.Join(dir, "raft/peers.json"),
	}
	defer node.Deprovision()

	mux, _ := mustNewOpenMux("")
	go mux.Serve()

	raftLn := mux.Listen(cluster.MuxRaftHeader)
	raftTn := tcp.NewLayer(raftLn, tcp.NewDialer(cluster.MuxRaftHeader, nil))
	node.Store = store.New(raftTn, &store.Config{
		DBConf: store.NewDBConfig(),
		Dir:    node.Dir,
		ID:     "node1",
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

func Test_SingleNodeBoot_OK(t *testing.T) {
	node := mustNewLeaderNode("leader1")
	defer node.Deprovision()

	_, err := node.Boot(filepath.Join("testdata", "auto-restore.sqlite"))
	if err != nil {
		t.Fatalf("failed to load data: %s", err.Error())
	}

	r, err := node.Query("SELECT * FROM foo WHERE id=2")
	if err != nil {
		t.Fatalf("failed to execute query: %s", err.Error())
	}
	if r != `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"fiona"]]}]}` {
		t.Fatalf("test received wrong result got %s", r)
	}
}

func Test_SingleNodeBoot_FailNotLeader(t *testing.T) {
	node := mustNewNode("node1", false)
	defer node.Deprovision()
	_, err := node.Boot(filepath.Join("testdata", "auto-restore.sqlite"))
	if err == nil {
		t.Fatalf("expected error loading data")
	}
}
