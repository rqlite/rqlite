/*
Package system runs system-level testing of rqlite. This includes testing of single nodes, and multi-node clusters.
*/
package system

import (
	"fmt"
	"sync"
	"testing"
	"time"
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
				t.Fatalf("failed to insert record: %s %s", err.Error(), resp)
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
				t.Fatalf("failed to insert record: %s %s", err.Error(), resp)
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

func Test_SingleParameterizedNode(t *testing.T) {
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
			stmt:     `SELECT * FROM foo WHERE name="baz"`,
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"]}]}`,
			execute:  false,
		},
		{
			stmt:     fmt.Sprintf(`SELECT * FROM foo WHERE name=%s`, `"baz";DROP TABLE FOO`),
			expected: `{"results":[{}]}`,
			execute:  false,
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

// func Test_SingleNodeRestart(t *testing.T) {
// 	// Deprovision of a node deletes the node's dir, so make a copy first.
// 	srcdir := filepath.Join("testdata", "v5.6.0-data")
// 	destdir := mustTempDir()
// 	if err := os.Remove(destdir); err != nil {
// 		t.Fatalf("failed to remove dest dir: %s", err)
// 	}
// 	if err := copyDir(srcdir, destdir); err != nil {
// 		t.Fatalf("failed to copy node test directory: %s", err)
// 	}

// 	mux := mustNewOpenMux("")

// 	node := mustNodeEncrypted(destdir, true, false, mux, "node1")
// 	defer node.Deprovision()
// 	if _, err := node.WaitForLeader(); err != nil {
// 		t.Fatal("node never became leader")
// 	}

// 	// Let's wait a few seconds to be sure logs are applied.
// 	n := 0
// 	for {
// 		time.Sleep(1 * time.Second)

// 		r, err := node.QueryNoneConsistency(`SELECT * FROM foo`)
// 		if err != nil {
// 			t.Fatalf("query failed: %s", err)
// 		}

// 		expected := `{"results":[{"columns":["id","name","age"],"types":["integer","text","integer"],"values":[[1,"fiona",20]]}]}`
// 		if r != expected {
// 			if n == 10 {
// 				t.Fatalf(`query received wrong result, got: %s exp: %s`, r, expected)
// 			}
// 		} else {
// 			break // Test successful!
// 		}

// 		n++
// 	}
// }

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
		mux := mustNewOpenMux("")
		node := mustNodeEncrypted(dir, true, false, mux, "")

		if _, err := node.WaitForLeader(); err != nil {
			t.Fatalf("node never became leader")
		}

		if err := node.Close(true); err != nil {
			t.Fatalf("failed to close node")
		}

		if err := node.Store.Open(true); err != nil {
			t.Fatalf("failed to re-open store: %s", err)
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
		mux := mustNewOpenMux("")
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

		if err := node.Store.Open(true); err != nil {
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

// Test_SingleNodeReopen tests that a node can be re-opened OK, with
// a snapshot present which was triggered by non-database commands.
// This tests that the code can handle a snapshot that doesn't
// contain database data. This shouldn't happen in real systems
func Test_SingleNodeNoopSnapReopen(t *testing.T) {
	onDisk := false

	for {
		t.Logf("running test %s, on-disk=%v", t.Name(), onDisk)

		dir := mustTempDir()
		mux := mustNewOpenMux("")
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

		if err := node.Store.Open(true); err != nil {
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
		mux := mustNewOpenMux("")
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

		if err := node.Store.Open(true); err != nil {
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
