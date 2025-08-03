package db

import (
	"errors"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	command "github.com/rqlite/rqlite/v8/command/proto"
)

func testBusyTimeout(t *testing.T, db *DB) {
	wantRw := rand.N(10000)
	wantRo := rand.N(10000)

	err := db.SetBusyTimeout(wantRw, wantRo)
	if err != nil {
		t.Fatalf("failed to set busy_timeout: %s", err.Error())
	}

	gotRw, gotRo, err := db.BusyTimeout()
	if err != nil {
		t.Fatalf("failed to get busy_timeout: %s", err.Error())
	}

	if gotRw != wantRw || gotRo != wantRo {
		t.Fatalf("want busy_timeout rw=%d, ro=%d, got rw=%d, ro=%d", wantRw, wantRo, gotRw, gotRo)
	}
}

func testCompileOptions(t *testing.T, db *DB) {
	_, err := db.CompileOptions()
	if err != nil {
		t.Fatalf("failed to retrieve compilation options: %s", err.Error())
	}
}

func testSetSynchronousMode(t *testing.T, db *DB) {
	modes := []SynchronousMode{
		SynchronousOff,
		SynchronousNormal,
		SynchronousFull,
		SynchronousExtra,
	}
	for _, m := range modes {
		if db.SetSynchronousMode(m) != nil {
			t.Fatalf("failed to set synchronous mode to %s", m)
		}
		mm, err := db.GetSynchronousMode()
		if err != nil {
			t.Fatalf("failed to get synchronous mode: %s", err.Error())
		}
		if mm != m {
			t.Fatalf("synchronous mode not set to %s", m)
		}
	}
}

func testTableNotExist(t *testing.T, db *DB) {
	q, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: foo"}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testTableCreation(t *testing.T, db *DB) {
	r, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	q, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testTableCreationFTS(t *testing.T, db *DB) {
	r, err := db.ExecuteStringStmt("CREATE VIRTUAL TABLE foo3 USING fts3(id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
	r, err = db.ExecuteStringStmt("CREATE VIRTUAL TABLE foo4 USING fts4(id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
	r, err = db.ExecuteStringStmt("CREATE VIRTUAL TABLE email USING fts5(sender, title, body)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	// Creating an FTS5 table actually affects rows, so just make sure it's not an error.
	if strings.Contains(asJSON(r), "error") {
		t.Fatalf("unexpected error for query, expected %s, got %s", `[{}]`, asJSON(r))
	}
}

func testSQLiteMasterTable(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	r, err := db.QueryStringStmt("SELECT * FROM sqlite_master")
	if err != nil {
		t.Fatalf("failed to query master table: %s", err.Error())
	}
	if exp, got := `[{"columns":["type","name","tbl_name","rootpage","sql"],"types":["text","text","text","int","text"],"values":[["table","foo","foo",2,"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testSQLiteTimeTypes(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo(d DATE, ts TIMESTAMP, dt DATETIME)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(d, ts, dt) VALUES("2004-02-14", "2004-02-14 07:15:00", "2021-10-22 19:39:32.016087")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt("SELECT d, ts, dt FROM foo")
	if err != nil {
		t.Fatalf("failed to query master table: %s", err.Error())
	}
	if exp, got := `[{"columns":["d","ts","dt"],"types":["date","timestamp","datetime"],"values":[["2004-02-14T00:00:00Z","2004-02-14T07:15:00Z","2021-10-22T19:39:32.016087Z"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testSQLiteRandomBlob(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE large_data (id INTEGER PRIMARY KEY, large_text TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	_, err = db.ExecuteStringStmt(`
	WITH RECURSIVE generate_large_data(id, large_text) AS (
		SELECT 1, randomblob(1)
		UNION ALL
		SELECT id + 1, randomblob(1)
		FROM generate_large_data
		WHERE id < 2
	)
	INSERT INTO large_data(id, large_text)
	SELECT id, large_text FROM generate_large_data
	`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	r, err := db.QueryStringStmt("SELECT * FROM large_data LIMIT 1")
	if err != nil {
		t.Fatalf("failed to query master table: %s", err.Error())
	}
	if !strings.Contains(asJSON(r), "large_text") {
		// Just check that it doesn't panic.
		t.Fatalf("unexpected results for query, expected large_text, got %s", asJSON(r))
	}
}

func testNotNULLField(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	r, err := db.QueryStringStmt(`PRAGMA table_info("foo")`)
	if err != nil {
		t.Fatalf("failed to get PRAGMA table_info: %s", err.Error())
	}
	if exp, got := `[{"columns":["cid","name","type","notnull","dflt_value","pk"],"types":["integer","text","text","integer","","integer"],"values":[[0,"id","INTEGER",1,null,1],[1,"name","TEXT",0,null,0]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testBLOB(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (name TEXT, data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name, data) VALUES("fiona", "hello")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name, data) VALUES("declan", X'74657374')`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["name","data"],"types":["text","blob"],"values":[["fiona","hello"],["declan","dGVzdA=="]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testHexQuery(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo(blob_column BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(blob_column) VALUES (x'53514C697465')`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	r, err := db.QueryStringStmt("SELECT x'53514C697465' from foo")
	if err != nil {
		t.Fatalf("failed to query master table: %s", err.Error())
	}
	if exp, got := `[{"columns":["x'53514C697465'"],"types":["text"],"values":[["SQLite"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testSTRICT(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (name TEXT, data BLOB) STRICT")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	res, err := db.ExecuteStringStmt(`INSERT INTO foo(name, data) VALUES("fiona", "hello")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"error":"cannot store TEXT value in BLOB column foo.data"}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	rows, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["name","data"],"types":["text","blob"]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testEmptyStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("")
	if err != nil {
		t.Fatalf("failed to execute empty statement: %s", err.Error())
	}
	_, err = db.ExecuteStringStmt(";")
	if err != nil {
		t.Fatalf("failed to execute empty statement with semicolon: %s", err.Error())
	}
}

func testGeopoly(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE VIRTUAL TABLE polygons USING geopoly(a, b, c)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	_, err = db.ExecuteStringStmt(`INSERT INTO polygons(_shape) VALUES('[[0,0],[2,0],[1,2],[0,0]]')`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	_, err = db.ExecuteStringStmt(`INSERT INTO polygons(_shape) VALUES('[[1,1],[3,1],[3,3],[1,3],[1,1]]')`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	rows, err := db.QueryStringStmt(`SELECT _shape FROM polygons WHERE geopoly_overlap(_shape, '[[0,0],[4,0],[4,4],[0,4],[0,0]]')`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["_shape"],"types":["text"],"values":[["\u0001\u0000\u0000\u0003\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000@\u0000\u0000\u0000\u0000\u0000\u0000\ufffd?\u0000\u0000\u0000@"],["\u0001\u0000\u0000\u0004\u0000\u0000\ufffd?\u0000\u0000\ufffd?\u0000\u0000@@\u0000\u0000\ufffd?\u0000\u0000@@\u0000\u0000@@\u0000\u0000\ufffd?\u0000\u0000@@"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testReadOnlyStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// build a table-driven test suite for StmtReadOnlyWithConn
	tests := []struct {
		query string
		ro    bool
	}{
		{
			query: "SELECT * FROM foo",
			ro:    true,
		},
		{
			query: "INSERT INTO foo(name) VALUES('fiona')",
			ro:    false,
		},
		{
			query: "INSERT INTO foo(name) VALUES('fiona') RETURNING *",
			ro:    false,
		},
		{
			query: "UPDATE foo SET name='fiona' WHERE id=1",
			ro:    false,
		},
		{
			query: "DELETE FROM foo WHERE id=1",
			ro:    false,
		},
		{
			query: "CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)",
			ro:    false,
		},
		{
			query: "DROP TABLE foo",
			ro:    false,
		},
	}

	for _, tt := range tests {
		ro, err := db.QueryStringStmt(tt.query)
		if err != nil {
			t.Fatalf("failed to check if statement is read-only: %s", err.Error())
		}
		if !tt.ro {
			if got, exp := asJSON(ro), `[{"error":"attempt to change database via query operation"}]`; got != exp {
				t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
			}
		}
	}
}

func testSimpleStatementsNumeric(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age NUMERIC)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name, age) VALUES("fiona", 20)`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name","age"],"types":["integer","text","numeric"],"values":[[1,"fiona",20]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleStatementsCollate(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo(x INTEGER PRIMARY KEY, a, b COLLATE BINARY, c COLLATE RTRIM, d COLLATE NOCASE)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo VALUES(1,'abc','abc', 'abc  ','abc')`,
			},
			{
				Sql: `INSERT INTO foo VALUES(2,'abc','abc', 'abc',  'ABC')`,
			},
			{
				Sql: `INSERT INTO foo VALUES(3,'abc','abc', 'abc ', 'Abc')`,
			},
			{
				Sql: `INSERT INTO foo VALUES(4,'abc','abc ','ABC',  'abc')`,
			},
		},
	}
	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	tests := []struct {
		query string
		exp   string
	}{
		{
			query: `SELECT x FROM foo WHERE a = b ORDER BY x`,
			exp:   `[{"columns":["x"],"types":["integer"],"values":[[1],[2],[3]]}]`,
		},
		{
			query: `SELECT x FROM foo WHERE a = b COLLATE RTRIM ORDER BY x`,
			exp:   `[{"columns":["x"],"types":["integer"],"values":[[1],[2],[3],[4]]}]`,
		},
		{
			query: `SELECT count(*) FROM foo GROUP BY d ORDER BY 1`,
			exp:   `[{"columns":["count(*)"],"types":["integer"],"values":[[4]]}]`,
		},
		{
			query: `SELECT count(*) FROM foo GROUP BY (d || '') ORDER BY 1`,
			exp:   `[{"columns":["count(*)"],"types":["integer"],"values":[[1],[1],[2]]}]`,
		},
		{
			query: `SELECT x FROM foo ORDER BY c COLLATE NOCASE, x`,
			exp:   `[{"columns":["x"],"types":["integer"],"values":[[2],[4],[3],[1]]}]`,
		},
	}

	for _, tt := range tests {
		r, err := db.QueryStringStmt(tt.query)
		if err != nil {
			t.Fatalf("failed to query table: %s", err.Error())
		}
		if exp, got := tt.exp, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
}

func testSimpleSingleStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("aoife")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.QueryStringStmt(`SELECT * FROM foo WHERE name="aoife"`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.QueryStringStmt(`SELECT * FROM foo WHERE name="dana"`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.QueryStringStmt(`SELECT * FROM foo ORDER BY name`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"aoife"],[1,"fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.QueryStringStmt(`SELECT *,name FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name","name"],"types":["integer","text","text"],"values":[[1,"fiona","fiona"],[2,"aoife","aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

// testSimpleExpressionStatements tests that types are set for expressions.
func testSimpleExpressionStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER, height REAL)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(id, name, age, height) VALUES(1, "fiona", 20, 6.7)`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(id, name, age, height) VALUES(2, "aoife", 40, 10.4)`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	tests := []struct {
		query string
		exp   string
	}{
		{
			query: `SELECT sum(name) FROM foo`,
			exp:   `[{"columns":["sum(name)"],"types":["real"],"values":[[0]]}]`,
		},
		{
			query: `SELECT sum(age) FROM foo`,
			exp:   `[{"columns":["sum(age)"],"types":["integer"],"values":[[60]]}]`,
		},
		{
			query: `SELECT sum(height) FROM foo`,
			exp:   `[{"columns":["sum(height)"],"types":["real"],"values":[[17.1]]}]`,
		},
		{
			query: `SELECT count(*) FROM foo`,
			exp:   `[{"columns":["count(*)"],"types":["integer"],"values":[[2]]}]`,
		},
		{
			query: `SELECT avg(height) FROM foo`,
			exp:   `[{"columns":["avg(height)"],"types":["real"],"values":[[8.55]]}]`,
		},
		{
			query: `SELECT avg(height),count(*),sum(age) FROM foo`,
			exp:   `[{"columns":["avg(height)","count(*)","sum(age)"],"types":["real","integer","integer"],"values":[[8.55,2,60]]}]`,
		},
	}

	for _, tt := range tests {
		r, err := db.QueryStringStmt(tt.query)
		if err != nil {
			t.Fatalf("failed to query table: %s", err.Error())
		}
		if exp, got := tt.exp, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
}

func testUpsertStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE vocabulary(word TEXT PRIMARY KEY, count INT DEFAULT 1)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt("INSERT INTO vocabulary(word) VALUES('jovial') ON CONFLICT(word) DO UPDATE SET count=count+1")
	if err != nil {
		t.Fatalf("failed to UPSERT first record: %s", err.Error())
	}
	r, err := db.QueryStringStmt("SELECT * FROM vocabulary")
	if err != nil {
		t.Fatalf("failed to query: %s", err.Error())
	}
	if exp, got := `[{"columns":["word","count"],"types":["text","int"],"values":[["jovial",1]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	_, err = db.ExecuteStringStmt("INSERT INTO vocabulary(word) VALUES('jovial') ON CONFLICT(word) DO UPDATE SET count=count+1")
	if err != nil {
		t.Fatalf("failed to UPSERT first record: %s", err.Error())
	}
	r, err = db.QueryStringStmt("SELECT * FROM vocabulary")
	if err != nil {
		t.Fatalf("failed to query: %s", err.Error())
	}
	if exp, got := `[{"columns":["word","count"],"types":["text","int"],"values":[["jovial",2]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testSimpleSingleJSONStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (c0 VARCHAR(36), c1 JSON, c2 NCHAR, c3 NVARCHAR, c4 CLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(c0, c1, c2, c3, c4) VALUES("fiona", '{"mittens": "foobar"}', "bob", "dana", "declan")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query: %s", err.Error())
	}
	if exp, got := `[{"columns":["c0","c1","c2","c3","c4"],"types":["varchar(36)","json","nchar","nvarchar","clob"],"values":[["fiona","{\"mittens\": \"foobar\"}","bob","dana","declan"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testSimpleSingleJSONBStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (tag JSONB)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(tag) VALUES('{"mittens": "qux"}')`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt(`SELECT tag->>'mittens' FROM foo WHERE tag->>'mittens' = 'qux'`)
	if err != nil {
		t.Fatalf("failed to query: %s", err.Error())
	}
	if exp, got := `[{"columns":["tag->>'mittens'"],"types":["text"],"values":[["qux"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testSimpleJoinStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE names (id INTEGER NOT NULL PRIMARY KEY, name TEXT, ssn TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO "names" VALUES(1,'bob','123-45-678')`,
			},
			{
				Sql: `INSERT INTO "names" VALUES(2,'tom','111-22-333')`,
			},
			{
				Sql: `INSERT INTO "names" VALUES(3,'matt','222-22-333')`,
			},
		},
	}
	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt("CREATE TABLE staff (id INTEGER NOT NULL PRIMARY KEY, employer TEXT, ssn TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO "staff" VALUES(1,'acme','222-22-333')`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt(`SELECT names.id,name,names.ssn,employer FROM names INNER JOIN staff ON staff.ssn = names.ssn`)
	if err != nil {
		t.Fatalf("failed to query table using JOIN: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name","ssn","employer"],"types":["integer","text","text","text"],"values":[[3,"matt","222-22-333","acme"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleSingleConcatStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt(`SELECT id || "_bar", name FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id || \"_bar\"","name"],"types":["text","text"],"values":[["1_bar","fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleMultiStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(name) VALUES("fiona")`,
			},
			{
				Sql: `INSERT INTO foo(name) VALUES("dana")`,
			},
		},
	}
	re, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1}]`, asJSON(re); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	req = &command.Request{
		Statements: []*command.Statement{
			{
				Sql: `SELECT * FROM foo`,
			},
			{
				Sql: `SELECT * FROM foo`,
			},
		},
	}
	ro, err := db.Query(req, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"dana"]]},{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"dana"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleSingleMultiLineStatements(t *testing.T, db *DB) {
	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: `
CREATE TABLE foo (
id INTEGER NOT NULL PRIMARY KEY,
name TEXT
)`,
			},
		},
	}
	_, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req = &command.Request{
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(name) VALUES("fiona")`,
			},
			{
				Sql: `INSERT INTO foo(name) VALUES("dana")`,
			},
		},
	}
	re, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1}]`, asJSON(re); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleFailingStatements_Execute(t *testing.T, db *DB) {
	r, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into nonexistent table: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: foo"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	r, err = db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to attempt creation of duplicate table: %s", err.Error())
	}
	if exp, got := `[{"error":"table foo already exists"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(`INSERT INTO foo(id, name) VALUES(11, "fiona")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":11,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	r, err = db.ExecuteStringStmt(`INSERT INTO foo(id, name) VALUES(11, "fiona")`)
	if err != nil {
		t.Fatalf("failed to attempt duplicate record insertion: %s", err.Error())
	}
	if exp, got := `[{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(`utter nonsense`)
	if err != nil {
		if exp, got := `[{"error":"near \"utter\": syntax error"}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
}

func testSimpleFailingStatements_Query(t *testing.T, db *DB) {
	ro, err := db.QueryStringStmt(`SELECT * FROM bar`)
	if err != nil {
		t.Fatalf("failed to attempt query of nonexistent table: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: bar"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	ro, err = db.QueryStringStmt(`SELECTxx * FROM foo`)
	if err != nil {
		t.Fatalf("failed to attempt nonsense query: %s", err.Error())
	}
	if exp, got := `[{"error":"near \"SELECTxx\": syntax error"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err = db.QueryStringStmt(`utter nonsense`)
	if err != nil {
		if exp, got := `[{"error":"near \"utter\": syntax error"}]`, asJSON(ro); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}

	r, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err = db.QueryStringStmt(`SELECT * FROM foo RETURNING *`)
	if err != nil {
		t.Fatalf("failed to attempt query of table: %s", err.Error())
	}
	if exp, got := `[{"error":"near \"RETURNING\": syntax error"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimplePragmaTableInfo(t *testing.T, db *DB) {
	r, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	res, err := db.QueryStringStmt(`PRAGMA table_info("foo")`)
	if err != nil {
		t.Fatalf("failed to query a common table expression: %s", err.Error())
	}
	if exp, got := `[{"columns":["cid","name","type","notnull","dflt_value","pk"],"types":["integer","text","text","integer","","integer"],"values":[[0,"id","INTEGER",1,null,1],[1,"name","TEXT",0,null,0]]}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testWriteOnQueryDatabaseShouldFail(t *testing.T, db *DB) {
	r, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(`INSERT INTO foo(id, name) VALUES(1, "fiona")`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	ro, err := db.QueryStringStmt(`INSERT INTO foo(id, name) VALUES(2, "fiona")`)
	if err != nil {
		t.Fatalf("error attempting read-only write test: %s", err)
	}
	if exp, got := `[{"error":"attempt to change database via query operation"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	ro, err = db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleParameterizedStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: "INSERT INTO foo(name) VALUES(?)",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "fiona",
						},
					},
				},
			},
		},
	}
	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	req.Statements[0].Parameters[0] = &command.Parameter{
		Value: &command.Parameter_S{
			S: "aoife",
		},
	}
	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	req.Statements[0].Sql = "SELECT * FROM foo WHERE name=?"
	req.Statements[0].Parameters[0] = &command.Parameter{
		Value: &command.Parameter_S{
			S: "aoife",
		},
	}
	r, err = db.Query(req, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	req.Statements[0].Parameters[0] = &command.Parameter{
		Value: &command.Parameter_S{
			S: "fiona",
		},
	}
	r, err = db.Query(req, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	req = &command.Request{
		Statements: []*command.Statement{
			{
				Sql: "SELECT * FROM foo WHERE NAME=?",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "fiona",
						},
					},
				},
			},
			{
				Sql: "SELECT * FROM foo WHERE NAME=?",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "aoife",
						},
					},
				},
			},
		},
	}
	r, err = db.Query(req, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]},{"columns":["id","name"],"types":["integer","text"],"values":[[2,"aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleParameterizedStatements_IN(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: "SELECT name FROM sqlite_master WHERE name IN(?)",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "foo",
						},
					},
				},
			},
		},
	}
	r, err := db.Query(req, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["name"],"types":["text"],"values":[["foo"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	req = &command.Request{
		Statements: []*command.Statement{
			{
				Sql: "SELECT name FROM sqlite_master WHERE name IN(?, ?)",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "foo",
						},
					},
					{
						Value: &command.Parameter_S{
							S: "bar",
						},
					},
				},
			},
		},
	}
	r, err = db.Query(req, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["name"],"types":["text"],"values":[["foo"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	req = &command.Request{
		Statements: []*command.Statement{
			{
				Sql: "SELECT name FROM sqlite_master WHERE name IN(?)",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "qux",
						},
					},
				},
			},
		},
	}
	r, err = db.Query(req, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["name"],"types":["text"]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleTwoParameterizedStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, first TEXT, last TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: "INSERT INTO foo(first, last) VALUES(?, ?)",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "bob",
						},
					},
					{
						Value: &command.Parameter_S{
							S: "bobbers",
						},
					},
				},
			},
		},
	}

	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","first","last"],"types":["integer","text","text"],"values":[[1,"bob","bobbers"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleNilParameterizedStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, first TEXT, last TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: "INSERT INTO foo(first, last) VALUES(?, ?)",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "bob",
						},
					},
					{
						Value: nil,
					},
				},
			},
		},
	}

	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","first","last"],"types":["integer","text","text"],"values":[[1,"bob",null]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleNamedParameterizedStatements(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, first TEXT, last TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	if _, err = db.ExecuteStringStmt(`INSERT INTO foo(first, last) VALUES("albert", "einstein")`); err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if _, err = db.ExecuteStringStmt(`INSERT INTO foo(first, last) VALUES("isaac", "newton")`); err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: "INSERT INTO foo(first, last) VALUES(:first, :last)",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "isaac",
						},
						Name: "first",
					},
					{
						Value: &command.Parameter_S{
							S: "asimov",
						},
						Name: "last",
					},
				},
			},
		},
	}
	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert parameterized statement: %s", err.Error())
	}

	rows, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","first","last"],"types":["integer","text","text"],"values":[[1,"albert","einstein"],[2,"isaac","newton"],[3,"isaac","asimov"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	req = &command.Request{
		Statements: []*command.Statement{
			{
				Sql: "SELECT * FROM foo WHERE first=:first AND last=:last",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "newton",
						},
						Name: "last",
					},
					{
						Value: &command.Parameter_S{
							S: "isaac",
						},
						Name: "first",
					},
				},
			},
		},
	}
	rn, err := db.Query(req, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","first","last"],"types":["integer","text","text"],"values":[[2,"isaac","newton"]]}]`, asJSON(rn); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	req = &command.Request{
		Statements: []*command.Statement{
			{
				Sql: "SELECT * FROM foo WHERE first=:first AND last=:last",
				Parameters: []*command.Parameter{
					{
						Value: &command.Parameter_S{
							S: "clarke",
						},
						Name: "last",
					},
					{
						Value: &command.Parameter_S{
							S: "isaac",
						},
						Name: "first",
					},
				},
			},
		},
	}
	rn, err = db.Query(req, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","first","last"],"types":["integer","text","text"]}]`, asJSON(rn); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleRequest(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, first TEXT, last TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// create table-driven tests
	tests := []struct {
		name  string
		stmts []string
		exp   string
	}{
		{
			name: "insert",
			stmts: []string{
				`INSERT INTO foo(first, last) VALUES("albert", "einstein")`,
				`INSERT INTO foo(first, last) VALUES("isaac", "newton")`,
			},
			exp: `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1}]`,
		},
		{
			name: "select",
			stmts: []string{
				`SELECT * FROM foo`,
			},
			exp: `[{"columns":["id","first","last"],"types":["integer","text","text"],"values":[[1,"albert","einstein"],[2,"isaac","newton"]]}]`,
		},
		{
			name: "update",
			stmts: []string{
				`UPDATE foo SET first="isaac", last="asimov" WHERE id=2`,
			},
			exp: `[{"last_insert_id":2,"rows_affected":1}]`,
		},
		{
			name: "insert and select",
			stmts: []string{
				`INSERT INTO foo(first, last) VALUES("richard", "feynman")`,
				`SELECT COUNT(*) FROM foo`,
				`SELECT last FROM foo WHERE first="richard"`,
			},
			exp: `[{"last_insert_id":3,"rows_affected":1},{"columns":["COUNT(*)"],"types":["integer"],"values":[[3]]},{"columns":["last"],"types":["text"],"values":[["feynman"]]}]`,
		},
		{
			name: "insert and select nonexistent table",
			stmts: []string{
				`INSERT INTO foo(first, last) VALUES("paul", "dirac")`,
				`SELECT COUNT(*) FROM foo`,
				`SELECT * FROM bar`,
			},
			exp: `[{"last_insert_id":4,"rows_affected":1},{"columns":["COUNT(*)"],"types":["integer"],"values":[[4]]},{"error":"no such table: bar"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := db.RequestStringStmts(tt.stmts)
			if err != nil {
				t.Fatalf("failed to request empty statements: %s", err.Error())
			}
			if exp, got := tt.exp, asJSON(r); exp != got {
				t.Fatalf(`Test "%s" failed, unexpected results for request exp: %s got: %s`, tt.name, exp, got)
			}
		})
	}
}

// testSimpleRequestTx tests that a transaction is rolled back when an error occurs, and that
// subsequent statements after the failed statement are not processed. This also checks that
// the code which checks if the statement is a query or not works when holding a transaction.
func testSimpleRequestTx(t *testing.T, db *DB) {
	mustExecute(db, `CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	mustExecute(db, `INSERT INTO foo(id, name) VALUES(1, "fiona")`)

	request := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "declan")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(3, "dana")`,
			},
		},
		Transaction: true,
	}
	r, err := db.Request(request, false)
	if err != nil {
		t.Fatalf("failed to make request: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for request\nexp: %s\ngot: %s", exp, got)
	}
}

func testCommonTableExpressions(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE test(x foo)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO test VALUES(1)`)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.QueryStringStmt(`WITH bar AS (SELECT * FROM test) SELECT * FROM test WHERE x = 1`)
	if err != nil {
		t.Fatalf("failed to query a common table expression: %s", err.Error())
	}
	if exp, got := `[{"columns":["x"],"types":["foo"],"values":[[1]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.QueryStringStmt(`WITH bar AS (SELECT * FROM test) SELECT * FROM test WHERE x = 2`)
	if err != nil {
		t.Fatalf("failed to query a common table expression: %s", err.Error())
	}
	if exp, got := `[{"columns":["x"],"types":["foo"]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testUniqueConstraints(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, CONSTRAINT name_unique UNIQUE (name))")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	r, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for INSERT\nexp: %s\ngot: %s", exp, got)
	}

	// UNIQUE constraint should fire.
	r, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}
	if exp, got := `[{"error":"UNIQUE constraint failed: foo.name"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for INSERT\nexp: %s\ngot: %s", exp, got)
	}
}

func testPartialFail(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	r, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"},{"last_insert_id":4,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSerialize(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(3, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	dstDB, err := os.CreateTemp("", "rqlite-bak-")
	if err != nil {
		t.Fatalf("failed to create temp file: %s", err.Error())
	}
	dstDB.Close()
	defer os.Remove(dstDB.Name())

	// Get the bytes, and write to a temp file.
	b, err := db.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize database: %s", err.Error())
	}

	if !IsDELETEModeEnabled(b) {
		t.Fatalf("expected DELETE mode to be enabled")
	}

	err = os.WriteFile(dstDB.Name(), b, 0644)
	if err != nil {
		t.Fatalf("failed to write serialized database to file: %s", err.Error())
	}

	newDB, err := Open(dstDB.Name(), false, false)
	if err != nil {
		t.Fatalf("failed to open on-disk serialized database: %s", err.Error())
	}
	defer newDB.Close()
	defer os.Remove(dstDB.Name())
	ro, err := newDB.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testDump(t *testing.T, db *DB) {
	const expRows = `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[0]]}]`

	if _, err := db.ExecuteStringStmt(`CREATE TABLE Album (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("load chinook: %v", err)
	}

	// verify original row count
	rows, err := db.QueryStringStmt("SELECT COUNT(*) FROM Album")
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if got := asJSON(rows); got != expRows {
		t.Fatalf("original count: got %s want %s", got, expRows)
	}

	tests := []struct {
		name   string
		tables []string
		want   string
	}{
		{"full dump", nil, expRows},
		{"no such table", []string{"foo"}, `[{"error":"no such table: Album"}]`},
		{"single table", []string{"Album"}, expRows},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var buf strings.Builder
			if err := db.Dump(&buf, tc.tables...); err != nil {
				t.Fatalf("dump %s: %v", tc.name, err)
			}

			// load dump into new DB
			newDB, path := mustCreateOnDiskDatabase()
			defer newDB.Close()
			defer os.Remove(path)

			if _, err := newDB.ExecuteStringStmt(buf.String()); err != nil {
				t.Fatalf("load %s: %v", tc.name, err)
			}

			// verify row count in new DB
			rows, err := newDB.QueryStringStmt("SELECT COUNT(*) FROM Album")
			if err != nil {
				t.Fatalf("query %s: %v", tc.name, err)
			}
			if got := asJSON(rows); got != tc.want {
				t.Fatalf("%s: got %s want %s", tc.name, got, tc.want)
			}
		})
	}
}

func testSize(t *testing.T, db *DB) {
	if _, err := db.Size(); err != nil {
		t.Fatalf("failed to read database size: %s", err)
	}
}
func testDBFileSize(t *testing.T, db *DB) {
	if _, err := db.FileSize(); err != nil {
		t.Fatalf("failed to read database file size: %s", err)
	}
}
func testDBWALSize(t *testing.T, db *DB) {
	if _, err := db.WALSize(); err != nil {
		t.Fatalf("failed to read database WAL file size: %s", err)
	}
}

func testStmtReadOnly(t *testing.T, db *DB) {
	r, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	tests := []struct {
		name string
		sql  string
		ro   bool
		err  error
	}{
		{
			name: "CREATE TABLE statement",
			sql:  "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)",
			err:  errors.New(`table foo already exists`),
		},
		{
			name: "CREATE TABLE statement",
			sql:  "CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)",
			ro:   false,
		},
		{
			name: "SELECT statement",
			sql:  "SELECT * FROM foo",
			ro:   true,
		},
		{
			name: "SELECT statement",
			sql:  "SELECT * FROM non_existent_table",
			err:  errors.New(`no such table: non_existent_table`),
		},
		{
			name: "INSERT statement",
			sql:  "INSERT INTO foo VALUES (1, 'test')",
			ro:   false,
		},
		{
			name: "UPDATE statement",
			sql:  "UPDATE foo SET name='test' WHERE id=1",
			ro:   false,
		},
		{
			name: "DELETE statement",
			sql:  "DELETE FROM foo WHERE id=1",
			ro:   false,
		},
		{
			name: "SELECT statement with positional parameters",
			sql:  "SELECT * FROM foo WHERE id = ?",
			ro:   true,
		},
		{
			name: "SELECT statement with named parameters",
			sql:  "SELECT * FROM foo WHERE id = @id AND name = @name",
			ro:   true,
		},
		{
			name: "INSERT statement with positional parameters",
			sql:  "INSERT INTO foo VALUES (?, ?)",
			ro:   false,
		},
		{
			name: "INSERT statement with named parameters",
			sql:  "INSERT INTO foo VALUES (@id, @name)",
			ro:   false,
		},
		{
			name: "WITH clause, read-only",
			sql:  "WITH bar AS (SELECT * FROM foo WHERE id = ?) SELECT * FROM bar",
			ro:   true,
		},
		{
			name: "WITH clause, not read-only",
			sql:  "WITH bar AS (SELECT * FROM foo WHERE id = ?) DELETE FROM foo WHERE id IN (SELECT id FROM bar)",
			ro:   false,
		},
		{
			name: "Invalid statement",
			sql:  "INVALID SQL STATEMENT",
			err:  errors.New(`near "INVALID": syntax error`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readOnly, err := db.StmtReadOnly(tt.sql)

			// Check if error is as expected
			if err != nil && tt.err == nil {
				t.Fatalf("unexpected error: got %v", err)
			} else if err == nil && tt.err != nil {
				t.Fatalf("expected error: got nil")
			} else if err != nil && tt.err != nil && err.Error() != tt.err.Error() {
				t.Fatalf("unexpected error: expected %v, got %v", tt.err, err)
			}

			// Check if result is as expected
			if readOnly != tt.ro {
				t.Fatalf("unexpected readOnly: expected %v, got %v", tt.ro, readOnly)
			}
		})
	}
}

func testJSON1(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE customer(name,phone)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`INSERT INTO customer (name, phone) VALUES("fiona", json('{"mobile":"789111", "home":"123456"}'))`)
	if err != nil {
		t.Fatalf("failed to insert JSON record: %s", err.Error())
	}

	q, err := db.QueryStringStmt("SELECT customer.phone FROM customer WHERE customer.name=='fiona'")
	if err != nil {
		t.Fatalf("failed to perform simple SELECT: %s", err.Error())
	}
	if exp, got := `[{"columns":["phone"],"types":["text"],"values":[["{\"mobile\":\"789111\",\"home\":\"123456\"}"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for simple query, expected %s, got %s", exp, got)
	}
	q, err = db.QueryStringStmt("SELECT json_extract(customer.phone, '$.mobile') FROM customer")
	if err != nil {
		t.Fatalf("failed to perform simple SELECT: %s", err.Error())
	}
	if exp, got := `[{"columns":["json_extract(customer.phone, '$.mobile')"],"types":["text"],"values":[["789111"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for JSON query, expected %s, got %s", exp, got)
	}
	q, err = db.QueryStringStmt("SELECT customer.phone ->> '$.mobile' FROM customer")
	if err != nil {
		t.Fatalf("failed to perform simple SELECT: %s", err.Error())
	}
	if exp, got := `[{"columns":["customer.phone ->> '$.mobile'"],"types":["text"],"values":[["789111"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for JSON query, expected %s, got %s", exp, got)
	}
}

func testDBSTAT_table(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	q, err := db.QueryStringStmt("SELECT * FROM dbstat")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `["name","path","pageno","pagetype","ncell","payload","unused","mx_payload","pgoffset","pgsize"]`, asJSON(q[0].Columns); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testCopy(t *testing.T, db *DB) {
	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(3, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	_, err = db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	dstFile := mustTempFile()
	defer os.Remove(dstFile)
	dstDB, err := Open(dstFile, false, false)
	if err != nil {
		t.Fatalf("failed to open destination database: %s", err)
	}
	defer dstDB.Close()

	err = db.Copy(dstDB)
	if err != nil {
		t.Fatalf("failed to copy database: %s", err.Error())
	}

	d, err := IsDELETEModeEnabledSQLiteFile(dstFile)
	if err != nil {
		t.Fatalf("failed to check if destination file is in DELETE mode: %s", err.Error())
	}
	if !d {
		t.Fatalf("Destination file not marked in DELETE mode")
	}

	ro, err := dstDB.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testBackup(t *testing.T, db *DB) {
	for _, vacuum := range []bool{false, true} {
		_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
		if err != nil {
			t.Fatalf("failed to create table: %s", err.Error())
		}
		_, err = db.ExecuteStringStmt("CREATE TABLE baz (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
		if err != nil {
			t.Fatalf("failed to create table: %s", err.Error())
		}

		req := &command.Request{
			Transaction: true,
			Statements: []*command.Statement{
				{
					Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
				},
				{
					Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
				},
				{
					Sql: `INSERT INTO foo(id, name) VALUES(3, "fiona")`,
				},
				{
					Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
				},
			},
		}
		_, err = db.Execute(req, false)
		if err != nil {
			t.Fatalf("failed to insert records: %s", err.Error())
		}

		dstDB := mustTempFile()
		defer os.Remove(dstDB)

		err = db.Backup(dstDB, vacuum)
		if err != nil {
			t.Fatalf("failed to backup database: %s", err.Error())
		}
		d, err := IsDELETEModeEnabledSQLiteFile(dstDB)
		if err != nil {
			t.Fatalf("failed to check if backup file is in DELETE mode: %s", err.Error())
		}
		if !d {
			t.Fatalf("Backup file not marked in DELETE mode")
		}

		newDB, err := Open(dstDB, false, false)
		if err != nil {
			t.Fatalf("failed to open backup database: %s", err.Error())
		}
		defer newDB.Close()
		defer os.Remove(dstDB)
		ro, err := newDB.QueryStringStmt(`SELECT * FROM foo`)
		if err != nil {
			t.Fatalf("failed to query table: %s", err.Error())
		}
		if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
		ro, err = newDB.QueryStringStmt(`SELECT name FROM sqlite_master`)
		if err != nil {
			t.Fatalf("failed to query table: %s", err.Error())
		}
		if exp, got := `[{"columns":["name"],"types":["text"],"values":[["foo"],["baz"]]}]`, asJSON(ro); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
}

func Test_DatabaseCommonOperations(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(*testing.T, *DB)
	}{
		{"BusyTimeout", testBusyTimeout},
		{"SetSynchronousMode", testSetSynchronousMode},
		{"CompileOptions", testCompileOptions},
		{"TableNotExist", testTableNotExist},
		{"TableCreation", testTableCreation},
		{"TableCreationFTS", testTableCreationFTS},
		{"SQLiteMasterTable", testSQLiteMasterTable},
		{"SQLiteTimeTypes", testSQLiteTimeTypes},
		{"NotNULLField", testNotNULLField},
		{"RandomBlob", testSQLiteRandomBlob},
		{"BasicBLOB", testBLOB},
		{"HexQuery", testHexQuery},
		{"Strict", testSTRICT},
		{"EmptyStatements", testEmptyStatements},
		{"GeopolyStatements", testGeopoly},
		{"ReadOnlyStatements", testReadOnlyStatements},
		{"SimpleSingleStatements", testSimpleSingleStatements},
		{"SimpleStatementsNumeric", testSimpleStatementsNumeric},
		{"SimpleStatementsCollate", testSimpleStatementsCollate},
		{"SimpleExpressionStatements", testSimpleExpressionStatements},
		{"SimpleUpsertStatements", testUpsertStatements},
		{"SimpleSingleJSONStatements", testSimpleSingleJSONStatements},
		{"SimpleSingleJSONBStatements", testSimpleSingleJSONBStatements},
		{"SimpleJoinStatements", testSimpleJoinStatements},
		{"SimpleSingleConcatStatements", testSimpleSingleConcatStatements},
		{"SimpleMultiStatements", testSimpleMultiStatements},
		{"SimpleSingleMultiLineStatements", testSimpleSingleMultiLineStatements},
		{"SimpleFailingStatements_Execute", testSimpleFailingStatements_Execute},
		{"SimpleFailingStatements_Query", testSimpleFailingStatements_Query},
		{"SimplePragmaTableInfo", testSimplePragmaTableInfo},
		{"WriteOnQueryDatabaseShouldFail", testWriteOnQueryDatabaseShouldFail},
		{"SimpleParameterizedStatements", testSimpleParameterizedStatements},
		{"SimpleParameterizedStatements_IN", testSimpleParameterizedStatements_IN},
		{"SimpleTwoParameterizedStatements", testSimpleTwoParameterizedStatements},
		{"SimpleNilParameterizedStatements", testSimpleNilParameterizedStatements},
		{"SimpleNamedParameterizedStatements", testSimpleNamedParameterizedStatements},
		{"SimpleRequest", testSimpleRequest},
		{"SimpleRequestTx", testSimpleRequestTx},
		{"CommonTableExpressions", testCommonTableExpressions},
		{"UniqueConstraints", testUniqueConstraints},
		{"PartialFail", testPartialFail},
		{"Serialize", testSerialize},
		{"Dump", testDump},
		{"Size", testSize},
		{"DBFileSize", testDBFileSize},
		{"DBWALSize", testDBWALSize},
		{"StmtReadOnly", testStmtReadOnly},
		{"JSON1", testJSON1},
		{"DBSTAT_table", testDBSTAT_table},
		{"Copy", testCopy},
		{"Backup", testBackup},
	}

	for _, tc := range testCases {
		db, path := mustCreateOnDiskDatabaseWAL()
		defer db.Close()
		defer os.Remove(path)
		t.Run(tc.name+":wal", func(t *testing.T) {
			tc.testFunc(t, db)
		})
	}
}
