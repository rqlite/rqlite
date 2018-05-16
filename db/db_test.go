package db

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/testdata/chinook"
)

/*
 * Lowest-layer database tests
 */

func TestNewDBOnDisk(t *testing.T) {
	t.Parallel()

	db, err := New(mustTempFilename(), "", false)
	if err != nil {
		t.Fatalf("failed to create new database: %s", err.Error())
	}
	if db == nil {
		t.Fatal("database is nil")
	}
}

func TestNewDBInMemory(t *testing.T) {
	t.Parallel()

	db, err := New(mustTempFilename(), "", true)
	if err != nil {
		t.Fatalf("failed to create new database: %s", err.Error())
	}
	if db == nil {
		t.Fatal("database is nil")
	}
}

func TestDBCreateOnDiskConnection(t *testing.T) {
	t.Parallel()

	db, path := mustCreateDatabase()
	defer os.Remove(path)

	conn, err := db.Connect()
	if err != nil {
		t.Fatalf("failed to create connection to on-disk database: %s", err.Error())
	}
	if conn == nil {
		t.Fatal("connection to on-disk database is nil")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("failed to close on-disk connection: %s", err.Error())
	}
}

func TestDBCreateInMemoryConnection(t *testing.T) {
	t.Parallel()

	db := mustCreateInMemoryDatabase()

	conn, err := db.Connect()
	if err != nil {
		t.Fatalf("failed to create connection to in-memory database: %s", err.Error())
	}
	if conn == nil {
		t.Fatal("connection to in-memory database is nil")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("failed to close in-memory connection: %s", err.Error())
	}
}

func TestDumpOnDisk(t *testing.T) {
	t.Parallel()

	db, path := mustCreateDatabase()
	defer os.Remove(path)
	c, err := db.Connect()
	if err != nil {
		t.Fatalf("failed to create connection to on-disk database: %s", err.Error())
	}

	_, err = c.Execute([]string{chinook.DB}, false, false)
	if err != nil {
		t.Fatalf("failed to load chinook dump: %s", err.Error())
	}

	var b strings.Builder
	if err := c.Dump(&b); err != nil {
		t.Fatalf("failed to dump database: %s", err.Error())
	}

	if b.String() != chinook.DB {
		t.Fatal("dumped database does not equal entered database")
	}
}

func TestDumpInMemory(t *testing.T) {
	t.Parallel()

	db := mustCreateInMemoryDatabase()
	c, err := db.Connect()
	if err != nil {
		t.Fatalf("failed to create connection to on-disk database: %s", err.Error())
	}

	_, err = c.Execute([]string{chinook.DB}, false, false)
	if err != nil {
		t.Fatalf("failed to load chinook dump: %s", err.Error())
	}

	var b strings.Builder
	if err := c.Dump(&b); err != nil {
		t.Fatalf("failed to dump database: %s", err.Error())
	}

	if b.String() != chinook.DB {
		t.Fatal("dumped database does not equal entered database")
	}
}

type testF func(test *testing.T, c *Conn)

var dbTestfunctions []testF = []testF{
	testTableCreation,
	testSQLiteMasterTable,
	testEmptyStatements,
	testSimpleSingleStatements,
	testSimpleJoinStatements,
	testSimpleSingleConcatStatements,
	testSimpleMultiStatements,
	testSimpleSingleMultiLineStatements,
	testSimpleFailingStatementsExecute,
	testSimpleFailingStatementsQuery,
	testSimplePragmaTableInfo,
	testCommonTableExpressions,
	testForeignKeyConstraints,
	testUniqueConstraints,
	testActiveTransaction,
	testAbortTransaction,
	testPartialFail,
	testSimpleTransaction,
	testPartialFailTransaction,
	testBackup,
	testLoad,
}

func TestDatabaseInMemory(t *testing.T) {
	t.Parallel()

	for _, f := range dbTestfunctions {
		dbInMem := mustCreateInMemoryDatabase()
		connInMem, err := dbInMem.Connect()
		if err != nil {
			t.Fatalf("failed to create connection to in-memory database: %s", err.Error())
		}

		f(t, connInMem)
		connInMem.Close()
	}
}

func TestDatabaseOnDisk(t *testing.T) {
	t.Parallel()

	for _, f := range dbTestfunctions {
		dbDisk, path := mustCreateDatabase()
		connDisk, err := dbDisk.Connect()
		if err != nil {
			t.Fatalf("failed to create connection to on-disk database: %s", err.Error())
		}

		f(t, connDisk)
		connDisk.Close()
		os.Remove(path)
	}
}

func testTableCreation(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	r, err := c.Query([]string{"SELECT * FROM foo"}, false, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testSQLiteMasterTable(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	r, err := c.Query([]string{"SELECT * FROM sqlite_master"}, false, false)
	if err != nil {
		t.Fatalf("failed to query master table: %s", err.Error())
	}
	if exp, got := `[{"columns":["type","name","tbl_name","rootpage","sql"],"types":["text","text","text","int","text"],"values":[["table","foo","foo",2,"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func testEmptyStatements(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{""}, false, false)
	if err != nil {
		t.Fatalf("failed to execute empty statement: %s", err.Error())
	}
	_, err = c.Execute([]string{";"}, false, false)
	if err != nil {
		t.Fatalf("failed to execute empty statement with semicolon: %s", err.Error())
	}
}

func testSimpleSingleStatements(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = c.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	_, err = c.Execute([]string{`INSERT INTO foo(name) VALUES("aoife")`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := c.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = c.Query([]string{`SELECT * FROM foo WHERE name="aoife"`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = c.Query([]string{`SELECT * FROM foo WHERE name="dana"`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = c.Query([]string{`SELECT * FROM foo ORDER BY name`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"aoife"],[1,"fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = c.Query([]string{`SELECT *,name FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name","name"],"types":["integer","text","text"],"values":[[1,"fiona","fiona"],[2,"aoife","aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleJoinStatements(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE names (id INTEGER NOT NULL PRIMARY KEY, name TEXT, ssn TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = c.Execute([]string{
		`INSERT INTO "names" VALUES(1,'bob','123-45-678')`,
		`INSERT INTO "names" VALUES(2,'tom','111-22-333')`,
		`INSERT INTO "names" VALUES(3,'matt','222-22-333')`,
	}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	_, err = c.Execute([]string{"CREATE TABLE staff (id INTEGER NOT NULL PRIMARY KEY, employer TEXT, ssn TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = c.Execute([]string{`INSERT INTO "staff" VALUES(1,'acme','222-22-333')`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := c.Query([]string{`SELECT names.id,name,names.ssn,employer FROM names INNER JOIN staff ON staff.ssn = names.ssn`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table using JOIN: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name","ssn","employer"],"types":["integer","text","text","text"],"values":[[3,"matt","222-22-333","acme"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleSingleConcatStatements(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = c.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := c.Query([]string{`SELECT id || "_bar", name FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id || \"_bar\"","name"],"types":["","text"],"values":[["1_bar","fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleMultiStatements(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	re, err := c.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`, `INSERT INTO foo(name) VALUES("dana")`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1}]`, asJSON(re); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	ro, err := c.Query([]string{`SELECT * FROM foo`, `SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"dana"]]},{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"dana"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleSingleMultiLineStatements(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{`
CREATE TABLE foo (
    id INTEGER NOT NULL PRIMARY KEY,
    name TEXT
)`}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	re, err := c.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`, `INSERT INTO foo(name) VALUES("dana")`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1}]`, asJSON(re); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleFailingStatementsExecute(t *testing.T, c *Conn) {
	r, err := c.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`}, false, false)
	if err != nil {
		t.Fatalf("error executing insertion into non-existent table: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: foo"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = c.Execute([]string{`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	r, err = c.Execute([]string{`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`}, false, false)
	if err != nil {
		t.Fatalf("failed to attempt creation of duplicate table: %s", err.Error())
	}
	if exp, got := `[{"error":"table foo already exists"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = c.Execute([]string{`INSERT INTO foo(id, name) VALUES(11, "fiona")`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":11,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	r, err = c.Execute([]string{`INSERT INTO foo(id, name) VALUES(11, "fiona")`}, false, false)
	if err != nil {
		t.Fatalf("failed to attempt duplicate record insertion: %s", err.Error())
	}
	if exp, got := `[{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = c.Execute([]string{`utter nonsense`}, false, false)
	if err != nil {
		if exp, got := `[{"error":"near \"utter\": syntax error"}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
}

func testSimpleFailingStatementsQuery(t *testing.T, c *Conn) {
	ro, err := c.Query([]string{`SELECT * FROM bar`}, false, false)
	if err != nil {
		t.Fatalf("failed to attempt query of non-existent table: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: bar"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	ro, err = c.Query([]string{`SELECTxx * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to attempt nonsense query: %s", err.Error())
	}
	if exp, got := `[{"error":"near \"SELECTxx\": syntax error"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	r, err := c.Query([]string{`utter nonsense`}, false, false)
	if err != nil {
		if exp, got := `[{"error":"near \"utter\": syntax error"}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
}

func testSimplePragmaTableInfo(t *testing.T, c *Conn) {
	r, err := c.Execute([]string{`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	res, err := c.Query([]string{`PRAGMA table_info("foo")`}, false, false)
	if err != nil {
		t.Fatalf("failed to query a common table expression: %s", err.Error())
	}
	if exp, got := `[{"columns":["cid","name","type","notnull","dflt_value","pk"],"types":["","","","","",""],"values":[[0,"id","INTEGER",1,null,1],[1,"name","TEXT",0,null,0]]}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

}

func testCommonTableExpressions(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE test(x foo)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = c.Execute([]string{`INSERT INTO test VALUES(1)`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := c.Query([]string{`WITH bar AS (SELECT * FROM test) SELECT * FROM test WHERE x = 1`}, false, false)
	if err != nil {
		t.Fatalf("failed to query a common table expression: %s", err.Error())
	}
	if exp, got := `[{"columns":["x"],"types":["foo"],"values":[[1]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = c.Query([]string{`WITH bar AS (SELECT * FROM test) SELECT * FROM test WHERE x = 2`}, false, false)
	if err != nil {
		t.Fatalf("failed to query a common table expression: %s", err.Error())
	}
	if exp, got := `[{"columns":["x"],"types":["foo"]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testUniqueConstraints(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, CONSTRAINT name_unique UNIQUE (name))"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	r, err := c.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`}, false, false)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for INSERT\nexp: %s\ngot: %s", exp, got)
	}

	// UNIQUE constraint should fire.
	r, err = c.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`}, false, false)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}
	if exp, got := `[{"error":"UNIQUE constraint failed: foo.name"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for INSERT\nexp: %s\ngot: %s", exp, got)
	}
}

func testActiveTransaction(t *testing.T, c *Conn) {
	if c.TransactionActive() {
		t.Fatal("transaction incorrectly marked as active")
	}

	if _, err := c.Execute([]string{`BEGIN`}, false, false); err != nil {
		t.Fatalf("error starting transaction: %s", err.Error())
	}

	if !c.TransactionActive() {
		t.Fatal("transaction incorrectly marked as inactive")
	}

	if _, err := c.Execute([]string{`COMMIT`}, false, false); err != nil {
		t.Fatalf("error starting transaction: %s", err.Error())
	}

	if c.TransactionActive() {
		t.Fatal("transaction incorrectly marked as active")
	}

	if _, err := c.Execute([]string{`BEGIN`}, false, false); err != nil {
		t.Fatalf("error starting transaction: %s", err.Error())
	}

	if !c.TransactionActive() {
		t.Fatal("transaction incorrectly marked as inactive")
	}

	if _, err := c.Execute([]string{`ROLLBACK`}, false, false); err != nil {
		t.Fatalf("error starting transaction: %s", err.Error())
	}

	if c.TransactionActive() {
		t.Fatal("transaction incorrectly marked as active")
	}
}

func testAbortTransaction(t *testing.T, c *Conn) {
	if err := c.AbortTransaction(); err != nil {
		t.Fatalf("error abrorting non-active transaction: %s", err.Error())
	}

	if _, err := c.Execute([]string{`BEGIN`}, false, false); err != nil {
		t.Fatalf("error starting transaction: %s", err.Error())
	}

	if !c.TransactionActive() {
		t.Fatal("transaction incorrectly marked as inactive")
	}

	if err := c.AbortTransaction(); err != nil {
		t.Fatalf("error abrorting non-active transaction: %s", err.Error())
	}

	if c.TransactionActive() {
		t.Fatal("transaction incorrectly marked as active")
	}
}

func testPartialFail(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	stmts := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
	}
	r, err := c.Execute(stmts, false, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"},{"last_insert_id":4,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := c.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testSimpleTransaction(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	stmts := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(3, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
	}
	r, err := c.Execute(stmts, true, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"last_insert_id":3,"rows_affected":1},{"last_insert_id":4,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := c.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testPartialFailTransaction(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	stmts := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
	}
	r, err := c.Execute(stmts, true, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := c.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testForeignKeyConstraints(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, ref INTEGER REFERENCES foo(id))"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Explicitly disable constraints.
	if err := c.EnableFKConstraints(false); err != nil {
		t.Fatalf("failed to enable foreign key constraints: %s", err.Error())
	}

	// Check constraints
	fk, err := c.FKConstraints()
	if err != nil {
		t.Fatalf("failed to check FK constraints: %s", err.Error())
	}
	if fk != false {
		t.Fatal("FK constraints are not disabled")
	}

	stmts := []string{
		`INSERT INTO foo(id, ref) VALUES(1, 2)`,
	}
	r, err := c.Execute(stmts, false, false)
	if err != nil {
		t.Fatalf("failed to execute FK test statement: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Explicitly enable constraints.
	if err := c.EnableFKConstraints(true); err != nil {
		t.Fatalf("failed to enable foreign key constraints: %s", err.Error())
	}

	// Check constraints
	fk, err = c.FKConstraints()
	if err != nil {
		t.Fatalf("failed to check FK constraints: %s", err.Error())
	}
	if fk != true {
		t.Fatal("FK constraints are not enabled")
	}

	stmts = []string{
		`INSERT INTO foo(id, ref) VALUES(1, 3)`,
	}
	r, err = c.Execute(stmts, false, false)
	if err != nil {
		t.Fatalf("failed to execute FK test statement: %s", err.Error())
	}
	if exp, got := `[{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testBackup(t *testing.T, c *Conn) {
	_, err := c.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	stmts := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(3, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
	}
	_, err = c.Execute(stmts, true, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	dstDB, path := mustCreateDatabase()
	defer os.Remove(path)
	dstConn, err := dstDB.Connect()
	if err != nil {
		t.Fatalf("failed to connect to destination database: %s", err.Error())
	}

	err = c.Backup(dstConn)
	if err != nil {
		t.Fatalf("failed to backup database: %s", err.Error())
	}

	ro, err := dstConn.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func testLoad(t *testing.T, c *Conn) {
	dstDB, path := mustCreateDatabase()
	defer os.Remove(path)
	dstConn, err := dstDB.Connect()
	if err != nil {
		t.Fatalf("failed to connect to destination database: %s", err.Error())
	}
	defer dstConn.Close()

	_, err = dstConn.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	stmts := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(3, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
	}
	_, err = dstConn.Execute(stmts, true, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	ro, err := dstConn.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	err = c.Load(dstConn)
	if err != nil {
		t.Fatalf("failed to load database: %s", err.Error())
	}

	ro, err = c.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

// mustExecute executes a spath, and panics on failure. Used for statements
// that should never fail, even taking into account test setup.
func mustExecute(c *Conn, stmt string) {
	_, err := c.Execute([]string{stmt}, false, false)
	if err != nil {
		panic(fmt.Sprintf("failed to execute statement: %s", err.Error()))
	}
}

// mustQuery executes a statement, and panics on failure. Used for statements
// that should never fail, even taking into account test setup.
func mustQuery(c *Conn, stmt string) {
	_, err := c.Query([]string{stmt}, false, false)
	if err != nil {
		panic(fmt.Sprintf("failed to query: %s", err.Error()))
	}
}

func mustCreateDatabase() (*DB, string) {
	path := mustTempFilename()
	db, err := New(mustTempFilename(), "", false)
	if err != nil {
		panic("failed to create in-memory database")
	}
	return db, path
}

func mustCreateInMemoryDatabase() *DB {
	db, err := New(mustTempFilename(), "", true)
	if err != nil {
		panic("failed to create in-memory database")
	}
	return db
}

func mustTempFilename() string {
	fd, err := ioutil.TempFile("", "rqlilte-tmp-test-")
	if err != nil {
		panic(err.Error())
	}
	if err := fd.Close(); err != nil {
		panic(err.Error())
	}
	if err := os.Remove(fd.Name()); err != nil {
		panic(err.Error())
	}
	return fd.Name()
}

func asJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic("failed to JSON marshal value")
	}
	return string(b)
}
