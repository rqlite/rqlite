package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/command/encoding"
	"github.com/rqlite/rqlite/db/testdata"
	"github.com/rqlite/rqlite/testdata/chinook"
)

/*
 * Lowest-layer database tests
 */

func Test_DbFileCreation(t *testing.T) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)
	dbPath := path.Join(dir, "test_db")

	db, err := Open(dbPath, false)
	if err != nil {
		t.Fatalf("failed to open new database: %s", err.Error())
	}
	if db == nil {
		t.Fatal("database is nil")
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("%s does not exist after open", dbPath)
	}
	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close database: %s", err.Error())
	}
}

func Test_CompileOptions(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.CompileOptions()
	if err != nil {
		t.Fatalf("failed to retrieve compilation options: %s", err.Error())
	}
}

func Test_TableNotExist(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	q, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: foo"}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func Test_TableCreation(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

// Test_TableCreationInMemory tests basic operation of an in-memory database,
// ensuring that using different connection objects (as the Execute and Query
// will do) works properly i.e. that the connections object work on the same
// in-memory database.
func Test_TableCreationInMemory(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

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

// Test_TableCreationInMemoryFK ensures foreign key constraints work
func Test_TableCreationInMemoryFK(t *testing.T) {
	createTableFoo := "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"
	createTableBar := "CREATE TABLE bar (fooid INTEGER NOT NULL PRIMARY KEY, FOREIGN KEY(fooid) REFERENCES foo(id))"
	insertIntoBar := "INSERT INTO bar(fooid) VALUES(1)"

	db := mustCreateInMemoryDatabase()
	defer db.Close()

	r, err := db.ExecuteStringStmt(createTableFoo)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(createTableBar)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(insertIntoBar)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	// Now, do same testing with FK constraints enabled.
	dbFK := mustCreateInMemoryDatabaseFK()
	defer dbFK.Close()

	r, err = dbFK.ExecuteStringStmt(createTableFoo)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = dbFK.ExecuteStringStmt(createTableBar)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = dbFK.ExecuteStringStmt(insertIntoBar)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"error":"FOREIGN KEY constraint failed"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func Test_SQLiteMasterTable(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

func Test_LoadIntoMemory(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	r, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	inmem, err := LoadIntoMemory(path, false)
	if err != nil {
		t.Fatalf("failed to create loaded in-memory database: %s", err.Error())
	}

	// Ensure it has been loaded correctly into the database
	r, err = inmem.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func Test_DeserializeIntoMemory(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

	// Get byte representation of database on disk which, according to SQLite docs
	// is the same as a serialized version.
	b, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read database on disk: %s", err.Error())
	}

	newDB, err := DeserializeIntoMemory(b, false)
	if err != nil {
		t.Fatalf("failed to deserialize database: %s", err.Error())
	}
	defer newDB.Close()

	ro, err := newDB.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Write a lot of records to the new database, to ensure it's fully functional.
	req = &command.Request{
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(name) VALUES("fiona")`,
			},
		},
	}
	for i := 0; i < 5000; i++ {
		_, err = newDB.Execute(req, false)
		if err != nil {
			t.Fatalf("failed to insert records: %s", err.Error())
		}
	}
	ro, err = newDB.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":[""],"values":[[5004]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_EmptyStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.ExecuteStringStmt("")
	if err != nil {
		t.Fatalf("failed to execute empty statement: %s", err.Error())
	}
	_, err = db.ExecuteStringStmt(";")
	if err != nil {
		t.Fatalf("failed to execute empty statement with semicolon: %s", err.Error())
	}
}

func Test_SimpleSingleStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

func Test_SimpleSingleJSONStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

func Test_SimpleJoinStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

func Test_SimpleSingleConcatStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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
	if exp, got := `[{"columns":["id || \"_bar\"","name"],"types":["","text"],"values":[["1_bar","fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SimpleMultiStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

func Test_SimpleSingleMultiLineStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

func Test_SimpleFailingStatements_Execute(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	r, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into non-existent table: %s", err.Error())
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

func Test_SimpleFailingStatements_Query(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	ro, err := db.QueryStringStmt(`SELECT * FROM bar`)
	if err != nil {
		t.Fatalf("failed to attempt query of non-existent table: %s", err.Error())
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
	r, err := db.QueryStringStmt(`utter nonsense`)
	if err != nil {
		if exp, got := `[{"error":"near \"utter\": syntax error"}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
}

func Test_SimplePragmaTableInfo(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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
	if exp, got := `[{"columns":["cid","name","type","notnull","dflt_value","pk"],"types":["","","","","",""],"values":[[0,"id","INTEGER",1,null,1],[1,"name","TEXT",0,null,0]]}]`, asJSON(res); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_WriteOnQueryOnDiskDatabase(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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
	if exp, got := `[{"error":"attempt to write a readonly database"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	ro, err = db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":[""],"values":[[1]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_WriteOnQueryInMemDatabase(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

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
	if exp, got := `[{"error":"attempt to write a readonly database"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err = db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":[""],"values":[[1]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_ConcurrentQueriesInMemory(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	r, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	for i := 0; i < 5000; i++ {
		r, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
		if err != nil {
			t.Fatalf("failed to insert record: %s", err.Error())
		}
		if exp, got := fmt.Sprintf(`[{"last_insert_id":%d,"rows_affected":1}]`, i+1), asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ro, err := db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
			if err != nil {
				t.Fatalf("failed to query table: %s", err.Error())
			}
			if exp, got := `[{"columns":["COUNT(*)"],"types":[""],"values":[[5000]]}]`, asJSON(ro); exp != got {
				t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
			}
		}()
	}
	wg.Wait()
}

func Test_SimpleParameterizedStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

func Test_CommonTableExpressions(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

func Test_UniqueConstraints(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

// Test_ConnectionIsolation test that ISOLATION behavior of on-disk databases doesn't
// change unexpectedly.
func Test_ConnectionIsolation(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	r, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt("BEGIN")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}

	q, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt("COMMIT")
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}

	q, err = db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

// Test_ConnectionIsolationMemory test that ISOLATION behavior of in-memory databases doesn't
// change unexpectedly.
func Test_ConnectionIsolationMemory(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	r, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt("BEGIN")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("error executing insertion into table: %s", err.Error())
	}

	_, err = db.QueryStringStmt("SELECT * FROM foo")
	if err == nil {
		t.Fatalf("SELECT should have received error")
	}
	if exp, got := "database is locked", err.Error(); exp != got {
		t.Fatalf("error incorrect, exp %s, got %s", exp, got)
	}
}

func Test_Size(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	if _, err := db.Size(); err != nil {
		t.Fatalf("failed to read database size: %s", err)
	}
}

func Test_DBFileSize(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	if _, err := db.FileSize(); err != nil {
		t.Fatalf("failed to read database file size: %s", err)
	}
}

func Test_PartialFail(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

func Test_SimpleTransaction(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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
	r, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"last_insert_id":3,"rows_affected":1},{"last_insert_id":4,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_PartialFailTransaction(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_Backup(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

	dstDB := mustTempFile()
	defer os.Remove(dstDB)

	err = db.Backup(dstDB)
	if err != nil {
		t.Fatalf("failed to backup database: %s", err.Error())
	}

	newDB, err := Open(dstDB, false)
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
}

func Test_Copy(t *testing.T) {
	srcDB, path := mustCreateDatabase()
	defer srcDB.Close()
	defer os.Remove(path)

	_, err := srcDB.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
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
	_, err = srcDB.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	dstFile := mustTempFile()
	defer os.Remove(dstFile)
	dstDB, err := Open(dstFile, false)
	if err != nil {
		t.Fatalf("failed to open destination database: %s", err)
	}
	defer dstDB.Close()

	err = srcDB.Copy(dstDB)
	if err != nil {
		t.Fatalf("failed to copy database: %s", err.Error())
	}

	ro, err := dstDB.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_Serialize(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

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

	dstDB, err := ioutil.TempFile("", "rqlite-bak-")
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
	err = ioutil.WriteFile(dstDB.Name(), b, 0644)
	if err != nil {
		t.Fatalf("failed to write serialized database to file: %s", err.Error())
	}

	newDB, err := Open(dstDB.Name(), false)
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

func Test_Dump(t *testing.T) {
	t.Parallel()

	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.ExecuteStringStmt(chinook.DB)
	if err != nil {
		t.Fatalf("failed to load chinook dump: %s", err.Error())
	}

	var b strings.Builder
	if err := db.Dump(&b); err != nil {
		t.Fatalf("failed to dump database: %s", err.Error())
	}

	if b.String() != chinook.DB {
		t.Fatal("dumped database does not equal entered database")
	}
}

func Test_DumpMemory(t *testing.T) {
	t.Parallel()

	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	inmem, err := LoadIntoMemory(path, false)
	if err != nil {
		t.Fatalf("failed to create loaded in-memory database: %s", err.Error())
	}

	_, err = inmem.ExecuteStringStmt(chinook.DB)
	if err != nil {
		t.Fatalf("failed to load chinook dump: %s", err.Error())
	}

	var b strings.Builder
	if err := inmem.Dump(&b); err != nil {
		t.Fatalf("failed to dump database: %s", err.Error())
	}

	if b.String() != chinook.DB {
		t.Fatal("dumped database does not equal entered database")
	}
}

// Test_1GiBInMemory tests that in-memory databases larger than 1GiB,
// but smaller than 2GiB, can be created without issue.
func Test_1GiBInMemory(t *testing.T) {
	t.Parallel()
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, txt TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	stmt := fmt.Sprintf(`INSERT INTO foo(txt) VALUES("%s")`, text.Lorum)
	for i := 0; i < 1715017; i++ {
		r, err := db.ExecuteStringStmt(stmt)
		if err != nil {
			t.Fatalf("failed to Execute statement %s", err.Error())
		}
		if len(r) != 1 {
			t.Fatalf("unexpected length for Execute results: %d", len(r))
		}
		if r[0].GetError() != "" {
			t.Fatalf("failed to insert record: %s", r[0].GetError())
		}
	}

	r, err := db.ExecuteStringStmt(stmt)
	if err != nil {
		t.Fatalf("failed to insert record %s", err.Error())
	}
	fmt.Println(r)

	sz, err := db.Size()
	if err != nil {
		t.Fatalf("failed to get size: %s", err.Error())
	}
	if sz <= 1024*1024*1024 {
		t.Fatalf("failed to create a database greater than 1 GiB in size: %d", sz)
	}
}

// Test_ParallelOperationsInMemory runs multiple accesses concurrently, ensuring
// that correct results are returned in every goroutine. It's not 100% that this
// test would bring out a bug, but it's almost 100%.
//
// See https://github.com/mattn/go-sqlite3/issues/959#issuecomment-890283264
func Test_ParallelOperationsInMemory(t *testing.T) {
	t.Parallel()
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	if _, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	if _, err := db.ExecuteStringStmt("CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	if _, err := db.ExecuteStringStmt("CREATE TABLE qux (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// Confirm schema is as expected, when checked from same goroutine.
	if rows, err := db.QueryStringStmt(`SELECT sql FROM sqlite_master`); err != nil {
		t.Fatalf("failed to query for schema after creation: %s", err.Error())
	} else {
		if exp, got := `[{"columns":["sql"],"types":["text"],"values":[["CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"],["CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"],["CREATE TABLE qux (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"]]}]`, asJSON(rows); exp != got {
			t.Fatalf("schema not as expected during after creation, exp %s, got %s", exp, got)
		}
	}

	var exWg sync.WaitGroup
	exWg.Add(3)

	foo := make(chan time.Time)
	bar := make(chan time.Time)
	qux := make(chan time.Time)
	done := make(chan bool)

	ticker := time.NewTicker(1 * time.Millisecond)
	go func() {
		for {
			select {
			case t := <-ticker.C:
				foo <- t
				bar <- t
				qux <- t
			case <-done:
				close(foo)
				close(bar)
				close(qux)
				return
			}
		}
	}()

	go func() {
		defer exWg.Done()
		for range foo {
			if _, err := db.ExecuteStringStmt(`INSERT INTO foo(id, name) VALUES(1, "fiona")`); err != nil {
				t.Fatalf("failed to insert records into foo: %s", err.Error())
			}
		}
	}()
	go func() {
		defer exWg.Done()
		for range bar {
			if _, err := db.ExecuteStringStmt(`INSERT INTO bar(id, name) VALUES(1, "fiona")`); err != nil {
				t.Fatalf("failed to insert records into bar: %s", err.Error())
			}
		}
	}()
	go func() {
		defer exWg.Done()
		for range qux {
			if _, err := db.ExecuteStringStmt(`INSERT INTO qux(id, name) VALUES(1, "fiona")`); err != nil {
				t.Fatalf("failed to insert records into qux: %s", err.Error())
			}
		}
	}()

	var qWg sync.WaitGroup
	qWg.Add(3)
	for i := 0; i < 3; i++ {
		go func(j int) {
			defer qWg.Done()
			var n int
			for {
				if rows, err := db.QueryStringStmt(`SELECT sql FROM sqlite_master`); err != nil {
					t.Fatalf("failed to query for schema during goroutine %d execution: %s", j, err.Error())
				} else {
					n++
					if exp, got := `[{"columns":["sql"],"types":["text"],"values":[["CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"],["CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"],["CREATE TABLE qux (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"]]}]`, asJSON(rows); exp != got {
						t.Fatalf("schema not as expected during goroutine execution, exp %s, got %s, after %d queries", exp, got, n)
					}
				}
				if n == 500000 {
					break
				}
			}
		}(i)
	}
	qWg.Wait()

	close(done)
	exWg.Wait()
}

func Test_DBSTAT_table(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

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

func Test_JSON1(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	_, err := db.ExecuteStringStmt("CREATE TABLE customer(name,phone);")
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
	if exp, got := `[{"columns":["phone"],"types":[""],"values":[["{\"mobile\":\"789111\",\"home\":\"123456\"}"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for simple query, expected %s, got %s", exp, got)
	}
	q, err = db.QueryStringStmt("SELECT json_extract(customer.phone, '$.mobile') FROM customer;")
	if err != nil {
		t.Fatalf("failed to perform simple SELECT: %s", err.Error())
	}
	if exp, got := `[{"columns":["json_extract(customer.phone, '$.mobile')"],"types":[""],"values":[["789111"]]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for JSON query, expected %s, got %s", exp, got)
	}
}

func mustCreateDatabase() (*DB, string) {
	var err error
	f := mustTempFile()
	db, err := Open(f, false)
	if err != nil {
		panic("failed to open database")
	}

	return db, f
}

func mustCreateInMemoryDatabase() *DB {
	db, err := OpenInMemory(false)
	if err != nil {
		panic("failed to open in-memory database")
	}
	return db
}

func mustCreateInMemoryDatabaseFK() *DB {
	db, err := OpenInMemory(true)
	if err != nil {
		panic("failed to open in-memory database with foreign key constraints")
	}
	return db
}

func mustWriteAndOpenDatabase(b []byte) (*DB, string) {
	var err error
	f := mustTempFile()
	err = ioutil.WriteFile(f, b, 0660)
	if err != nil {
		panic("failed to write file")
	}

	db, err := Open(f, false)
	if err != nil {
		panic("failed to open database")
	}
	return db, f
}

// mustExecute executes a statement, and panics on failure. Used for statements
// that should never fail, even taking into account test setup.
func mustExecute(db *DB, stmt string) {
	_, err := db.ExecuteStringStmt(stmt)
	if err != nil {
		panic(fmt.Sprintf("failed to execute statement: %s", err.Error()))
	}
}

// mustQuery executes a statement, and panics on failure. Used for statements
// that should never fail, even taking into account test setup.
func mustQuery(db *DB, stmt string) {
	_, err := db.QueryStringStmt(stmt)
	if err != nil {
		panic(fmt.Sprintf("failed to query: %s", err.Error()))
	}
}

func asJSON(v interface{}) string {
	b, err := encoding.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}

// mustTempFile returns a path to a temporary file in directory dir. It is up to the
// caller to remove the file once it is no longer needed.
func mustTempFile() string {
	tmpfile, err := ioutil.TempFile("", "rqlite-db-test")
	if err != nil {
		panic(err.Error())
	}
	tmpfile.Close()
	return tmpfile.Name()
}
