package db

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"fmt"
)

/*
 * Lowest-layer database tests
 */

func Test_Config(t *testing.T) {
	c := NewConfig()
	c.DSN = "cache=shared&mode=memory"
	if c.FQDSN("/foo/bar/db.sqlite") != "file:/foo/bar/db.sqlite?cache=shared&mode=memory" {
		t.Fatalf("Fully qualified DSN not correct, got: %s", c.FQDSN("/foo/bar/db.sqlite"))
	}
}

func Test_DbFileCreation(t *testing.T) {
	dir, err := ioutil.TempDir("", "rqlite-test-")
	defer os.RemoveAll(dir)

	db, err := Open(path.Join(dir, "test_db"))
	if err != nil {
		t.Fatalf("failed to open new database: %s", err.Error())
	}
	if db == nil {
		t.Fatal("database is nil")
	}
	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close database: %s", err.Error())
	}
}

func Test_TableCreation(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	r, err := db.Query([]string{"SELECT * FROM foo"}, false, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func Test_SimpleSingleStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[1,"fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	_, err = db.Execute([]string{`INSERT INTO foo(name) VALUES("aoife")`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err = db.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[1,"fiona"],[2,"aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.Query([]string{`SELECT * FROM foo WHERE name="aoife"`}, false, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[2,"aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.Query([]string{`SELECT * FROM foo WHERE name="dana"`}, false, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.Query([]string{`SELECT * FROM foo ORDER BY name`}, false, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[2,"aoife"],[1,"fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.Query([]string{`SELECT *,name FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name","name"],"values":[[1,"fiona","fiona"],[2,"aoife","aoife"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SimpleMultiStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	re, err := db.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`, `INSERT INTO foo(name) VALUES("dana")`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1}]`, asJSON(re); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	ro, err := db.Query([]string{`SELECT * FROM foo`, `SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[1,"fiona"],[2,"dana"]]},{"columns":["id","name"],"values":[[1,"fiona"],[2,"dana"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SimpleFailingStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	r, err := db.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`}, false, false)
	if err != nil {
		t.Fatalf("error executing insertion into non-existent table: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: foo"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.Execute([]string{`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	r, err = db.Execute([]string{`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`}, false, false)
	if err != nil {
		t.Fatalf("failed to attempt creation of duplicate table: %s", err.Error())
	}
	if exp, got := `[{"error":"table foo already exists"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.Execute([]string{`INSERT INTO foo(id, name) VALUES(11, "fiona")`}, false, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":11,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	r, err = db.Execute([]string{`INSERT INTO foo(id, name) VALUES(11, "fiona")`}, false, false)
	if err != nil {
		t.Fatalf("failed to attempt duplicate record insertion: %s", err.Error())
	}
	if exp, got := `[{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	ro, err := db.Query([]string{`SELECT * FROM bar`}, false, false)
	if err != nil {
		t.Fatalf("failed to attempt query of non-existant table: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: bar"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	ro, err = db.Query([]string{`SELECTxx * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to attempt nonsense query: %s", err.Error())
	}
	if exp, got := `[{"error":"near \"SELECTxx\": syntax error"}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	r, err = db.Execute([]string{`utter nonsense`}, false, false)
	if err != nil {
		t.Fatalf("failed to attempt nonsense execution: %s", err.Error())
	}
	if exp, got := `[{"error":"near \"utter\": syntax error"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_PartialFail(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	stmts := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
	}
	r, err := db.Execute(stmts, false, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"},{"last_insert_id":4,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[1,"fiona"],[2,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SimpleTransaction(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	stmts := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(3, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
	}
	r, err := db.Execute(stmts, true, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"last_insert_id":3,"rows_affected":1},{"last_insert_id":4,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_PartialFailTransaction(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	stmts := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
	}
	r, err := db.Execute(stmts, true, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.Query([]string{`SELECT * FROM foo`}, false, false)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_Backup(t *testing.T) {
	db, path := mustCreateDatabase()
	fmt.Println(path)
	defer db.Close()
	//defer os.Remove(path)

	_, err := db.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	stmts := []string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(3, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
	}
	_, err = db.Execute(stmts, true, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}

	bk, err := db.Backup()
	if err != nil {
		t.Fatalf("failed to backup database: %s", err.Error())
	}

	src, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read database file: %s", err.Error())
	}

	if !bytes.Equal(bk, src) {
		t.Fatalf("backup is not the same as source")
	}
}

func mustCreateDatabase() (*DB, string) {
	var err error
	f, err := ioutil.TempFile("", "rqlilte-test-")
	if err != nil {
		panic("failed to create temp file")
	}
	f.Close()

	db, err := Open(f.Name())
	if err != nil {
		panic("failed to open database")
	}

	return db, f.Name()
}

func asJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic("failed to JSON marshal value")
	}
	return string(b)
}
