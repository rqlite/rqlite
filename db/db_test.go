package db

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

/*
 * Lowest-layer database tests
 */

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

	_, err := db.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	r, err := db.Query([]string{"SELECT * FROM foo"}, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"]}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func Test_SimpleSingleStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`}, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.Query([]string{`SELECT * FROM foo`}, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[1,"fiona"]]}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	_, err = db.Execute([]string{`INSERT INTO foo(name) VALUES("aoife")`}, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err = db.Query([]string{`SELECT * FROM foo`}, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[1,"fiona"],[2,"aoife"]]}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.Query([]string{`SELECT * FROM foo WHERE name="aoife"`}, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[2,"aoife"]]}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.Query([]string{`SELECT * FROM foo WHERE name="dana"`}, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"]}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.Query([]string{`SELECT * FROM foo ORDER BY name`}, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[2,"aoife"],[1,"fiona"]]}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	r, err = db.Query([]string{`SELECT *,name FROM foo`}, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name","name"],"values":[[1,"fiona","fiona"],[2,"aoife","aoife"]]}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SimpleMultiStatements(t *testing.T) {
	db, path := mustCreateDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.Execute([]string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"}, false)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	_, err = db.Execute([]string{`INSERT INTO foo(name) VALUES("fiona")`}, false)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}

	r, err := db.Query([]string{`SELECT * FROM foo`, `SELECT * FROM foo`}, false)
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"values":[[1,"fiona"]]},{"columns":["id","name"],"values":[[1,"fiona"]]}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

// func Test_FailingSimpleStatements(t *testing.T) {
// 	db, path := mustCreateDatabase()
// 	defer db.Close()
// 	defer os.Remove(path)

// 	err := db.Execute(`INSERT INTO foo(name) VALUES("fiona")`)
// 	if err == nil {
// 		t.Fatal("inserted record into empty table OK")
// 	}
// 	if err.Error() != "no such table: foo" {
// 		t.Fatal("unexpected error returned")
// 	}

// 	if err = db.Execute("create table foo (id integer not null primary key, name text)"); err != nil {
// 		t.Fatalf("failed to create table: %s", err.Error())
// 	}
// 	if err = db.Execute("create table foo (id integer not null primary key, name text)"); err == nil {
// 		t.Fatal("duplicate table created OK")
// 	}
// 	if err.Error() != "table foo already exists" {
// 		t.Fatalf("unexpected error returned: %s", err.Error())
// 	}

// 	if err = db.Execute(`INSERT INTO foo(id, name) VALUES(11, "fiona")`); err != nil {
// 		t.Fatalf("failed to insert record: %s", err.Error())
// 	}
// 	if err = db.Execute(`INSERT INTO foo(id, name) VALUES(11, "fiona")`); err == nil {
// 		t.Fatal("duplicated record inserted OK")
// 	}
// 	if err.Error() != "UNIQUE constraint failed: foo.id" {
// 		t.Fatal("unexpected error returned")
// 	}

// 	if err = db.Execute("SELECT * FROM bar"); err == nil {
// 		t.Fatal("no error when querying non-existant table")
// 	}
// 	if err.Error() != "no such table: bar" {
// 		t.Fatal("unexpected error returned")
// 	}

// 	if err = db.Execute("utter nonsense"); err == nil {
// 		t.Fatal("no error when issuing nonsense query")
// 	}
// 	if err.Error() != `near "utter": syntax error` {
// 		t.Fatal("unexpected error returned")
// 	}
// }

// func Test_SimpleTransactions(t *testing.T) {
// 	db, path := mustCreateDatabase()
// 	defer db.Close()
// 	defer os.Remove(path)

// 	if err := db.Execute("create table foo (id integer not null primary key, name text)"); err != nil {
// 		t.Fatalf("failed to create table: %s", err.Error())
// 	}

// 	if err := db.StartTransaction(); err != nil {
// 		t.Fatalf("failed to start transaction: %s", err.Error())
// 	}
// 	for i := 0; i < 10; i++ {
// 		_ = db.Execute(`INSERT INTO foo(name) VALUES("philip")`)
// 	}
// 	if err := db.CommitTransaction(); err != nil {
// 		t.Fatalf("failed to commit transaction: %s", err.Error())
// 	}

// 	r, err := db.Query("SELECT name FROM foo")
// 	if err != nil {
// 		t.Fatalf("failed to query after commited transaction: %s", err.Error())
// 	}
// 	if len(r) != 10 {
// 		t.Fatalf("incorrect number of results returned: %d", len(r))
// 	}

// 	if err := db.StartTransaction(); err != nil {
// 		t.Fatalf("failed to start transaction: %s", err.Error())
// 	}
// 	for i := 0; i < 10; i++ {
// 		_ = db.Execute(`INSERT INTO foo(name) VALUES("philip")`)
// 	}
// 	if err := db.RollbackTransaction(); err != nil {
// 		t.Fatalf("failed to rollback transaction: %s", err.Error())
// 	}

// 	r, err = db.Query("SELECT name FROM foo")
// 	if err != nil {
// 		t.Fatalf("failed to query after commited transaction: %s", err.Error())
// 	}
// 	if len(r) != 10 {
// 		t.Fatalf("incorrect number of results returned: %d", len(r))
// 	}
// }

// func Test_TransactionsConstraintViolation(t *testing.T) {
// 	db, path := mustCreateDatabase()
// 	defer db.Close()
// 	defer os.Remove(path)
// 	var err error

// 	if err = db.Execute("create table foo (id integer not null primary key, name text)"); err != nil {
// 		t.Fatalf("failed to create table: %s", err.Error())
// 	}

// 	if err = db.StartTransaction(); err != nil {
// 		t.Fatalf("failed to start transaction: %s", err.Error())
// 	}
// 	if err = db.Execute(`INSERT INTO foo(id, name) VALUES(11, "fiona")`); err != nil {
// 		t.Fatalf("failed to insert record: %s", err.Error())
// 	}
// 	if err = db.Execute(`INSERT INTO foo(id, name) VALUES(11, "fiona")`); err == nil {
// 		t.Fatal("duplicated record inserted OK")
// 	}
// 	if err.Error() != "UNIQUE constraint failed: foo.id" {
// 		t.Fatal("unexpected error returned")
// 	}
// 	if err := db.RollbackTransaction(); err != nil {
// 		t.Fatalf("failed to rollback transaction: %s", err.Error())
// 	}
// 	r, err := db.Query("SELECT name FROM foo")
// 	if err != nil {
// 		t.Fatalf("failed to query after commited transaction: %s", err.Error())
// 	}
// 	if len(r) != 0 {
// 		t.Fatalf("incorrect number of results returned: %d", len(r))
// 	}
// }

// func Test_TransactionsHardFail(t *testing.T) {
// 	db, path := mustCreateDatabase()
// 	defer db.Close()
// 	defer os.Remove(path)
// 	var err error

// 	if err = db.Execute("create table foo (id integer not null primary key, name text)"); err != nil {
// 		t.Fatalf("failed to create table: %s", err.Error())
// 	}
// 	if err = db.Execute(`INSERT INTO foo(id, name) VALUES(1, "fiona")`); err != nil {
// 		t.Fatalf("failed to insert record: %s", err.Error())
// 	}

// 	if err = db.StartTransaction(); err != nil {
// 		t.Fatalf("failed to start transaction: %s", err.Error())
// 	}
// 	if err = db.Execute(`INSERT INTO foo(id, name) VALUES(2, "fiona")`); err != nil {
// 		t.Fatalf("failed to insert record: %s", err.Error())
// 	}
// 	if err = db.Close(); err != nil {
// 		t.Fatalf("failed to close database: %s", err.Error())
// 	}

// 	db, err = Open(path)
// 	if err != nil {
// 		t.Fatalf("failed to re-open database: %s", err.Error())
// 	}
// 	r, err := db.Query("SELECT * FROM foo")
// 	if err != nil {
// 		t.Fatalf("failed to query after hard close: %s", err.Error())
// 	}
// 	if len(r) != 1 {
// 		t.Fatalf("incorrect number of results returned: %d", len(r))
// 	}
// }

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

func asJson(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic("failed to JSON marshal value")
	}
	return string(b)
}
