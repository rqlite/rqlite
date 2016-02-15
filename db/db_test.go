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
	db, path := mustOpenDatabase()
	defer db.Close()
	defer os.Remove(path)

	err := db.Execute("create table foo (id integer not null primary key, name text)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	r, err := db.Query("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if len(r) != 0 {
		t.Fatalf("expected 0 results, got %d", len(r))
	}
}

func Test_SimpleStatements(t *testing.T) {
	db, path := mustOpenDatabase()
	defer db.Close()
	defer os.Remove(path)

	if err := db.Execute("create table foo (id integer not null primary key, name text)"); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	if err := db.Execute(`INSERT INTO foo(name) VALUES("fiona")`); err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	r, err := db.Query("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"id":"1","name":"fiona"}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	if err := db.Execute(`INSERT INTO foo(name) VALUES("aoife")`); err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	r, err = db.Query("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"id":"1","name":"fiona"},{"id":"2","name":"aoife"}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	if err := db.Execute("UPDATE foo SET Name='Who knows?' WHERE Id=1"); err != nil {
		t.Fatalf("failed to update record: %s", err.Error())
	}
	r, err = db.Query("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"id":"1","name":"Who knows?"},{"id":"2","name":"aoife"}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	if err := db.Execute("DELETE FROM foo WHERE Id=2"); err != nil {
		t.Fatalf("failed to delete record: %s", err.Error())
	}
	r, err = db.Query("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"id":"1","name":"Who knows?"}]`, asJson(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

// func Test_FailingSimpleStatements(t *testing.T) {
// 	dir, err := ioutil.TempDir("", "rqlite-test-")
// 	defer os.RemoveAll(dir)
// 	db := New(path.Join(dir, "test_db"))
// 	defer db.Close()

// 	err = db.Execute("INSERT INTO foo(name) VALUES(\"fiona\")")
// 	c.Assert(err, NotNil)
// 	c.Assert(err.Error(), Equals, "no such table: foo")

// 	err = db.Execute("create table foo (id integer not null primary key, name text)")
// 	c.Assert(err, IsNil)
// 	err = db.Execute("create table foo (id integer not null primary key, name text)")
// 	c.Assert(err, NotNil)
// 	c.Assert(err.Error(), Equals, "table foo already exists")

// 	err = db.Execute("INSERT INTO foo(id, name) VALUES(11, \"fiona\")")
// 	c.Assert(err, IsNil)
// 	err = db.Execute("INSERT INTO foo(id, name) VALUES(11, \"fiona\")")
// 	c.Assert(err, NotNil)
// 	c.Assert(err.Error(), Equals, "UNIQUE constraint failed: foo.id")

// 	err = db.Execute("SELECT * FROM bar")
// 	c.Assert(err, NotNil)
// 	c.Assert(err.Error(), Equals, "no such table: bar")

// 	err = db.Execute("utter nonsense")
// 	c.Assert(err, NotNil)
// 	c.Assert(err.Error(), Equals, "near \"utter\": syntax error")
// }

// func Test_SimpleTransactions(t *testing.T) {
// 	dir, err := ioutil.TempDir("", "rqlite-test-")
// 	defer os.RemoveAll(dir)
// 	db := New(path.Join(dir, "test_db"))
// 	defer db.Close()

// 	err = db.Execute("create table foo (id integer not null primary key, name text)")
// 	c.Assert(err, IsNil)

// 	err = db.StartTransaction()
// 	c.Assert(err, IsNil)
// 	for i := 0; i < 10; i++ {
// 		_ = db.Execute("INSERT INTO foo(name) VALUES(\"philip\")")
// 	}
// 	err = db.CommitTransaction()
// 	c.Assert(err, IsNil)

// 	r, err := db.Query("SELECT name FROM foo")
// 	c.Assert(len(r), Equals, 10)
// 	for i := range r {
// 		c.Assert(r[i]["name"], Equals, "philip")
// 	}

// 	err = db.StartTransaction()
// 	c.Assert(err, IsNil)
// 	for i := 0; i < 10; i++ {
// 		_ = db.Execute("INSERT INTO foo(name) VALUES(\"philip\")")
// 	}
// 	err = db.RollbackTransaction()
// 	c.Assert(err, IsNil)

// 	r, err = db.Query("select name from foo") // Test lowercase
// 	c.Assert(len(r), Equals, 10)
// 	for i := range r {
// 		c.Assert(r[i]["name"], Equals, "philip")
// 	}
// }

// func Test_TransactionsConstraintViolation(t *testing.T) {
// 	dir, err := ioutil.TempDir("", "rqlite-test-")
// 	defer os.RemoveAll(dir)
// 	db := New(path.Join(dir, "test_db"))
// 	defer db.Close()

// 	err = db.Execute("create table foo (id integer not null primary key, name text)")
// 	c.Assert(err, IsNil)

// 	err = db.StartTransaction()
// 	c.Assert(err, IsNil)
// 	err = db.Execute("INSERT INTO foo(id, name) VALUES(1, \"fiona\")")
// 	c.Assert(err, IsNil)
// 	err = db.Execute("INSERT INTO foo(id, name) VALUES(1, \"fiona\")")
// 	c.Assert(err, NotNil)
// 	err = db.RollbackTransaction()
// 	c.Assert(err, IsNil)

// 	r, err := db.Query("SELECT * FROM foo")
// 	c.Assert(len(r), Equals, 0)
// }

// func Test_TransactionsHardFail(t *testing.T) {
// 	dir, err := ioutil.TempDir("", "rqlite-test-")
// 	defer os.RemoveAll(dir)
// 	db := New(path.Join(dir, "test_db"))

// 	err = db.Execute("create table foo (id integer not null primary key, name text)")
// 	c.Assert(err, IsNil)
// 	err = db.Execute("INSERT INTO foo(id, name) VALUES(1, \"fiona\")")
// 	c.Assert(err, IsNil)

// 	err = db.StartTransaction()
// 	c.Assert(err, IsNil)
// 	err = db.Execute("INSERT INTO foo(id, name) VALUES(2, \"dana\")")
// 	c.Assert(err, IsNil)
// 	db.Close()

// 	db = Open(path.Join(dir, "test_db"))
// 	c.Assert(db, NotNil)
// 	r, err := db.Query("SELECT * FROM foo")
// 	c.Assert(len(r), Equals, 1)
// 	c.Assert(r[0]["name"], Equals, "fiona")
// 	db.Close()
// }

func mustOpenDatabase() (*DB, string) {
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
