package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/command"
	text "github.com/rqlite/rqlite/db/testdata"
)

// Test_TableCreationInMemory tests basic operation of an in-memory database,
// ensuring that using different connection objects (as the Execute and Query
// will do) works properly i.e. that the connections object work on the same
// in-memory database.
func Test_TableCreationInMemory(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	if !db.InMemory() {
		t.Fatal("in-memory database marked as not in-memory")
	}

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

// Test_ParallelOperationsInMemory runs multiple accesses concurrently, ensuring
// that correct results are returned in every goroutine. It's not 100% that this
// test would bring out a bug, but it's almost 100%.
//
// See https://github.com/mattn/go-sqlite3/issues/959#issuecomment-890283264
func Test_ParallelOperationsInMemory(t *testing.T) {
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
				t.Logf("failed to insert records into foo: %s", err.Error())
			}
		}
	}()
	go func() {
		defer exWg.Done()
		for range bar {
			if _, err := db.ExecuteStringStmt(`INSERT INTO bar(id, name) VALUES(1, "fiona")`); err != nil {
				t.Logf("failed to insert records into bar: %s", err.Error())
			}
		}
	}()
	go func() {
		defer exWg.Done()
		for range qux {
			if _, err := db.ExecuteStringStmt(`INSERT INTO qux(id, name) VALUES(1, "fiona")`); err != nil {
				t.Logf("failed to insert records into qux: %s", err.Error())
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
					t.Logf("failed to query for schema during goroutine %d execution: %s", j, err.Error())
				} else {
					n++
					if exp, got := `[{"columns":["sql"],"types":["text"],"values":[["CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"],["CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"],["CREATE TABLE qux (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"]]}]`, asJSON(rows); exp != got {
						t.Logf("schema not as expected during goroutine execution, exp %s, got %s, after %d queries", exp, got, n)
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

// Test_TableCreationLoadRawInMemory tests for https://sqlite.org/forum/forumpost/d443fb0730
func Test_TableCreationLoadRawInMemory(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	_, err := db.ExecuteStringStmt("CREATE TABLE logs (entry TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	done := make(chan struct{})
	defer close(done)

	// Insert some records continually, as fast as possible. Do it from a goroutine.
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				_, err := db.ExecuteStringStmt(`INSERT INTO logs(entry) VALUES("hello")`)
				if err != nil {
					return
				}
			}
		}
	}()

	// Get the count over and over again.
	for i := 0; i < 5000; i++ {
		rows, err := db.QueryStringStmt(`SELECT COUNT(*) FROM logs`)
		if err != nil {
			t.Fatalf("failed to query for count: %s", err)
		}

		if rows[0].Error != "" {
			t.Fatalf("rows had error after %d queries: %s", i, rows[0].Error)
		}
	}
}

// Test_1GiBInMemory tests that in-memory databases larger than 1GiB,
// but smaller than 2GiB, can be created without issue.
func Test_1GiBInMemory(t *testing.T) {
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
	if exp, got := `[{"last_insert_id":1715018,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("got incorrect response, exp: %s, got: %s", exp, got)
	}

	sz, err := db.Size()
	if err != nil {
		t.Fatalf("failed to get size: %s", err.Error())
	}
	if sz <= 1024*1024*1024 {
		t.Fatalf("failed to create a database greater than 1 GiB in size: %d", sz)
	}
}
