package db

import (
	"context"
	"os"
	"testing"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

func Test_QualifyColumns_Join(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer os.Remove(path)
	defer db.Close()

	_, err := db.Execute(&command.Request{
		Statements: []*command.Statement{
			{Sql: `CREATE TABLE contacts (id INTEGER PRIMARY KEY, name TEXT)`},
			{Sql: `CREATE TABLE titles (id INTEGER PRIMARY KEY, contact_id INTEGER, title TEXT)`},
			{Sql: `INSERT INTO contacts(id, name) VALUES(1, "alice")`},
			{Sql: `INSERT INTO titles(id, contact_id, title) VALUES(1, 1, "engineer")`},
		},
	}, false)
	if err != nil {
		t.Fatalf("failed to setup tables: %s", err.Error())
	}

	// Query without qualify flag — columns should be unqualified
	req := &command.Request{
		Statements: []*command.Statement{
			{Sql: `SELECT * FROM contacts JOIN titles ON contacts.id = titles.contact_id`},
		},
	}
	rows, err := db.Query(req, false)
	if err != nil {
		t.Fatalf("failed to query: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name","id","contact_id","title"],"types":["integer","text","integer","integer","text"],"values":[[1,"alice",1,1,"engineer"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected unqualified results\nexp: %s\ngot: %s", exp, got)
	}

	// Query with qualify flag via context — columns should be table-qualified
	ctx := NewContextWithQualifyColumns(context.Background())
	rows, err = db.QueryWithContext(ctx, req, false)
	if err != nil {
		t.Fatalf("failed to query with qualify: %s", err.Error())
	}
	if exp, got := `[{"columns":["contacts.id","contacts.name","titles.id","titles.contact_id","titles.title"],"types":["integer","text","integer","integer","text"],"values":[[1,"alice",1,1,"engineer"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected qualified results\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_QualifyColumns_SingleTable(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer os.Remove(path)
	defer db.Close()

	_, err := db.Execute(&command.Request{
		Statements: []*command.Statement{
			{Sql: `CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)`},
			{Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`},
		},
	}, false)
	if err != nil {
		t.Fatalf("failed to setup table: %s", err.Error())
	}

	ctx := NewContextWithQualifyColumns(context.Background())
	rows, err := db.QueryWithContext(ctx, &command.Request{
		Statements: []*command.Statement{
			{Sql: `SELECT * FROM foo`},
		},
	}, false)
	if err != nil {
		t.Fatalf("failed to query with qualify: %s", err.Error())
	}
	if exp, got := `[{"columns":["foo.id","foo.name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected qualified results\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_QualifyColumns_ExplicitColumns(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer os.Remove(path)
	defer db.Close()

	_, err := db.Execute(&command.Request{
		Statements: []*command.Statement{
			{Sql: `CREATE TABLE a (id INTEGER PRIMARY KEY, x TEXT)`},
			{Sql: `CREATE TABLE b (id INTEGER PRIMARY KEY, y TEXT)`},
			{Sql: `INSERT INTO a(id, x) VALUES(1, "hello")`},
			{Sql: `INSERT INTO b(id, y) VALUES(1, "world")`},
		},
	}, false)
	if err != nil {
		t.Fatalf("failed to setup tables: %s", err.Error())
	}

	ctx := NewContextWithQualifyColumns(context.Background())
	rows, err := db.QueryWithContext(ctx, &command.Request{
		Statements: []*command.Statement{
			{Sql: `SELECT a.id, b.y FROM a JOIN b ON a.id = b.id`},
		},
	}, false)
	if err != nil {
		t.Fatalf("failed to query: %s", err.Error())
	}
	if exp, got := `[{"columns":["a.id","b.y"],"types":["integer","text"],"values":[[1,"world"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected qualified results\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_QualifyColumns_LeftJoin(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer os.Remove(path)
	defer db.Close()

	_, err := db.Execute(&command.Request{
		Statements: []*command.Statement{
			{Sql: `CREATE TABLE p (id INTEGER PRIMARY KEY, name TEXT)`},
			{Sql: `CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER, val TEXT)`},
			{Sql: `INSERT INTO p(id, name) VALUES(1, "parent")`},
		},
	}, false)
	if err != nil {
		t.Fatalf("failed to setup tables: %s", err.Error())
	}

	ctx := NewContextWithQualifyColumns(context.Background())
	rows, err := db.QueryWithContext(ctx, &command.Request{
		Statements: []*command.Statement{
			{Sql: `SELECT * FROM p LEFT JOIN c ON p.id = c.pid`},
		},
	}, false)
	if err != nil {
		t.Fatalf("failed to query: %s", err.Error())
	}
	if exp, got := `[{"columns":["p.id","p.name","c.id","c.pid","c.val"],"types":["integer","text","integer","integer","text"],"values":[[1,"parent",null,null,null]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected qualified results\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_QualifyColumns_Request(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer os.Remove(path)
	defer db.Close()

	_, err := db.Execute(&command.Request{
		Statements: []*command.Statement{
			{Sql: `CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)`},
			{Sql: `CREATE TABLE t2 (id INTEGER PRIMARY KEY, t1_id INTEGER, label TEXT)`},
			{Sql: `INSERT INTO t1(id, name) VALUES(1, "row1")`},
			{Sql: `INSERT INTO t2(id, t1_id, label) VALUES(1, 1, "lbl")`},
		},
	}, false)
	if err != nil {
		t.Fatalf("failed to setup tables: %s", err.Error())
	}

	ctx := NewContextWithQualifyColumns(context.Background())
	results, err := db.RequestWithContext(ctx, &command.Request{
		Statements: []*command.Statement{
			{Sql: `SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id`},
		},
	}, false)
	if err != nil {
		t.Fatalf("failed to request: %s", err.Error())
	}
	if exp, got := `[{"columns":["t1.id","t1.name","t2.id","t2.t1_id","t2.label"],"types":["integer","text","integer","integer","text"],"values":[[1,"row1",1,1,"lbl"]]}]`, asJSON(results); exp != got {
		t.Fatalf("unexpected qualified results\nexp: %s\ngot: %s", exp, got)
	}
}
