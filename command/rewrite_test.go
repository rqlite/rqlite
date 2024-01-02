package command

import (
	"regexp"
	"testing"

	"github.com/rqlite/rqlite/v8/proto/command"
)

func Test_NoRewrites(t *testing.T) {
	for _, str := range []string{
		`INSERT INTO "names" VALUES (1, 'bob', '123-45-678')`,
		`INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678')`,
		`SELECT title FROM albums ORDER BY RANDOM()`,
		`INSERT INTO foo(name, age) VALUES(?, ?)`,
	} {

		stmts := []*command.Statement{
			{
				Sql: str,
			},
		}
		if err := Rewrite(stmts, false); err != nil {
			t.Fatalf("failed to not rewrite: %s", err)
		}
		if stmts[0].Sql != str {
			t.Fatalf("SQL is modified: %s", stmts[0].Sql)
		}
	}
}

func Test_NoRewritesMulti(t *testing.T) {
	stmts := []*command.Statement{
		{
			Sql: `INSERT INTO "names" VALUES (1, 'bob', '123-45-678')`,
		},
		{
			Sql: `INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678')`,
		},
		{
			Sql: `SELECT title FROM albums ORDER BY RANDOM()`,
		},
	}
	if err := Rewrite(stmts, false); err != nil {
		t.Fatalf("failed to not rewrite: %s", err)
	}
	if len(stmts) != 3 {
		t.Fatalf("returned stmts is wrong length: %d", len(stmts))
	}
	if stmts[0].Sql != `INSERT INTO "names" VALUES (1, 'bob', '123-45-678')` {
		t.Fatalf("SQL is modified: %s", stmts[0].Sql)
	}
	if stmts[1].Sql != `INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678')` {
		t.Fatalf("SQL is modified: %s", stmts[0].Sql)
	}
	if stmts[2].Sql != `SELECT title FROM albums ORDER BY RANDOM()` {
		t.Fatalf("SQL is modified: %s", stmts[0].Sql)
	}
}

func Test_Rewrites(t *testing.T) {
	testSQLs := []string{
		`INSERT INTO "names" VALUES (1, 'bob', '123-45-678')`, `INSERT INTO "names" VALUES \(1, 'bob', '123-45-678'\)`,
		`INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678')`, `INSERT INTO "names" VALUES \(-?[0-9]+, 'bob', '123-45-678'\)`,
		`SELECT title FROM albums ORDER BY RANDOM()`, `SELECT title FROM albums ORDER BY RANDOM\(\)`,
		`SELECT RANDOM()`, `SELECT -?[0-9]+`,
		`CREATE TABLE tbl (col1 TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)`, `CREATE TABLE tbl \(col1 TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP\)`,
	}
	for i := 0; i < len(testSQLs)-1; i += 2 {
		stmts := []*command.Statement{
			{
				Sql: testSQLs[i],
			},
		}
		if err := Rewrite(stmts, true); err != nil {
			t.Fatalf("failed to not rewrite: %s", err)
		}

		match := regexp.MustCompile(testSQLs[i+1])
		if !match.MatchString(stmts[0].Sql) {
			t.Fatalf("test %d failed, %s (rewritten as %s) does not regex-match with %s", i, testSQLs[i], stmts[0].Sql, testSQLs[i+1])
		}
	}
}
