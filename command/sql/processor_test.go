package sql

import (
	"regexp"
	"testing"

	"github.com/rqlite/rqlite/v8/command/proto"
)

func Test_RANDOM_NoRewrites(t *testing.T) {
	for _, str := range []string{
		`INSERT INTO "names" VALUES (1, 'bob', '123-45-678')`,
		`INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678')`,
		`SELECT title FROM albums ORDER BY RANDOM()`,
		`INSERT INTO foo(name, age) VALUES(?, ?)`,
	} {

		stmts := []*proto.Statement{
			{
				Sql: str,
			},
		}
		if err := Process(stmts, false, false); err != nil {
			t.Fatalf("failed to not rewrite: %s", err)
		}
		if stmts[0].Sql != str {
			t.Fatalf("SQL is modified: %s", stmts[0].Sql)
		}
	}
}

func Test_RANDOM_NoRewritesMulti(t *testing.T) {
	stmts := []*proto.Statement{
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
	if err := Process(stmts, false, false); err != nil {
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

func Test_RANDOM_Rewrites(t *testing.T) {
	testSQLs := []string{
		`INSERT INTO "names" VALUES (1, 'ann', '123-45-678')`, `INSERT INTO "names" VALUES \(1, 'ann', '123-45-678'\)`,
		`INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678')`, `INSERT INTO "names" VALUES \(-?[0-9]+, 'bob', '123-45-678'\)`,
		`SELECT title FROM albums ORDER BY RANDOM()`, `SELECT title FROM albums ORDER BY RANDOM\(\)`,
		`SELECT random()`, `SELECT -?[0-9]+`,
		`CREATE TABLE tbl (col1 TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)`, `CREATE TABLE tbl \(col1 TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP\)`,
	}
	for i := 0; i < len(testSQLs)-1; i += 2 {
		stmts := []*proto.Statement{
			{
				Sql: testSQLs[i],
			},
		}
		if err := Process(stmts, true, false); err != nil {
			t.Fatalf("failed to not rewrite: %s", err)
		}

		match := regexp.MustCompile(testSQLs[i+1])
		if !match.MatchString(stmts[0].Sql) {
			t.Fatalf("test %d failed, %s (rewritten as %s) does not regex-match with %s", i, testSQLs[i], stmts[0].Sql, testSQLs[i+1])
		}
	}
}

func Test_RANDOMBLOB_Rewrites(t *testing.T) {
	testSQLs := []string{
		`INSERT INTO "names" VALUES (randomblob(0))`, `INSERT INTO "names" VALUES \(x'[0-9A-F]{2}'\)`,
		`INSERT INTO "names" VALUES (randomblob(4))`, `INSERT INTO "names" VALUES \(x'[0-9A-F]{8}'\)`,
		`INSERT INTO "names" VALUES (randomblob(16))`, `INSERT INTO "names" VALUES \(x'[0-9A-F]{32}'\)`,
		`INSERT INTO "names" VALUES (RANDOMBLOB(16))`, `INSERT INTO "names" VALUES \(x'[0-9A-F]{32}'\)`,
		`INSERT INTO "names" VALUES (RANDOMBLOB(16))`, `INSERT INTO "names" VALUES \(x'[0-9A-F]{32}'\)`,
		`INSERT INTO "names" VALUES (hex(RANDOMBLOB(16)))`, `INSERT INTO "names" VALUES \(hex\(x'[0-9A-F]{32}'\)\)`,
		`INSERT INTO "names" VALUES (lower(hex(RANDOMBLOB(16))))`, `INSERT INTO "names" VALUES \(lower\(hex\(x'[0-9A-F]{32}'\)\)\)`,
	}
	for i := 0; i < len(testSQLs)-1; i += 2 {
		stmts := []*proto.Statement{
			{
				Sql: testSQLs[i],
			},
		}
		if err := Process(stmts, true, false); err != nil {
			t.Fatalf("failed to not rewrite: %s", err)
		}

		match := regexp.MustCompile(testSQLs[i+1])
		if !match.MatchString(stmts[0].Sql) {
			t.Fatalf("test %d failed, %s (rewritten as %s ) does not regex-match with %s", i, testSQLs[i], stmts[0].Sql, testSQLs[i+1])
		}
	}
}

func Test_Time_Rewrites(t *testing.T) {
	testSQLs := []string{
		`SELECT date('now','start of month')`, `SELECT date\([0-9]+\.[0-9]+, 'start of month'\)`,
		`INSERT INTO "values" VALUES (time("2020-07-01 14:23"))`, `INSERT INTO "values" VALUES \(time\("2020-07-01 14:23"\)\)`,
		`INSERT INTO "values" VALUES (time('now'))`, `INSERT INTO "values" VALUES \(time\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (TIME("now"))`, `INSERT INTO "values" VALUES \(TIME\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (datetime("now"))`, `INSERT INTO "values" VALUES \(datetime\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (DATE("now"))`, `INSERT INTO "values" VALUES \(DATE\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (julianday("now"))`, `INSERT INTO "values" VALUES \(julianday\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (unixepoch("now"))`, `INSERT INTO "values" VALUES \(unixepoch\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (strftime("%F", "now"))`, `INSERT INTO "values" VALUES \(strftime\("%F", [0-9]+\.[0-9]+\)\)`,
	}
	for i := 0; i < len(testSQLs)-1; i += 2 {
		stmts := []*proto.Statement{
			{
				Sql: testSQLs[i],
			},
		}
		if err := Process(stmts, false, true); err != nil {
			t.Fatalf("failed to not rewrite: %s", err)
		}

		match := regexp.MustCompile(testSQLs[i+1])
		if !match.MatchString(stmts[0].Sql) {
			t.Fatalf("test %d failed, %s (rewritten as %s ) does not match", i, testSQLs[i], stmts[0].Sql)
		}
	}
}

func Test_RETURNING_None(t *testing.T) {
	for _, str := range []string{
		`INSERT INTO "names" VALUES (1, 'bob', '123-45-678')`,
		`INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678')`,
		`SELECT title FROM albums ORDER BY RANDOM()`,
		`INSERT INTO foo(name, age) VALUES(?, ?)`,
	} {

		stmts := []*proto.Statement{
			{
				Sql: str,
			},
		}
		if err := Process(stmts, false, false); err != nil {
			t.Fatalf("failed to not rewrite: %s", err)
		}
		if stmts[0].ForceQuery {
			t.Fatalf("ForceQuery is set")
		}
	}
}

func Test_RETURNING_Some(t *testing.T) {
	for sql, b := range map[string]bool{
		`INSERT INTO "names" VALUES (1, 'bob', '123-45-678') RETURNING *`: true,
		`INSERT INTO "names" VALUES (1, 'bob', 'RETURNING')`:              false,
		`INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678')`:      false,
		`SELECT title FROM albums ORDER BY RANDOM()`:                      false,
		`INSERT INTO foo(name, age) VALUES(?, ?)`:                         false,
	} {

		stmts := []*proto.Statement{
			{
				Sql: sql,
			},
		}
		if err := Process(stmts, false, false); err != nil {
			t.Fatalf("failed to not rewrite: %s", err)
		}
		if exp, got := b, stmts[0].ForceQuery; exp != got {
			t.Fatalf(`expected %v for SQL "%s", but got %v`, exp, sql, got)
		}
	}
}

func Test_RETURNING_SomeMulti(t *testing.T) {
	stmts := []*proto.Statement{
		{
			Sql: `INSERT INTO "names" VALUES (1, 'bob', '123-45-678') RETURNING *`,
		},
		{
			Sql: `INSERT INTO "names" VALUES (1, 'bob', 'RETURNING')`,
		},
	}
	if err := Process(stmts, false, false); err != nil {
		t.Fatalf("failed to not rewrite: %s", err)
	}

	if exp, got := true, stmts[0].ForceQuery; exp != got {
		t.Fatalf(`expected %v for SQL "%s", but got %v`, exp, stmts[0].Sql, got)
	}
	if exp, got := false, stmts[1].ForceQuery; exp != got {
		t.Fatalf(`expected %v for SQL "%s", but got %v`, exp, stmts[1].Sql, got)
	}
}

func Test_Both(t *testing.T) {
	stmt := &proto.Statement{
		Sql: `INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678') RETURNING *`,
	}

	if err := Process([]*proto.Statement{stmt}, true, false); err != nil {
		t.Fatalf("failed to not rewrite: %s", err)
	}
	match := regexp.MustCompile(`INSERT INTO "names" VALUES \(-?[0-9]+, 'bob', '123-45-678'\)`)
	if !match.MatchString(stmt.Sql) {
		t.Fatalf("SQL is not rewritten: %s", stmt.Sql)
	}
	if !stmt.ForceQuery {
		t.Fatalf("ForceQuery is not set")
	}
}

func Test_Complex(t *testing.T) {
	sql := `
	SELECT
    datetime('now', '+' || CAST(ABS(RANDOM() % 1000) AS TEXT) || ' seconds') AS random_future_time,
    date('now', '-' || CAST((RANDOM() % 30) AS TEXT) || ' days') AS random_past_date,
    time('now', '+' || CAST((RANDOM() % 3600) AS TEXT) || ' seconds') AS random_future_time_of_day,
    julianday('now') - julianday(date('now', '-' || CAST((RANDOM() % 365) AS TEXT) || ' days')) AS random_days_ago,
    strftime('%Y-%m-%d %H:%M:%f', 'now', '+' || CAST(RANDOM() % 10000 AS TEXT) || ' seconds') AS precise_future_timestamp,
    RANDOM() % 100 AS random_integer,
    ROUND(RANDOM() / 1000000000.0, 2) AS random_decimal,
    CASE
        WHEN RANDOM() % 2 = 0 THEN 'Even'
        ELSE 'Odd'
    END AS random_parity
	`
	stmt := &proto.Statement{
		Sql: sql,
	}

	if err := Process([]*proto.Statement{stmt}, true, true); err != nil {
		t.Fatalf("failed to rewrite complex statement: %s", err)
	}
}
