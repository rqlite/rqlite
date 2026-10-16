package sql

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/sql"
)

func Test_Rewriter_Random(t *testing.T) {
	for i, tt := range []struct {
		in          string
		exp         string
		rewriteRand bool
		modified    bool
	}{
		{
			in:          `INSERT INTO foo(col1) VALUES (random())`,
			exp:         `INSERT INTO "foo" \("col1"\) VALUES \(random\(\)\)`,
			rewriteRand: false,
			modified:    false,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (random())`,
			exp:         `INSERT INTO \"foo\" \("col1"\) VALUES \(-?[0-9]+\)`,
			rewriteRand: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678')`,
			exp:         `INSERT INTO "names" VALUES \(-?[0-9]+, 'bob', '123-45-678'\)`,
			rewriteRand: true,
			modified:    true,
		},
		{
			in:          `SELECT * FROM foo ORDER BY random()`,
			exp:         `SELECT \* FROM "foo" ORDER BY random\(\)`,
			rewriteRand: true,
			modified:    false,
		},
		{
			in:          `SELECT random()`,
			exp:         `SELECT -?[0-9]+`,
			rewriteRand: true,
			modified:    true,
		},
		{
			in:          `SELECT abs(random())`,
			exp:         `SELECT abs\(random\(\)\)`,
			rewriteRand: false,
			modified:    false,
		},
		{
			in:          `SELECT abs(random())`,
			exp:         `SELECT abs\(-?[0-9]+\)`,
			rewriteRand: true,
			modified:    true,
		},
		{
			in:          `SELECT random() + 1`,
			exp:         `SELECT -?[0-9]+ \+ 1`,
			rewriteRand: true,
			modified:    true,
		},
		{
			in:          `SELECT "v" FROM "foo"`,
			exp:         `SELECT "v" FROM "foo"`,
			rewriteRand: true,
			modified:    false,
		},
	} {
		rw := NewRewriter()
		rw.RewriteRand = tt.rewriteRand

		s, err := sql.NewParser(strings.NewReader(tt.in)).ParseStatement()
		if err != nil {
			t.Fatal(err)
		}

		s, f, _, err := rw.Do(s)
		if err != nil {
			t.Fatal(err)
		}

		match := regexp.MustCompile(tt.exp)
		rendered := s.String()
		if !match.MatchString(rendered) {
			t.Fatalf("test %d failed, exp: '%s', got: '%s'", i, tt.exp, rendered)
		}

		if tt.modified != f {
			t.Fatalf("test %d modified wrong, exp %t, got %t", i, tt.modified, f)
		}
	}
}

func Test_Rewriter_Time(t *testing.T) {
	for i, tt := range []struct {
		in          string
		exp         string
		rewriteTime bool
		modified    bool
	}{
		{
			in:          `INSERT INTO foo(col1) VALUES (time('now'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (time('now'))`,
			rewriteTime: false,
			modified:    false,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (time('now'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (time(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (date('now'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (date(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (datetime('now'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (datetime(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (julianday('now'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (julianday(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (unixepoch('now'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (unixepoch(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (strftime('abc', 'now'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (strftime('abc', 2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (timediff('abc', 'now'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (timediff('abc', 2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (timediff('now', 'now'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (timediff(2440422.500000, 2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		// CURRENT_TIMESTAMP, CURRENT_DATE and CURRENT_TIME are equivalent to
		// datetime('now'), date('now') and time('now').
		{
			in:          `INSERT INTO foo(col1) VALUES (CURRENT_TIMESTAMP)`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (CURRENT_TIMESTAMP)`,
			rewriteTime: false,
			modified:    false,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (CURRENT_TIMESTAMP)`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (datetime(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (current_date)`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (date(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (Current_Time)`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (time(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			// The statement from rqlite issue 2965.
			in:          `INSERT INTO timestamp_probe VALUES (1, CURRENT_TIMESTAMP, strftime('%Y-%m-%d %H:%M:%f', 'now'))`,
			exp:         `INSERT INTO "timestamp_probe" VALUES (1, datetime(2440422.500000), strftime('%Y-%m-%d %H:%M:%f', 2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `UPDATE foo SET ts = CURRENT_TIMESTAMP WHERE ts < CURRENT_DATE`,
			exp:         `UPDATE "foo" SET "ts" = datetime(2440422.500000) WHERE "ts" < date(2440422.500000)`,
			rewriteTime: true,
			modified:    true,
		},
		{
			// A column default is schema, evaluated by SQLite on each insert.
			in:          `CREATE TABLE foo (ts DATETIME DEFAULT CURRENT_TIMESTAMP)`,
			exp:         `CREATE TABLE "foo" ("ts" DATETIME DEFAULT CURRENT_TIMESTAMP)`,
			rewriteTime: true,
			modified:    false,
		},
	} {
		rw := NewRewriter()
		rw.RewriteTime = tt.rewriteTime
		rw.nowFn = func() time.Time {
			return time.Date(1969, 7, 20, 0, 0, 0, 0, time.UTC)
		}

		s, err := sql.NewParser(strings.NewReader(tt.in)).ParseStatement()
		if err != nil {
			t.Fatal(err)
		}

		s, f, _, err := rw.Do(s)
		if err != nil {
			t.Fatal(err)
		}

		rendered := s.String()
		if rendered != tt.exp {
			t.Fatalf("test %d failed, exp: '%s', got: '%s'", i, tt.exp, rendered)
		}

		if tt.modified != f {
			t.Fatalf("test %d modified wrong, exp %t, got %t", i, tt.modified, f)
		}
	}
}

// Test_Rewriter_SchemaUntouched checks that expressions stored in the schema
// and evaluated later by SQLite -- column defaults, CHECK constraints,
// generated columns, indexes, triggers and views -- are never rewritten.
// Freezing time('now') or random() in a column default would make every
// future row carry the same value.
func Test_Rewriter_SchemaUntouched(t *testing.T) {
	for i, tt := range []struct {
		in  string
		exp string
	}{
		{
			in:  `CREATE TABLE foo (ts DEFAULT (datetime('now')))`,
			exp: `CREATE TABLE "foo" ("ts" DEFAULT (datetime('now')))`,
		},
		{
			in:  `CREATE TABLE foo (id DEFAULT (random()))`,
			exp: `CREATE TABLE "foo" ("id" DEFAULT (random()))`,
		},
		{
			in:  `CREATE TABLE foo (ts, CHECK (ts < datetime('now')))`,
			exp: `CREATE TABLE "foo" ("ts", CHECK ("ts" < datetime('now')))`,
		},
		{
			in:  `CREATE TABLE foo (ts, d GENERATED ALWAYS AS (datetime('now')) VIRTUAL)`,
			exp: `CREATE TABLE "foo" ("ts", "d" GENERATED ALWAYS AS (datetime('now')) VIRTUAL)`,
		},
		{
			in:  `ALTER TABLE foo ADD COLUMN ts DEFAULT (datetime('now'))`,
			exp: `ALTER TABLE "foo" ADD COLUMN "ts" DEFAULT (datetime('now'))`,
		},
		{
			in:  `CREATE INDEX idx ON foo (ts) WHERE ts > datetime('now')`,
			exp: `CREATE INDEX "idx" ON "foo" ("ts") WHERE "ts" > datetime('now')`,
		},
		{
			in:  `CREATE TRIGGER tr AFTER INSERT ON foo BEGIN UPDATE foo SET ts = datetime('now'); END`,
			exp: `CREATE TRIGGER "tr" AFTER INSERT ON "foo" BEGIN UPDATE "foo" SET "ts" = datetime('now'); END`,
		},
		{
			in:  `CREATE VIEW v AS SELECT datetime('now') AS ts FROM foo`,
			exp: `CREATE VIEW "v" AS SELECT datetime('now') AS "ts" FROM "foo"`,
		},
		{
			// CREATE TABLE ... AS SELECT evaluates once, so it is rewritten.
			in:  `CREATE TABLE bar AS SELECT datetime('now') AS ts FROM foo`,
			exp: `CREATE TABLE "bar" AS SELECT datetime(2440422.500000) AS "ts" FROM "foo"`,
		},
	} {
		rw := NewRewriter()
		rw.nowFn = func() time.Time {
			return time.Date(1969, 7, 20, 0, 0, 0, 0, time.UTC)
		}

		s, err := sql.NewParser(strings.NewReader(tt.in)).ParseStatement()
		if err != nil {
			t.Fatalf("test %d: %s", i, err)
		}

		s, f, _, err := rw.Do(s)
		if err != nil {
			t.Fatal(err)
		}

		rendered := s.String()
		if rendered != tt.exp {
			t.Fatalf("test %d failed, exp: '%s', got: '%s'", i, tt.exp, rendered)
		}
		wantModified := strings.Contains(tt.exp, "2440422")
		if f != wantModified {
			t.Fatalf("test %d modified wrong, exp %t, got %t", i, wantModified, f)
		}
	}
}
