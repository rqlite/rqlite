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
