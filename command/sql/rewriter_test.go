package sql

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
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
		// A time function called without a time value uses the current time,
		// as reported in rqlite issue 3054.
		{
			in:          `INSERT INTO foo(col1) VALUES (time())`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (time(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (date())`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (date(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (datetime())`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (datetime(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (julianday())`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (julianday(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (unixepoch())`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (unixepoch(2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (strftime('%s'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (strftime('%s', 2440422.500000))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (datetime())`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (datetime())`,
			rewriteTime: false,
			modified:    false,
		},
		// The 'subsec' modifier in place of a time value also means the
		// current time.
		{
			in:          `INSERT INTO foo(col1) VALUES (unixepoch('subsec'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (unixepoch(2440422.500000, 'subsec'))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (datetime('SubSecond', '+1 day'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (datetime(2440422.500000, 'SubSecond', '+1 day'))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (strftime('%s', 'subsec'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (strftime('%s', 2440422.500000, 'subsec'))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (timediff('subsec', '2026-01-01'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (timediff(2440422.500000, '2026-01-01'))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (datetime('now', 'subsec'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (datetime(2440422.500000, 'subsec'))`,
			rewriteTime: true,
			modified:    true,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (datetime('2026-01-01', 'subsec'))`,
			exp:         `INSERT INTO "foo" ("col1") VALUES (datetime('2026-01-01', 'subsec'))`,
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

// Test_Rewriter_TimeParams checks that a parameter which supplies the time
// value to a time function, and which is bound to a value meaning the current
// time, is rewritten in the same way as the literal would be.
func Test_Rewriter_TimeParams(t *testing.T) {
	const jd = 2440422.5
	str := func(s string) *proto.Parameter {
		return &proto.Parameter{Value: &proto.Parameter_S{S: s}}
	}
	named := func(n, s string) *proto.Parameter {
		return &proto.Parameter{Value: &proto.Parameter_S{S: s}, Name: n}
	}
	num := func(i int64) *proto.Parameter {
		return &proto.Parameter{Value: &proto.Parameter_I{I: i}}
	}
	blob := func(s string) *proto.Parameter {
		return &proto.Parameter{Value: &proto.Parameter_Y{Y: []byte(s)}}
	}

	for i, tt := range []struct {
		in          string
		params      []*proto.Parameter
		exp         string
		expParams   []any // Expected parameter values, in order.
		noRewriteTm bool
	}{
		{
			// Every use is as a time value, so the bound value is changed.
			in:        `INSERT INTO foo VALUES (datetime(?))`,
			params:    []*proto.Parameter{str("now")},
			exp:       `INSERT INTO "foo" VALUES (datetime(?))`,
			expParams: []any{jd},
		},
		{
			in:        `INSERT INTO foo VALUES (datetime(?))`,
			params:    []*proto.Parameter{str("NOW")},
			exp:       `INSERT INTO "foo" VALUES (datetime(?))`,
			expParams: []any{jd},
		},
		{
			// A time function reads a blob as text.
			in:        `INSERT INTO foo VALUES (datetime(?))`,
			params:    []*proto.Parameter{blob("now")},
			exp:       `INSERT INTO "foo" VALUES (datetime(?))`,
			expParams: []any{jd},
		},
		{
			in:          `INSERT INTO foo VALUES (datetime(?))`,
			params:      []*proto.Parameter{str("now")},
			exp:         `INSERT INTO "foo" VALUES (datetime(?))`,
			expParams:   []any{"now"},
			noRewriteTm: true,
		},
		{
			// Only the parameter in the time value position is changed.
			in:        `INSERT INTO foo VALUES (?, datetime(?, ?), ?)`,
			params:    []*proto.Parameter{str("now"), str("now"), str("+1 day"), num(5)},
			exp:       `INSERT INTO "foo" VALUES (?, datetime(?, ?), ?)`,
			expParams: []any{"now", jd, "+1 day", int64(5)},
		},
		{
			// A parameter which is not 'now' is untouched.
			in:        `INSERT INTO foo VALUES (datetime(?), date(?))`,
			params:    []*proto.Parameter{str("2026-01-01"), num(5)},
			exp:       `INSERT INTO "foo" VALUES (datetime(?), date(?))`,
			expParams: []any{"2026-01-01", int64(5)},
		},
		{
			in:        `INSERT INTO foo VALUES (strftime(?, ?), timediff(?, ?))`,
			params:    []*proto.Parameter{str("now"), str("now"), str("now"), str("2026-01-01")},
			exp:       `INSERT INTO "foo" VALUES (strftime(?, ?), timediff(?, ?))`,
			expParams: []any{"now", jd, jd, "2026-01-01"},
		},
		{
			// Parameters are numbered in statement order, not in the order
			// the statement is walked.
			in:        `INSERT INTO foo VALUES (?, date(?)) ON CONFLICT DO UPDATE SET a = time(?) WHERE b = ?`,
			params:    []*proto.Parameter{num(1), str("now"), str("now"), str("now")},
			exp:       `INSERT INTO "foo" VALUES (?, date(?)) ON CONFLICT DO UPDATE SET "a" = time(?) WHERE "b" = ?`,
			expParams: []any{int64(1), jd, jd, "now"},
		},
		{
			in:        `INSERT INTO foo VALUES (?2, date(?1), time(?1))`,
			params:    []*proto.Parameter{str("now"), str("now")},
			exp:       `INSERT INTO "foo" VALUES (?2, date(?1), time(?1))`,
			expParams: []any{jd, "now"},
		},
		{
			in:        `INSERT INTO foo VALUES (date(:t), time(@t), datetime($u))`,
			params:    []*proto.Parameter{named("u", "2026-01-01"), named("t", "now")},
			exp:       `INSERT INTO "foo" VALUES (date(:t), time(@t), datetime($u))`,
			expParams: []any{"2026-01-01", jd},
		},
		{
			// A named parameter in the statement bound by position.
			in:        `INSERT INTO foo VALUES (:a, date(:t))`,
			params:    []*proto.Parameter{str("now"), str("now")},
			exp:       `INSERT INTO "foo" VALUES (:a, date(:t))`,
			expParams: []any{"now", jd},
		},
		{
			// The value is also used as it is, so it must not be changed.
			// Its use as a time value is replaced in the statement instead.
			in:        `INSERT INTO foo VALUES (:t, date(:t))`,
			params:    []*proto.Parameter{named("t", "now")},
			exp:       `INSERT INTO "foo" VALUES (:t, date(2440422.500000))`,
			expParams: []any{"now"},
		},
		{
			in:        `INSERT INTO foo VALUES (?1, date(?1), ?)`,
			params:    []*proto.Parameter{str("now"), num(5)},
			exp:       `INSERT INTO "foo" VALUES (?1, date(2440422.500000), ?)`,
			expParams: []any{"now", int64(5)},
		},
		{
			// Replacing :t would renumber the parameters after it, and so
			// change what the second value is bound to. Left alone.
			in:        `INSERT INTO foo VALUES (date(:t), ?, :t)`,
			params:    []*proto.Parameter{named("t", "now"), num(5)},
			exp:       `INSERT INTO "foo" VALUES (date(:t), ?, :t)`,
			expParams: []any{"now", int64(5)},
		},
		{
			// 'subsec' in place of a time value means the current time. The
			// modifier is kept, with the time value inserted ahead of it.
			in:        `INSERT INTO foo VALUES (unixepoch(?), strftime(?, ?))`,
			params:    []*proto.Parameter{str("subsec"), str("%s"), str("SubSecond")},
			exp:       `INSERT INTO "foo" VALUES (unixepoch(2440422.500000, ?), strftime(?, 2440422.500000, ?))`,
			expParams: []any{"subsec", "%s", "SubSecond"},
		},
		{
			in:        `INSERT INTO foo VALUES (timediff(?, ?))`,
			params:    []*proto.Parameter{str("2026-01-01"), str("subsec")},
			exp:       `INSERT INTO "foo" VALUES (timediff(?, ?))`,
			expParams: []any{"2026-01-01", jd},
		},
		{
			// 'subsec' as a modifier, after a time value, is untouched.
			in:        `INSERT INTO foo VALUES (datetime(?, ?))`,
			params:    []*proto.Parameter{str("2026-01-01"), str("subsec")},
			exp:       `INSERT INTO "foo" VALUES (datetime(?, ?))`,
			expParams: []any{"2026-01-01", "subsec"},
		},
	} {
		rw := NewRewriter()
		rw.RewriteTime = !tt.noRewriteTm
		rw.Params = tt.params
		rw.nowFn = func() time.Time {
			return time.Date(1969, 7, 20, 0, 0, 0, 0, time.UTC)
		}

		s, err := sql.NewParser(strings.NewReader(tt.in)).ParseStatement()
		if err != nil {
			t.Fatal(err)
		}
		s, _, _, err = rw.Do(s)
		if err != nil {
			t.Fatal(err)
		}
		if rendered := s.String(); rendered != tt.exp {
			t.Fatalf("test %d failed, exp: '%s', got: '%s'", i, tt.exp, rendered)
		}

		if len(tt.params) != len(tt.expParams) {
			t.Fatalf("test %d has wrong number of expected parameters", i)
		}
		for j, p := range tt.params {
			var got any
			switch v := p.GetValue().(type) {
			case *proto.Parameter_S:
				got = v.S
			case *proto.Parameter_Y:
				got = string(v.Y)
			case *proto.Parameter_D:
				got = v.D
			case *proto.Parameter_I:
				got = v.I
			}
			if got != tt.expParams[j] {
				t.Fatalf("test %d parameter %d wrong, exp: %v, got: %v", i, j, tt.expParams[j], got)
			}
		}
	}
}
