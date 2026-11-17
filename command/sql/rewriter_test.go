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

// Test_Rewriter_RandomBlobSize checks that RANDOMBLOB() is rewritten when its
// size is not a plain whole number, as reported in rqlite issue 3055.
func Test_Rewriter_RandomBlobSize(t *testing.T) {
	for i, tt := range []struct {
		in  string
		exp string // Regular expression.
	}{
		{in: `SELECT randomblob(8)`, exp: `^SELECT x'[0-9A-F]{16}'$`},
		{in: `SELECT randomblob(8.0)`, exp: `^SELECT x'[0-9A-F]{16}'$`},
		{in: `SELECT randomblob(8.9)`, exp: `^SELECT x'[0-9A-F]{16}'$`},
		{in: `SELECT randomblob(1e1)`, exp: `^SELECT x'[0-9A-F]{20}'$`},
		{in: `SELECT randomblob(0x10)`, exp: `^SELECT x'[0-9A-F]{32}'$`},
		{in: `SELECT randomblob(2*4)`, exp: `^SELECT x'[0-9A-F]{16}'$`},
		{in: `SELECT randomblob(2 * (3 + 1))`, exp: `^SELECT x'[0-9A-F]{16}'$`},
		{in: `SELECT randomblob(10 - 2)`, exp: `^SELECT x'[0-9A-F]{16}'$`},
		{in: `SELECT randomblob(7 / 2)`, exp: `^SELECT x'[0-9A-F]{6}'$`},
		{in: `SELECT randomblob(7 / 2.0)`, exp: `^SELECT x'[0-9A-F]{6}'$`},
		{in: `SELECT randomblob(7 % 4)`, exp: `^SELECT x'[0-9A-F]{6}'$`},
		{in: `SELECT randomblob(+8)`, exp: `^SELECT x'[0-9A-F]{16}'$`},
		// A size of less than 1, or of NULL, results in a single byte.
		{in: `SELECT randomblob(0)`, exp: `^SELECT x'[0-9A-F]{2}'$`},
		{in: `SELECT randomblob(-3)`, exp: `^SELECT x'[0-9A-F]{2}'$`},
		{in: `SELECT randomblob(2 - 5)`, exp: `^SELECT x'[0-9A-F]{2}'$`},
		{in: `SELECT randomblob(1 / 0)`, exp: `^SELECT x'[0-9A-F]{2}'$`},
		{in: `SELECT randomblob(1 % 0)`, exp: `^SELECT x'[0-9A-F]{2}'$`},
		// Too big for SQLite to create, so left for SQLite to reject.
		{in: `SELECT randomblob(1000000001)`, exp: `^SELECT randomblob\(1000000001\)$`},
		{in: `SELECT randomblob(1000000 * 1000000)`, exp: `^SELECT randomblob\(1000000 \* 1000000\)$`},
		{in: `SELECT randomblob(9223372036854775807 + 1)`, exp: `^SELECT randomblob\(9223372036854775807 \+ 1\)$`},
		// The size can't be known without running the statement.
		{in: `SELECT randomblob(length(name)) FROM foo`, exp: `^SELECT randomblob\(length\("name"\)\) FROM "foo"$`},
		{in: `SELECT randomblob(n * 2) FROM foo`, exp: `^SELECT randomblob\("n" \* 2\) FROM "foo"$`},
	} {
		rw := NewRewriter()
		s, err := sql.NewParser(strings.NewReader(tt.in)).ParseStatement()
		if err != nil {
			t.Fatal(err)
		}
		s, _, _, err = rw.Do(s)
		if err != nil {
			t.Fatal(err)
		}
		if rendered := s.String(); !regexp.MustCompile(tt.exp).MatchString(rendered) {
			t.Fatalf("test %d failed, exp: '%s', got: '%s'", i, tt.exp, rendered)
		}
	}
}

// Test_Rewriter_RandomBlobParams checks that RANDOMBLOB() is rewritten when
// its size is supplied by a parameter.
func Test_Rewriter_RandomBlobParams(t *testing.T) {
	num := func(i int64) *proto.Parameter {
		return &proto.Parameter{Value: &proto.Parameter_I{I: i}}
	}
	named := func(n string, i int64) *proto.Parameter {
		return &proto.Parameter{Value: &proto.Parameter_I{I: i}, Name: n}
	}
	str := func(s string) *proto.Parameter {
		return &proto.Parameter{Value: &proto.Parameter_S{S: s}}
	}
	// blob is an expected parameter value, random bytes of the given length.
	type blob int

	for i, tt := range []struct {
		in        string
		params    []*proto.Parameter
		exp       string // Regular expression.
		expParams []any  // Expected parameter values, in order.
		noRewrite bool
	}{
		{
			// The parameter takes the place of the call, and is bound to
			// the random bytes.
			in:        `INSERT INTO foo VALUES (randomblob(?))`,
			params:    []*proto.Parameter{num(8)},
			exp:       `^INSERT INTO "foo" VALUES \(\?\)$`,
			expParams: []any{blob(8)},
		},
		{
			in:        `INSERT INTO foo VALUES (randomblob(?))`,
			params:    []*proto.Parameter{num(8)},
			exp:       `^INSERT INTO "foo" VALUES \(randomblob\(\?\)\)$`,
			expParams: []any{int64(8)},
			noRewrite: true,
		},
		{
			// Other parameters stay bound to the same values.
			in:        `INSERT INTO foo VALUES (?, hex(randomblob(?)), ?)`,
			params:    []*proto.Parameter{num(8), num(4), str("x")},
			exp:       `^INSERT INTO "foo" VALUES \(\?, hex\(\?\), \?\)$`,
			expParams: []any{int64(8), blob(4), "x"},
		},
		{
			// The size is read the way SQLite would read it.
			in: `INSERT INTO foo VALUES (randomblob(?), randomblob(?), randomblob(?), randomblob(?), randomblob(?), randomblob(?))`,
			params: []*proto.Parameter{
				{Value: &proto.Parameter_D{D: 8.9}},
				str(" 7abc"),
				str("abc"),
				{Value: &proto.Parameter_B{B: true}},
				{},
				num(-5),
			},
			exp:       `^INSERT INTO "foo" VALUES \(\?, \?, \?, \?, \?, \?\)$`,
			expParams: []any{blob(8), blob(7), blob(1), blob(1), blob(1), blob(1)},
		},
		{
			// Parameters are numbered in statement order, not in the order
			// the statement is walked.
			in:        `INSERT INTO foo VALUES (?, ?) ON CONFLICT DO UPDATE SET a = randomblob(?) WHERE b = ?`,
			params:    []*proto.Parameter{num(1), num(2), num(3), num(4)},
			exp:       `SET "a" = \? WHERE "b" = \?$`,
			expParams: []any{int64(1), int64(2), blob(3), int64(4)},
		},
		{
			in:        `INSERT INTO foo VALUES (randomblob(:n), $m)`,
			params:    []*proto.Parameter{named("m", 3), named("n", 8)},
			exp:       `^INSERT INTO "foo" VALUES \(:n, \$m\)$`,
			expParams: []any{int64(3), blob(8)},
		},
		{
			// A named parameter in the statement bound by position.
			in:        `INSERT INTO foo VALUES (:a, randomblob(:n))`,
			params:    []*proto.Parameter{num(3), num(8)},
			exp:       `^INSERT INTO "foo" VALUES \(:a, :n\)$`,
			expParams: []any{int64(3), blob(8)},
		},
		{
			// The size is also used as it is, so it must not be changed.
			// The call is replaced in the statement instead.
			in:        `INSERT INTO foo VALUES (:n, randomblob(:n))`,
			params:    []*proto.Parameter{named("n", 8)},
			exp:       `^INSERT INTO "foo" VALUES \(:n, x'[0-9A-F]{16}'\)$`,
			expParams: []any{int64(8)},
		},
		{
			in:        `INSERT INTO foo VALUES (?1, randomblob(?1), ?)`,
			params:    []*proto.Parameter{num(8), num(5)},
			exp:       `^INSERT INTO "foo" VALUES \(\?1, x'[0-9A-F]{16}', \?\)$`,
			expParams: []any{int64(8), int64(5)},
		},
		{
			// Each call gets its own bytes. The parameter must remain in
			// the statement, so it takes the place of the last call.
			in:        `INSERT INTO foo VALUES (randomblob(?1), randomblob(?1))`,
			params:    []*proto.Parameter{num(8)},
			exp:       `^INSERT INTO "foo" VALUES \(x'[0-9A-F]{16}', \?1\)$`,
			expParams: []any{blob(8)},
		},
		{
			// Replacing the call would renumber the parameters after it,
			// and so change what the second value is bound to. Left alone.
			in:        `INSERT INTO foo VALUES (randomblob(:n), ?, :n)`,
			params:    []*proto.Parameter{named("n", 8), num(5)},
			exp:       `^INSERT INTO "foo" VALUES \(randomblob\(:n\), \?, :n\)$`,
			expParams: []any{int64(8), int64(5)},
		},
		{
			// No value is bound to the parameter.
			in:        `INSERT INTO foo VALUES (randomblob(?))`,
			params:    nil,
			exp:       `^INSERT INTO "foo" VALUES \(randomblob\(\?\)\)$`,
			expParams: []any{},
		},
		{
			// Too big for SQLite to create, so left for SQLite to reject.
			in:        `INSERT INTO foo VALUES (randomblob(?))`,
			params:    []*proto.Parameter{num(1000000001)},
			exp:       `^INSERT INTO "foo" VALUES \(randomblob\(\?\)\)$`,
			expParams: []any{int64(1000000001)},
		},
		{
			// A parameter as part of an expression is not supported.
			in:        `INSERT INTO foo VALUES (randomblob(? * 2))`,
			params:    []*proto.Parameter{num(4)},
			exp:       `^INSERT INTO "foo" VALUES \(randomblob\(\? \* 2\)\)$`,
			expParams: []any{int64(4)},
		},
		{
			// Not rewritten in an ORDER BY, as with RANDOM().
			in:        `SELECT * FROM foo ORDER BY randomblob(?)`,
			params:    []*proto.Parameter{num(4)},
			exp:       `ORDER BY randomblob\(\?\)$`,
			expParams: []any{int64(4)},
		},
	} {
		rw := NewRewriter()
		rw.RewriteRand = !tt.noRewrite
		rw.Params = tt.params

		s, err := sql.NewParser(strings.NewReader(tt.in)).ParseStatement()
		if err != nil {
			t.Fatal(err)
		}
		s, _, _, err = rw.Do(s)
		if err != nil {
			t.Fatal(err)
		}
		if rendered := s.String(); !regexp.MustCompile(tt.exp).MatchString(rendered) {
			t.Fatalf("test %d failed, exp: '%s', got: '%s'", i, tt.exp, rendered)
		}

		if len(tt.params) != len(tt.expParams) {
			t.Fatalf("test %d has wrong number of expected parameters", i)
		}
		for j, p := range tt.params {
			if n, ok := tt.expParams[j].(blob); ok {
				y, ok := p.GetValue().(*proto.Parameter_Y)
				if !ok {
					t.Fatalf("test %d parameter %d is not a blob, got: %v", i, j, p.GetValue())
				}
				if len(y.Y) != int(n) {
					t.Fatalf("test %d parameter %d wrong length, exp: %d, got: %d", i, j, n, len(y.Y))
				}
				continue
			}
			var got any
			switch v := p.GetValue().(type) {
			case *proto.Parameter_S:
				got = v.S
			case *proto.Parameter_I:
				got = v.I
			}
			if got != tt.expParams[j] {
				t.Fatalf("test %d parameter %d wrong, exp: %v, got: %v", i, j, tt.expParams[j], got)
			}
		}
	}
}
