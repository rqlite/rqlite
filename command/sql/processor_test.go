package sql

import (
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
)

func Test_ContainsTime(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		expected bool
	}{
		// Test cases where a time-related function is present
		{
			name:     "Contains time function - time()",
			stmt:     "select time('now')",
			expected: true,
		},
		{
			name:     "Contains time function - date()",
			stmt:     "select date('now')",
			expected: true,
		},
		{
			name:     "Contains time function - datetime()",
			stmt:     "select datetime('now')",
			expected: true,
		},
		{
			name:     "Contains time function - julianday()",
			stmt:     "select julianday('now')",
			expected: true,
		},
		{
			name:     "Contains time function - unixepoch(",
			stmt:     "select unixepoch(",
			expected: true,
		},
		{
			name:     "Contains time function - timediff()",
			stmt:     "select timediff('2023-01-01', '2022-01-01')",
			expected: true,
		},
		{
			name:     "Contains strftime function - strftime()",
			stmt:     "select strftime('2023-01-01', '2022-01-01')",
			expected: true,
		},

		// The parenthesis need not follow the function name directly.
		{
			name:     "Contains time function - space before parenthesis",
			stmt:     "select datetime ('now')",
			expected: true,
		},
		{
			name:     "Contains time function - newline and tab before parenthesis",
			stmt:     "select julianday\n\t('now')",
			expected: true,
		},
		{
			name:     "Contains time function - block comment before parenthesis",
			stmt:     "select date /* c */ ('now')",
			expected: true,
		},
		{
			name:     "Contains time function - line comment before parenthesis",
			stmt:     "select time -- c\n('now')",
			expected: true,
		},
		{
			name:     "Contains time function - double-quoted name",
			stmt:     `select "datetime"('now')`,
			expected: true,
		},
		{
			name:     "Contains time function - bracket-quoted name",
			stmt:     "select [unixepoch] ()",
			expected: true,
		},
		{
			name:     "Contains time function - second occurrence is the call",
			stmt:     "update foo set d = date ()",
			expected: true,
		},

		// Test cases where no time-related function is present
		{
			name:     "No time function - unrelated function",
			stmt:     "select length('string')",
			expected: false,
		},
		{
			name:     "No time function - empty statement",
			stmt:     "",
			expected: false,
		},
		{
			name:     "No time function - similar but not exact match",
			stmt:     "select someotherfunction()",
			expected: false,
		},

		{
			name:     "No time function - update statement",
			stmt:     "update foo set bar = 1 where id = 2",
			expected: false,
		},
		{
			name:     "No time function - name followed by other text",
			stmt:     "select date from foo where (id = 1)",
			expected: false,
		},

		// Test cases where input may be unexpected
		{
			name:     "Edge case - case sensitivity",
			stmt:     "select Time('now')",
			expected: false, // Function expects input to be lower-case
		},
		{
			name:     "Edge case - substring match",
			stmt:     "select sometimedata from table", // Contains "time" as part of a word
			expected: false,
		},

		// The time keywords are literals, not function calls.
		{
			name:     "Contains time keyword - current_timestamp",
			stmt:     "insert into foo values (current_timestamp)",
			expected: true,
		},
		{
			name:     "Contains time keyword - current_time",
			stmt:     "select current_time",
			expected: true,
		},
		{
			name:     "Contains time keyword - current_date",
			stmt:     "select current_date",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ContainsTime(tt.stmt)
			if result != tt.expected {
				t.Errorf("ContainsTime(%q) = %v; want %v", tt.stmt, result, tt.expected)
			}
		})
	}
}

func Test_ContainsRandom(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		expected bool
	}{
		// Test cases where a random-related function is present
		{
			name:     "Contains random function - random()",
			stmt:     "select random()",
			expected: true,
		},
		{
			name:     "Contains random function - randomblob()",
			stmt:     "select randomblob(16)",
			expected: true,
		},

		// The parenthesis need not follow the function name directly.
		{
			name:     "Contains random function - space before parenthesis",
			stmt:     "select random ()",
			expected: true,
		},
		{
			name:     "Contains random function - newline before parenthesis",
			stmt:     "select randomblob\n(16)",
			expected: true,
		},
		{
			name:     "Contains random function - comment before parenthesis",
			stmt:     "select random /* c */ ()",
			expected: true,
		},
		{
			name:     "Contains random function - double-quoted name",
			stmt:     `select "random"()`,
			expected: true,
		},

		// Test cases where no random-related function is present
		{
			name:     "No random function - unrelated function",
			stmt:     "select length('string')",
			expected: false,
		},
		{
			name:     "No random function - empty statement",
			stmt:     "",
			expected: false,
		},
		{
			name:     "No random function - similar but not exact match",
			stmt:     "select some_random_function()",
			expected: false,
		},

		// Test cases where input may be unexpected
		{
			name:     "Edge case - case sensitivity",
			stmt:     "select Random()",
			expected: false, // Function expects input to be lower-case
		},
		{
			name:     "Edge case - substring match",
			stmt:     "select somerandomdata from table", // Contains "random" as part of a word
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ContainsRandom(tt.stmt)
			if result != tt.expected {
				t.Errorf("ContainsRandom(%q) = %v; want %v", tt.stmt, result, tt.expected)
			}
		})
	}
}

func Test_ContainsReturning(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		expected bool
	}{
		// Test cases where a RETURNING clause is present
		{
			name:     "Contains RETURNING clause - simple case",
			stmt:     "insert into table (col1) values (1) returning col1",
			expected: true,
		},
		{
			name:     "Contains RETURNING clause - middle of statement",
			stmt:     "update table set col1 = 2 returning col1, col2",
			expected: true,
		},
		{
			name:     "Contains RETURNING clause - at the end",
			stmt:     "delete from table returning *",
			expected: true,
		},

		// Test cases where no RETURNING clause is present
		{
			name:     "No RETURNING clause - unrelated statement",
			stmt:     "select * from table",
			expected: false,
		},
		{
			name:     "No RETURNING clause - empty statement",
			stmt:     "",
			expected: false,
		},
		{
			name:     "No RETURNING clause - substring but not clause",
			stmt:     "select something from table where returning_value = 1",
			expected: false,
		},

		// Test cases where input may be unexpected
		{
			name:     "Edge case - case sensitivity",
			stmt:     "insert into table Returning Col1",
			expected: false, // Function assumes lower-case input
		},
		{
			name:     "Edge case - no trailing space",
			stmt:     "some text containing returninglike",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ContainsReturning(tt.stmt)
			if result != tt.expected {
				t.Errorf("ContainsReturning(%q) = %v; want %v", tt.stmt, result, tt.expected)
			}
		})
	}
}

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
		`SELECT random ()`, `SELECT -?[0-9]+$`,
		"SELECT \"random\"\n()", `SELECT -?[0-9]+$`,
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
		`INSERT INTO "values" VALUES (datetime ('now'))`, `INSERT INTO "values" VALUES \(datetime\([0-9]+\.[0-9]+\)\)`,
		"INSERT INTO \"values\" VALUES (julianday\n\t('now'))", `INSERT INTO "values" VALUES \(julianday\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (date /* today */ ('now'))`, `INSERT INTO "values" VALUES \(date\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES ("unixepoch"('now'))`, `INSERT INTO "values" VALUES \(unixepoch\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (unixepoch('subsec'))`, `INSERT INTO "values" VALUES \(unixepoch\([0-9]+\.[0-9]+, 'subsec'\)\)`,
		`INSERT INTO "values" VALUES (datetime())`, `INSERT INTO "values" VALUES \(datetime\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (DATE(), time())`, `INSERT INTO "values" VALUES \(DATE\([0-9]+\.[0-9]+\), time\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (julianday(), unixepoch())`, `INSERT INTO "values" VALUES \(julianday\([0-9]+\.[0-9]+\), unixepoch\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (strftime('%s'))`, `INSERT INTO "values" VALUES \(strftime\('%s', [0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (CURRENT_TIMESTAMP)`, `INSERT INTO "values" VALUES \(datetime\([0-9]+\.[0-9]+\)\)`,
		`INSERT INTO "values" VALUES (current_date, CURRENT_TIME)`, `INSERT INTO "values" VALUES \(date\([0-9]+\.[0-9]+\), time\([0-9]+\.[0-9]+\)\)`,
		`CREATE TABLE tbl (col1 TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)`, `CREATE TABLE tbl \(col1 TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP\)`,
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
		`UPDATE t SET d = 'd1' WHERE d = '' RETURNING *`:                  true,
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

func Test_RETURNING_KeywordAsIdent(t *testing.T) {
	for sql, b := range map[string]bool{
		`UPDATE t SET desc = 'd1' WHERE desc = '' RETURNING *`: true, // SQLite keyword as column name
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

func Test_JulianDay(t *testing.T) {
	const exp = 2455146.458333333 // 2009-11-10 23:00:00 UTC
	const tol = 1e-8              // days; about 0.86 ms tolerance

	denver := time.FixedZone("UTC-7", -7*3600)
	tokyo := time.FixedZone("UTC+9", 9*3600)

	tests := []struct {
		name string
		t    time.Time
	}{
		{"UTC", time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)},
		{"UTC-7", time.Date(2009, time.November, 10, 16, 0, 0, 0, denver)},
		{"UTC+9", time.Date(2009, time.November, 11, 8, 0, 0, 0, tokyo)},
	}

	for _, tt := range tests {
		if got := JulianDay(tt.t); math.Abs(got-exp) > tol {
			t.Fatalf("%s: JulianDay is wrong, exp: %.10f, got: %.10f", tt.name, exp, got)
		}
	}
}
