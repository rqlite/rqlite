package sql_test

import (
	"strings"
	"testing"

	"github.com/benbjohnson/lux/sql"
)

func TestScanner_Scan(t *testing.T) {
	t.Run("IDENT", func(t *testing.T) {
		t.Run("Unquoted", func(t *testing.T) {
			AssertScan(t, `foo_BAR123`, sql.IDENT, `foo_BAR123`)
		})
		t.Run("Quoted", func(t *testing.T) {
			AssertScan(t, `"crazy ~!#*&# column name"" foo"`, sql.QIDENT, `crazy ~!#*&# column name" foo`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertScan(t, `"unfinished`, sql.ILLEGAL, `"unfinished`)
		})
		t.Run("x", func(t *testing.T) {
			AssertScan(t, `x`, sql.IDENT, `x`)
		})
		t.Run("StartingX", func(t *testing.T) {
			AssertScan(t, `xyz`, sql.IDENT, `xyz`)
		})
	})

	t.Run("KEYWORD", func(t *testing.T) {
		AssertScan(t, `BEGIN`, sql.BEGIN, `BEGIN`)
	})

	t.Run("STRING", func(t *testing.T) {
		t.Run("OK", func(t *testing.T) {
			AssertScan(t, `'this is ''a'' string'`, sql.STRING, `this is 'a' string`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertScan(t, `'unfinished`, sql.ILLEGAL, `'unfinished`)
		})
	})
	t.Run("BLOB", func(t *testing.T) {
		t.Run("LowerX", func(t *testing.T) {
			AssertScan(t, `x'0123456789abcdef'`, sql.BLOB, `0123456789abcdef`)
		})
		t.Run("UpperX", func(t *testing.T) {
			AssertScan(t, `X'0123456789ABCDEF'`, sql.BLOB, `0123456789ABCDEF`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertScan(t, `x'0123`, sql.ILLEGAL, `x'0123`)
		})
		t.Run("BadHex", func(t *testing.T) {
			AssertScan(t, `x'hello`, sql.ILLEGAL, `x'h`)
		})
	})

	t.Run("INTEGER", func(t *testing.T) {
		AssertScan(t, `123`, sql.INTEGER, `123`)
	})

	t.Run("FLOAT", func(t *testing.T) {
		AssertScan(t, `123.456`, sql.FLOAT, `123.456`)
		AssertScan(t, `.1`, sql.FLOAT, `.1`)
		AssertScan(t, `123e456`, sql.FLOAT, `123e456`)
		AssertScan(t, `123E456`, sql.FLOAT, `123E456`)
		AssertScan(t, `123.456E78`, sql.FLOAT, `123.456E78`)
		AssertScan(t, `123.E45`, sql.FLOAT, `123.E45`)
		AssertScan(t, `123E+4`, sql.FLOAT, `123E+4`)
		AssertScan(t, `123E-4`, sql.FLOAT, `123E-4`)
		AssertScan(t, `123E`, sql.ILLEGAL, `123E`)
		AssertScan(t, `123E+`, sql.ILLEGAL, `123E+`)
		AssertScan(t, `123E-`, sql.ILLEGAL, `123E-`)
	})
	t.Run("BIND", func(t *testing.T) {
		AssertScan(t, `?'`, sql.BIND, `?`)
		AssertScan(t, `?123'`, sql.BIND, `?123`)
		AssertScan(t, `:foo_bar123'`, sql.BIND, `:foo_bar123`)
		AssertScan(t, `@bar'`, sql.BIND, `@bar`)
		AssertScan(t, `$baz'`, sql.BIND, `$baz`)
	})

	t.Run("EOF", func(t *testing.T) {
		AssertScan(t, " \n\t\r", sql.EOF, ``)
	})

	t.Run("SEMI", func(t *testing.T) {
		AssertScan(t, ";", sql.SEMI, ";")
	})
	t.Run("LP", func(t *testing.T) {
		AssertScan(t, "(", sql.LP, "(")
	})
	t.Run("RP", func(t *testing.T) {
		AssertScan(t, ")", sql.RP, ")")
	})
	t.Run("COMMA", func(t *testing.T) {
		AssertScan(t, ",", sql.COMMA, ",")
	})
	t.Run("NE", func(t *testing.T) {
		AssertScan(t, "!=", sql.NE, "!=")
	})
	t.Run("BITNOT", func(t *testing.T) {
		AssertScan(t, "!", sql.BITNOT, "!")
	})
	t.Run("EQ", func(t *testing.T) {
		AssertScan(t, "=", sql.EQ, "=")
	})
	t.Run("LE", func(t *testing.T) {
		AssertScan(t, "<=", sql.LE, "<=")
	})
	t.Run("LSHIFT", func(t *testing.T) {
		AssertScan(t, "<<", sql.LSHIFT, "<<")
	})
	t.Run("LT", func(t *testing.T) {
		AssertScan(t, "<", sql.LT, "<")
	})
	t.Run("GE", func(t *testing.T) {
		AssertScan(t, ">=", sql.GE, ">=")
	})
	t.Run("RSHIFT", func(t *testing.T) {
		AssertScan(t, ">>", sql.RSHIFT, ">>")
	})
	t.Run("GT", func(t *testing.T) {
		AssertScan(t, ">", sql.GT, ">")
	})
	t.Run("BITAND", func(t *testing.T) {
		AssertScan(t, "&", sql.BITAND, "&")
	})
	t.Run("CONCAT", func(t *testing.T) {
		AssertScan(t, "||", sql.CONCAT, "||")
	})
	t.Run("BITOR", func(t *testing.T) {
		AssertScan(t, "|", sql.BITOR, "|")
	})
	t.Run("PLUS", func(t *testing.T) {
		AssertScan(t, "+", sql.PLUS, "+")
	})
	t.Run("MINUS", func(t *testing.T) {
		AssertScan(t, "-", sql.MINUS, "-")
	})
	t.Run("STAR", func(t *testing.T) {
		AssertScan(t, "*", sql.STAR, "*")
	})
	t.Run("SLASH", func(t *testing.T) {
		AssertScan(t, "/", sql.SLASH, "/")
	})
	t.Run("REM", func(t *testing.T) {
		AssertScan(t, "%", sql.REM, "%")
	})
	t.Run("DOT", func(t *testing.T) {
		AssertScan(t, ".", sql.DOT, ".")
	})
	t.Run("ILLEGAL", func(t *testing.T) {
		AssertScan(t, "^", sql.ILLEGAL, "^")
	})
}

// AssertScan asserts the value of the first scan to s.
func AssertScan(tb testing.TB, s string, expectedTok sql.Token, expectedLit string) {
	tb.Helper()
	_, tok, lit := sql.NewScanner(strings.NewReader(s)).Scan()
	if tok != expectedTok || lit != expectedLit {
		tb.Fatalf("Scan(%q)=<%s,%s>, want <%s,%s>", s, tok, lit, expectedTok, expectedLit)
	}
}
