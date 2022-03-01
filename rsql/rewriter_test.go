package rsql

import (
	"testing"
)

func Test_NewRewriter(t *testing.T) {
	rw := NewRewriter(false, false, nil)
	if rw == nil {
		t.Fatalf("failed to create Rewriter")
	}
}

func Test_RewriterDisabled(t *testing.T) {
	rw := NewRewriter(false, false, nil)

	for _, s := range []string{
		`SELECT * FROM foo`,
		`SELECT * FROM foo ORDER BY random()`,
		`SELECT date('now')`,
		`SELECT * FROM foo WHERE timestamp < datetime('now')`,
		`INSERT INTO foo (col1) VALUES(random())`,
	} {
		stmt, err := rw.Do(s)
		if err != nil {
			t.Fatalf("failed to rewrite %s: %s", s, err.Error())
		}
		if stmt != s {
			t.Fatalf("statement was changed unexpectedly, exp %s, got %s", s, stmt)
		}
	}
}

func Test_RewriterSimpleErrors(t *testing.T) {
	rw := NewRewriter(false, false, nil)
	_, err := rw.Do("SEC  ")
	if err != nil {
		t.Fatalf("parsing of bad statement succeeded")
	}
}

func Test_RewriterRandom(t *testing.T) {
	f := func() uint64 {
		return 1234
	}
	rw := NewRewriter(false, true, f)

	for _, tt := range []struct {
		in  string
		out string
	}{
		{
			in:  `INSERT INTO "foo" ("col1") VALUES(random())`,
			out: `INSERT INTO "foo" ("col1") VALUES(1234)`,
		},
	} {
		stmt, err := rw.Do(tt.in)
		if err != nil {
			t.Fatalf("parsing of bad statement succeeded")
		}
		if stmt != tt.out {
			t.Fatalf("random() not rewritten, exp %s, got %s", tt.out, stmt)
		}
	}
}
