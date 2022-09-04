package command

import (
	"strings"

	"github.com/rqlite/sql"
)

// Rewrite rewrites the statements such that RANDOM is rewritten,
// if r is true.
func Rewrite(stmts []*Statement, r bool) error {
	if !r {
		return nil
	}

	rw := &sql.Rewriter{
		RewriteRand: r,
	}

	for i := range stmts {
		s, err := sql.NewParser(strings.NewReader(stmts[i].Sql)).ParseStatement()
		if err != nil {
			return err
		}

		s, f, err := rw.Do(s)

		// Only replace the incoming statement if there was no error or if the
		// rewriter did anything. If the statement is bad SQLite syntax, let
		// SQLite deal with it -- and get back its error. Those errors will
		// probably be clearer.
		if err == nil && f {
			stmts[i].Sql = s.String()
		}
	}
	return nil
}
