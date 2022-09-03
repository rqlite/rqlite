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
		if err != nil {
			return err
		}
		if f {
			// Only replace the incoming statement if the rewriter did anything.
			stmts[i].Sql = s.String()
		}
	}
	return nil
}
