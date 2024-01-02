package command

import (
	"strings"

	"github.com/rqlite/rqlite/v8/proto/command"
	"github.com/rqlite/sql"
)

// Rewrite rewrites the statements such that RANDOM is rewritten,
// if r is true.
func Rewrite(stmts []*command.Statement, r bool) error {
	if !r {
		return nil
	}

	rw := &sql.Rewriter{
		RewriteRand: r,
	}

	for i := range stmts {
		// Only replace the incoming statement with a rewritten version if
		// there was no error, or if the rewriter did anything. If the statement
		// is bad SQLite syntax, let SQLite deal with it -- and let its error
		// be returned. Those errors will probably be clearer.
		s, err := sql.NewParser(strings.NewReader(stmts[i].Sql)).ParseStatement()
		if err != nil {
			continue
		}
		s, f, err := rw.Do(s)
		if err != nil || !f {
			continue
		}

		stmts[i].Sql = s.String()
	}
	return nil
}
