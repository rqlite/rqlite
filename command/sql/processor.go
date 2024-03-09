package sql

import (
	"strings"

	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/sql"
)

func Process(stmts []*proto.Statement, r bool) error {
	rw := &Rewriter{
		RewriteRand: r,
	}

	for i := range stmts {
		if !r && !containsReturning(stmts[i]) {
			// random-rewriting is disabled, and the statement can't contain a
			// RETURNING clause, so there's nothing to do.
			continue
		}
		s, err := sql.NewParser(strings.NewReader(stmts[i].Sql)).ParseStatement()
		if err != nil {
			continue
		}
		s, f, r, err := rw.Do(s)
		if err != nil {
			continue
		}

		if f {
			stmts[i].Sql = s.String()
		}
		stmts[i].Returning = r
	}
	return nil
}

func containsReturning(stmt *proto.Statement) bool {
	return strings.Contains(strings.ToLower(stmt.Sql), "returning")
}

type Rewriter struct {
	RewriteRand bool

	randRewritten bool
	returning     bool
}

func (rw *Rewriter) Do(stmt sql.Statement) (sql.Statement, bool, bool, error) {
	err := sql.Walk(rw, stmt)
	if err != nil {
		return nil, false, false, err
	}
	return stmt, rw.randRewritten, rw.returning, nil
}

func (rw *Rewriter) Visit(node sql.Node) (w sql.Visitor, err error) {
	switch n := node.(type) {
	case *sql.ReturningClause:
		rw.returning = true
	case *sql.OrderingTerm:
		// Don't rewrite any further down this branch, as ordering by RANDOM
		// should be left to SQLite itself.
		return nil, nil
	case *sql.Call:
		if rw.RewriteRand {
			rw.randRewritten =
				rw.randRewritten || strings.ToUpper(n.Name.Name) == "RANDOM"
			n.Eval = rw.randRewritten
		}
		return rw, nil
	}
	return rw, nil
}

func (rw *Rewriter) VisitEnd(node sql.Node) error {
	return nil
}
