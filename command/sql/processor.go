package sql

import (
	"strings"

	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/sql"
)

// Process processes the given SQL statements, rewriting them if necessary. If
// random-rewriting is enabled, calls to the RANDOM() function are replaced with
// an actual random value. If a statement contains a RETURNING clause, the
// statement is marked as a query, so that the result set can be returned to the
// client.
func Process(stmts []*proto.Statement, rw bool) error {
	for i := range stmts {
		if !rw && !containsReturning(stmts[i]) {
			// random-rewriting is disabled, and the statement can't contain a
			// RETURNING clause, so there's nothing to do.
			continue
		}
		stmt, err := sql.NewParser(strings.NewReader(stmts[i].Sql)).ParseStatement()
		if err != nil {
			continue
		}
		rewriter := &Rewriter{
			RewriteRand: rw,
		}
		rwStmt, rewritten, ret, err := rewriter.Do(stmt)
		if err != nil {
			continue
		}

		if rewritten {
			stmts[i].Sql = rwStmt.String()
		}
		stmts[i].ForceQuery = ret
	}
	return nil
}

func containsReturning(stmt *proto.Statement) bool {
	return strings.Contains(strings.ToLower(stmt.Sql), "returning")
}

// Rewriter rewrites SQL statements to replace calls to the RANDOM() function
// with an actual random value, if requested. It also checks for the presence of
// a RETURNING clause.
type Rewriter struct {
	RewriteRand bool

	randRewritten bool
	returning     bool
}

// Do rewrites the given statement, if necessary. It returns the rewritten
// statement, a flag indicating whether the statement was rewritten, a flag
// indicating when a RETURNING clause was found, and an error.
func (rw *Rewriter) Do(stmt sql.Statement) (sql.Statement, bool, bool, error) {
	err := sql.Walk(rw, stmt)
	if err != nil {
		return nil, false, false, err
	}
	return stmt, rw.randRewritten, rw.returning, nil
}

// Visit implements the sql.Visitor interface.
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

// VisitEnd implements the sql.Visitor interface.
func (rw *Rewriter) VisitEnd(node sql.Node) error {
	return nil
}
