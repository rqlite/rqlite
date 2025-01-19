package sql

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/random"
	"github.com/rqlite/sql"
)

// Process processes the given SQL statements, rewriting them if necessary. If
// random-rewriting is enabled, calls to the RANDOM() function are replaced with
// an actual random value. If a statement contains a RETURNING clause, the
// statement is marked as a query, so that the result set can be returned to the
// client.
func Process(stmts []*proto.Statement, rwrand, rwtime bool) error {
	for i := range stmts {
		lowered := strings.ToLower(stmts[i].Sql)
		if (!rwtime || !containsTime(lowered)) &&
			(!rwrand || !containsRandom(lowered)) &&
			!containsReturning(lowered) {
			continue
		}
		parsed, err := sql.NewParser(strings.NewReader(stmts[i].Sql)).ParseStatement()
		if err != nil {
			continue
		}
		rewriter := NewRewriter()
		rewriter.RewriteRand = rwrand
		rewriter.RewriteTime = rwtime
		rwStmt, rewritten, ret, err := rewriter.Do(parsed)
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

func containsTime(stmt string) bool {
	targets := []string{"time(", "date(", "julianday(", "unixepoch(", "timediff("}
	for _, target := range targets {
		if strings.Contains(stmt, target) {
			return true
		}
	}
	return false
}

func containsRandom(stmt string) bool {
	targets := []string{"random(", "randomblob("}
	for _, target := range targets {
		if strings.Contains(stmt, target) {
			return true
		}
	}
	return false
}

func containsReturning(stmt string) bool {
	return strings.Contains(stmt, "returning")
}

// Rewriter rewrites SQL statements.
type Rewriter struct {
	RewriteRand bool
	RewriteTime bool

	randFn func() int64
	nowFn  func() time.Time

	orderedBy bool
	modified  bool
	returning bool
}

// NewRewriter returns a new Rewriter. This object is not thread
// safe, and should not be shared between goroutines.
func NewRewriter() *Rewriter {
	return &Rewriter{
		RewriteRand: true,
		RewriteTime: true,

		randFn: func() int64 {
			return rand.Int63()
		},
		nowFn: time.Now,
	}
}

// Do rewrites the provided statement. If the statement is rewritten, the second return value is true.
func (rw *Rewriter) Do(stmt sql.Statement) (sql.Statement, bool, bool, error) {
	rw.modified = false
	node, err := sql.Walk(rw, stmt)
	if err != nil {
		return nil, false, false, err
	}
	return node.(sql.Statement), rw.modified, rw.returning, nil
}

func (rw *Rewriter) Visit(node sql.Node) (w sql.Visitor, n sql.Node, err error) {
	retNode := node

	switch n := retNode.(type) {
	case *sql.ReturningClause:
		rw.returning = true
	case *sql.OrderingTerm:
		// NO random() rewriting past this point.
		rw.orderedBy = true
		return rw, node, nil
	case *sql.Call:
		// If used, ensure the value is same for the duration of the statement
		jd := julianDayAsNumberLit(rw.nowFn())

		if rw.RewriteTime && len(n.Args) > 0 &&
			(strings.EqualFold(n.Name.Name, "date") ||
				strings.EqualFold(n.Name.Name, "time") ||
				strings.EqualFold(n.Name.Name, "datetime") ||
				strings.EqualFold(n.Name.Name, "julianday") ||
				strings.EqualFold(n.Name.Name, "unixepoch")) {
			if isNow(n.Args[0]) {
				n.Args[0] = jd
			}
			rw.modified = true
		} else if rw.RewriteTime && len(n.Args) > 1 &&
			strings.EqualFold(n.Name.Name, "strftime") {
			if isNow(n.Args[1]) {
				n.Args[1] = jd
			}
			rw.modified = true
		} else if rw.RewriteTime && len(n.Args) > 1 &&
			strings.EqualFold(n.Name.Name, "timediff") {
			if isNow(n.Args[0]) {
				n.Args[0] = jd
			}
			if isNow(n.Args[1]) {
				n.Args[1] = jd
			}
			rw.modified = true
		} else if !rw.orderedBy && rw.RewriteRand && strings.EqualFold(n.Name.Name, "random") {
			retNode = &sql.NumberLit{Value: strconv.Itoa(int(rw.randFn()))}
			rw.modified = true
		} else if !rw.orderedBy && rw.RewriteRand && strings.EqualFold(n.Name.Name, "randomblob") {
			if len(n.Args) == 1 {
				lit, ok := n.Args[0].(*sql.NumberLit)
				if !ok {
					break
				}
				n, err := strconv.Atoi(lit.Value)
				if err != nil {
					break
				}
				retNode = &sql.BlobLit{Value: fmt.Sprintf(`%X`, random.Bytes(max(n, 1)))}
				rw.modified = true
			}
		}
	}
	return rw, retNode, nil
}

func (rw *Rewriter) VisitEnd(node sql.Node) (sql.Node, error) {
	switch node.(type) {
	case *sql.OrderingTerm:
		rw.orderedBy = false
	}
	return node, nil
}

func isNow(e sql.Expr) bool {
	if i, ok := e.(*sql.Ident); ok {
		return strings.EqualFold(i.Name, "now")
	} else if s, ok := e.(*sql.StringLit); ok {
		return strings.EqualFold(s.Value, "now")
	}
	return false
}

func julianDayAsNumberLit(t time.Time) *sql.NumberLit {
	return &sql.NumberLit{Value: fmt.Sprintf("%f", julianDay(t))}
}

func julianDay(t time.Time) float64 {
	year := t.Year()
	month := int(t.Month())
	day := t.Day()
	hour := t.Hour()
	minute := t.Minute()
	second := t.Second()
	nanosecond := t.Nanosecond()

	// Adjust for months January and February
	if month <= 2 {
		year--
		month += 12
	}

	// Calculate the Julian Day Number
	A := year / 100
	B := 2 - A + A/4

	// Convert time to fractional day
	fractionalDay := (float64(hour) +
		float64(minute)/60 +
		(float64(second)+float64(nanosecond)/1e9)/3600) / 24.0

	// Use math.Floor to correctly handle the integer parts
	jd := math.Floor(365.25*float64(year+4716)) +
		math.Floor(30.6001*float64(month+1)) +
		float64(day) + float64(B) - 1524.5 + fractionalDay

	return jd
}
