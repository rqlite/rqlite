package sql

import (
	"expvar"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/random"
	"github.com/rqlite/sql"
)

const (
	numRewrittenStmts = "num_rewritten_stmts"
	numParserPanics   = "num_parser_panics"
)

// stats captures stats for the SQL processor.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("sql.processor")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numRewrittenStmts, 0)
	stats.Add(numParserPanics, 0)
}

// Process processes the given SQL statements, rewriting them if necessary. If
// random-rewriting is enabled, calls to the RANDOM() function are replaced with
// an actual random value. If time-rewriting is enabled, 'now' arguments to the
// time functions are replaced with the current Julian day, as are time values
// which are omitted and so default to the current time, and the keywords
// CURRENT_TIMESTAMP, CURRENT_DATE and CURRENT_TIME are replaced with the
// equivalent datetime(), date() and time() calls on that Julian day. If a statement
// contains a RETURNING clause, the statement is marked as a query, so that the
// result set can be returned to the client. If a statement is an EXPLAIN, it is
// marked as such.
func Process(stmts []*proto.Statement, rwrand, rwtime bool) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			stats.Add(numParserPanics, 1)
			retErr = fmt.Errorf("panic during SQL processing: %v", r)
		}
	}()
	for i := range stmts {
		lowered := strings.ToLower(stmts[i].Sql)
		if (!rwtime || !ContainsTime(lowered)) &&
			(!rwrand || !ContainsRandom(lowered)) &&
			!ContainsReturning(lowered) &&
			!ContainsExplain(lowered) {
			continue
		}
		parsed, err := sql.NewParser(strings.NewReader(stmts[i].Sql)).ParseStatement()
		if err != nil {
			continue
		}
		_, stmts[i].SqlExplain = parsed.(*sql.ExplainStatement)
		rewriter := NewRewriter()
		rewriter.RewriteRand = rwrand
		rewriter.RewriteTime = rwtime
		rwStmt, rewritten, ret, err := rewriter.Do(parsed)
		if err != nil {
			continue
		}

		if rewritten {
			stats.Add(numRewrittenStmts, 1)
			stmts[i].Sql = rwStmt.String()
		}
		stmts[i].ForceQuery = ret
	}
	return nil
}

// ContainsTime returns true if the statement may contain a call to a
// time-related function, or one of the CURRENT_TIMESTAMP, CURRENT_DATE and
// CURRENT_TIME keywords. It is a cheap check used to decide if a statement
// needs to be parsed, so it may return true for a statement which turns out
// to contain neither. The function performs a lower-case comparison so it is
// up to the caller to ensure the statement is lower-cased.
func ContainsTime(stmt string) bool {
	// Since this is a simple substring search, time and date also match
	// datetime and strftime.
	for _, name := range []string{"time", "date", "julianday", "unixepoch", "timediff"} {
		if containsCall(stmt, name) {
			return true
		}
	}
	return strings.Contains(stmt, "current_time") || strings.Contains(stmt, "current_date")
}

// ContainsRandom returns true if the statement may contain a call to a
// random-related function. It is a cheap check used to decide if a statement
// needs to be parsed, so it may return true for a statement which turns out
// not to contain one. The function performs a lower-case comparison so it is
// up to the caller to ensure the statement is lower-cased.
func ContainsRandom(stmt string) bool {
	// Since this is a simple substring search, random also matches randomblob.
	return containsCall(stmt, "random") || containsCall(stmt, "randomblob")
}

// containsCall returns true if name appears in the statement followed by an
// opening parenthesis. SQL allows whitespace and comments between a function
// name and the parenthesis, and allows the name to be quoted, so all of those
// count as a match.
func containsCall(stmt, name string) bool {
	for {
		i := strings.Index(stmt, name)
		if i < 0 {
			return false
		}
		stmt = stmt[i+len(name):]
		rest := strings.TrimLeft(stmt, " \t\r\n\f\"`]")
		if strings.HasPrefix(rest, "(") ||
			strings.HasPrefix(rest, "/*") ||
			strings.HasPrefix(rest, "--") {
			return true
		}
	}
}

// ContainsReturning returns true if the statement contains a RETURNING clause.
// The function performs a lower-case comparison so it is up to the caller to
// ensure the statement is lower-cased.
func ContainsReturning(stmt string) bool {
	return strings.Contains(stmt, "returning ")
}

// ContainsExplain returns true if the statement contains an EXPLAIN clause.
// The function performs a lower-case comparison so it is up to the caller to
// ensure the statement is lower-cased.
func ContainsExplain(stmt string) bool {
	return strings.Contains(stmt, "explain ")
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

		randFn: rand.Int64,
		nowFn:  time.Now,
	}
}

// Do rewrites the provided statement. The second return value is true if the
// statement was rewritten, and the third is true if it contains a RETURNING
// clause.
func (rw *Rewriter) Do(stmt sql.Statement) (sql.Statement, bool, bool, error) {
	rw.modified = false
	node, err := sql.Walk(rw, stmt)
	if err != nil {
		return nil, false, false, err
	}
	return node.(sql.Statement), rw.modified, rw.returning, nil
}

// Visit implements sql.Visitor. It is called for each node in the statement
// before its children are visited, and may replace the node.
func (rw *Rewriter) Visit(node sql.Node) (w sql.Visitor, n sql.Node, err error) {
	retNode := node

	// Capture it once per call, to match SQLite.
	now := rw.nowFn()

	switch retNode.(type) {
	case *sql.DefaultConstraint, *sql.CheckConstraint, *sql.GeneratedConstraint,
		*sql.CreateIndexStatement, *sql.CreateTriggerStatement, *sql.CreateViewStatement:
		// These expressions are stored in the schema and evaluated by SQLite
		// every time they are used, so they must be left exactly as written.
		// Rewriting time('now') in a column default would freeze it to the
		// moment the table was created. Returning a nil Visitor stops the
		// walk descending into this subtree.
		return nil, node, nil
	}

	switch n := retNode.(type) {
	case *sql.ReturningClause:
		rw.returning = true
	case *sql.OrderingTerm:
		// NO random() rewriting past this point.
		rw.orderedBy = true
		return rw, node, nil
	case *sql.TimestampLit:
		// CURRENT_TIMESTAMP, CURRENT_DATE and CURRENT_TIME are shorthand for
		// datetime('now'), date('now') and time('now'), and are rewritten the
		// same way: to the matching function applied to a fixed Julian day.
		if rw.RewriteTime {
			var fn string
			switch strings.ToLower(n.Value) {
			case "current_timestamp":
				fn = "datetime"
			case "current_date":
				fn = "date"
			case "current_time":
				fn = "time"
			}
			if fn != "" {
				retNode = &sql.Call{
					Name: &sql.Ident{Name: fn},
					Args: []sql.Expr{julianDayAsNumberLit(now)},
				}
				rw.modified = true
			}
		}
	case *sql.Call:
		// If used, ensure the value is same for the duration of the statement
		jd := julianDayAsNumberLit(now)

		if rw.RewriteTime && isTimeFunc(n.Name.Name) {
			// The time value is the first argument. If it is absent SQLite
			// uses the current time, exactly as if 'now' had been passed.
			// The same is true if the first argument is the 'subsec'
			// modifier, which is then kept so it applies to the new value.
			if len(n.Args) == 0 {
				n.Args = []sql.Expr{jd}
			} else if isNow(n.Args[0]) {
				n.Args[0] = jd
			} else if isSubsec(n.Args[0]) {
				n.Args = append([]sql.Expr{jd}, n.Args...)
			}
			rw.modified = true
		} else if rw.RewriteTime && len(n.Args) > 0 &&
			strings.EqualFold(n.Name.Name, "strftime") {
			// The time value follows the format, and is also optional.
			if len(n.Args) == 1 {
				n.Args = append(n.Args, jd)
			} else if isNow(n.Args[1]) {
				n.Args[1] = jd
			} else if isSubsec(n.Args[1]) {
				n.Args = append([]sql.Expr{n.Args[0], jd}, n.Args[1:]...)
			}
			rw.modified = true
		} else if rw.RewriteTime && len(n.Args) > 1 &&
			strings.EqualFold(n.Name.Name, "timediff") {
			// timediff takes no modifiers, and always returns fractional
			// seconds, so 'subsec' here simply means the current time.
			if isNow(n.Args[0]) || isSubsec(n.Args[0]) {
				n.Args[0] = jd
			}
			if isNow(n.Args[1]) || isSubsec(n.Args[1]) {
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

// VisitEnd implements sql.Visitor. It is called for each node after its
// children have been visited.
func (rw *Rewriter) VisitEnd(node sql.Node) (sql.Node, error) {
	switch node.(type) {
	case *sql.OrderingTerm:
		rw.orderedBy = false
	}
	return node, nil
}

// isTimeFunc returns true if name is a SQLite time function which takes the
// time value as its first argument.
func isTimeFunc(name string) bool {
	for _, fn := range []string{"date", "time", "datetime", "julianday", "unixepoch"} {
		if strings.EqualFold(name, fn) {
			return true
		}
	}
	return false
}

func isNow(e sql.Expr) bool {
	return isString(e, "now")
}

// isSubsec returns true if the expression is the 'subsec' modifier. When
// SQLite finds it in place of a time value, it uses the current time.
func isSubsec(e sql.Expr) bool {
	return isString(e, "subsec") || isString(e, "subsecond")
}

func isString(e sql.Expr, v string) bool {
	if i, ok := e.(*sql.Ident); ok {
		return strings.EqualFold(i.Name, v)
	} else if s, ok := e.(*sql.StringLit); ok {
		return strings.EqualFold(s.Value, v)
	}
	return false
}

func julianDayAsNumberLit(t time.Time) *sql.NumberLit {
	return &sql.NumberLit{Value: fmt.Sprintf("%f", JulianDay(t))}
}

func JulianDay(t time.Time) float64 {
	t = t.UTC()
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
