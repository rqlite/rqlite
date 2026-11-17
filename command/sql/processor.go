package sql

import (
	"expvar"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
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

// maxRandomBlobSize is the largest blob RANDOMBLOB() will be replaced with.
// It is the largest blob SQLite will create by default, so a request for
// anything bigger is left for SQLite to reject.
const maxRandomBlobSize = 1000000000

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
// an actual random value, and calls to RANDOMBLOB() with actual random bytes.
// The size passed to RANDOMBLOB() may be a number, arithmetic on numbers, or
// a bound parameter. If time-rewriting is enabled, 'now' arguments to the
// time functions are replaced with the current Julian day, and the keywords
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
		rewriter.Params = stmts[i].Parameters
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

// ContainsTime returns true if the statement contains a time-related function
// or one of the CURRENT_TIMESTAMP, CURRENT_DATE and CURRENT_TIME keywords.
// The function performs a lower-case comparison so it is up to the caller to
// ensure the statement is lower-cased.
func ContainsTime(stmt string) bool {
	// Since this is a simple substring search, it also matches datetime(,
	// strftime( and current_timestamp.
	targets := []string{"time(", "date(", "julianday(", "unixepoch(", "timediff(",
		"current_time", "current_date"}
	for _, target := range targets {
		if strings.Contains(stmt, target) {
			return true
		}
	}
	return false
}

// ContainsRandom returns true if the statement contains a random-related function.
// The function performs a lower-case comparison so it is up to the caller to
// ensure the statement is lower-cased.
func ContainsRandom(stmt string) bool {
	targets := []string{"random(", "randomblob("}
	for _, target := range targets {
		if strings.Contains(stmt, target) {
			return true
		}
	}
	return false
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

	// Params are the values bound to the parameters of the statement. When
	// rewriting random, a parameter which supplies the size to RANDOMBLOB()
	// has its value changed to the random bytes.
	Params []*proto.Parameter

	stmt    sql.Statement
	binds   *boundParams
	removed map[*sql.BindExpr]bool // Parameters replaced in the statement.

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
	rw.stmt = stmt
	rw.binds = nil
	rw.removed = nil
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
					Args: []sql.Expr{julianDayAsNumberLit(rw.nowFn())},
				}
				rw.modified = true
			}
		}
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
			if len(n.Args) != 1 {
				break
			}
			if b, ok := n.Args[0].(*sql.BindExpr); ok {
				retNode, err = rw.randomBlobParam(n, b)
				if err != nil {
					return nil, nil, err
				}
				break
			}
			size, ok := constInt(n.Args[0])
			if !ok || size > maxRandomBlobSize {
				break
			}
			retNode = randomBlobLit(size)
			rw.modified = true
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

// randomBlobParam rewrites a call to RANDOMBLOB() which takes its size from
// the parameter b, returning the node to use in place of the call.
//
// The call can't simply be replaced with the random bytes, since removing a
// parameter from the statement changes which value each of the remaining
// parameters is bound to. So the parameter takes the place of the call, and
// its bound value is changed from the size to the random bytes. If the size
// is also used elsewhere in the statement its value can't be changed, and the
// call is replaced with the bytes, as long as that leaves the numbering of the
// remaining parameters as it was. If it doesn't, the call is left alone.
func (rw *Rewriter) randomBlobParam(call *sql.Call, b *sql.BindExpr) (sql.Node, error) {
	if rw.binds == nil {
		binds, err := newBoundParams(rw.stmt, rw.Params)
		if err != nil {
			return nil, err
		}
		rw.binds = binds
	}
	p := rw.binds.param[b]
	if p == nil {
		return call, nil
	}
	size, ok := paramInt(p)
	if !ok || size > maxRandomBlobSize {
		return call, nil
	}

	inUse := false
	for _, o := range rw.binds.uses[p] {
		if o != b && !rw.removed[o] {
			inUse = true
			break
		}
	}
	if !inUse {
		p.Value = &proto.Parameter_Y{Y: random.Bytes(int(max(size, 1)))}
		rw.modified = true
		return b, nil
	}

	removed := map[*sql.BindExpr]bool{b: true}
	for o := range rw.removed {
		removed[o] = true
	}
	if !sameInts(rw.binds.indexes, bindIndexes(rw.binds.binds, removed)) {
		return call, nil
	}
	rw.removed = removed
	rw.modified = true
	return randomBlobLit(size), nil
}

// randomBlobLit returns a blob literal of size random bytes. As with
// RANDOMBLOB() itself, a size of less than 1 results in a single byte.
func randomBlobLit(size int64) *sql.BlobLit {
	return &sql.BlobLit{Value: fmt.Sprintf(`%X`, random.Bytes(int(max(size, 1))))}
}

// boundParams records which value each parameter in a statement is bound to.
type boundParams struct {
	binds   []*sql.BindExpr // In the order they appear in the statement.
	indexes []int           // The index SQLite assigns to each of binds.
	param   map[*sql.BindExpr]*proto.Parameter
	uses    map[*proto.Parameter][]*sql.BindExpr
}

// newBoundParams returns the bindings of params to the parameters in stmt.
func newBoundParams(stmt sql.Statement, params []*proto.Parameter) (*boundParams, error) {
	c := &bindCollector{}
	if _, err := sql.Walk(c, stmt); err != nil {
		return nil, err
	}
	bp := &boundParams{
		binds: c.binds,
		param: make(map[*sql.BindExpr]*proto.Parameter),
		uses:  make(map[*proto.Parameter][]*sql.BindExpr),
	}
	sort.Slice(bp.binds, func(i, j int) bool {
		return bp.binds[i].NamePos.Offset < bp.binds[j].NamePos.Offset
	})
	bp.indexes = bindIndexes(bp.binds, nil)

	// Values are bound in order, each by name if it has one and by its
	// position in the list if not, so a later value replaces an earlier one.
	bound := make(map[int]*proto.Parameter)
	for i, p := range params {
		if p.GetName() == "" {
			bound[i+1] = p
			continue
		}
		for j, b := range bp.binds {
			if !strings.HasPrefix(b.Name, "?") && b.Name[1:] == p.GetName() {
				bound[bp.indexes[j]] = p
			}
		}
	}
	for i, b := range bp.binds {
		if p := bound[bp.indexes[i]]; p != nil {
			bp.param[b] = p
			bp.uses[p] = append(bp.uses[p], b)
		}
	}
	return bp, nil
}

// bindCollector is a sql.Visitor which collects the parameters in a statement.
type bindCollector struct {
	binds []*sql.BindExpr
}

// Visit implements sql.Visitor.
func (c *bindCollector) Visit(node sql.Node) (sql.Visitor, sql.Node, error) {
	if b, ok := node.(*sql.BindExpr); ok {
		c.binds = append(c.binds, b)
	}
	return c, node, nil
}

// VisitEnd implements sql.Visitor.
func (c *bindCollector) VisitEnd(node sql.Node) (sql.Node, error) {
	return node, nil
}

// bindIndexes returns the index SQLite assigns to each of the given
// parameters, which must be in the order they appear in the statement. A
// parameter in skip is treated as if it were absent, and has an index of 0.
func bindIndexes(binds []*sql.BindExpr, skip map[*sql.BindExpr]bool) []int {
	indexes := make([]int, len(binds))
	named := make(map[string]int)
	largest := 0
	for i, b := range binds {
		if skip[b] {
			continue
		}
		if b.Name == "?" {
			largest++
			indexes[i] = largest
		} else if n, err := strconv.Atoi(b.Name[1:]); err == nil && b.Name[0] == '?' {
			indexes[i] = n
			largest = max(largest, n)
		} else if idx, ok := named[b.Name]; ok {
			indexes[i] = idx
		} else {
			largest++
			indexes[i] = largest
			named[b.Name] = largest
		}
	}
	return indexes
}

// sameInts returns true if every non-zero value in b equals the value at
// the same position in a.
func sameInts(a, b []int) bool {
	for i := range b {
		if b[i] != 0 && b[i] != a[i] {
			return false
		}
	}
	return true
}

// paramInt returns the value of the parameter as SQLite would read it when
// an integer is called for. It returns false if the value is not supported.
func paramInt(p *proto.Parameter) (int64, bool) {
	switch v := p.GetValue().(type) {
	case *proto.Parameter_I:
		return v.I, true
	case *proto.Parameter_D:
		return floatToInt(v.D), true
	case *proto.Parameter_B:
		if v.B {
			return 1, true
		}
		return 0, true
	case *proto.Parameter_S:
		return textToInt(v.S), true
	case *proto.Parameter_Y:
		return textToInt(string(v.Y)), true
	case nil:
		return 0, true
	}
	return 0, false
}

// textToInt returns the integer at the start of s, ignoring anything which
// follows it. It returns 0 if there is none.
func textToInt(s string) int64 {
	s = strings.TrimLeft(s, " \t\n\v\f\r")
	end := 0
	if end < len(s) && (s[end] == '-' || s[end] == '+') {
		end++
	}
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	// On error n is 0 if there are no digits, and the nearest value if the
	// digits are out of range, which is what is wanted.
	n, _ := strconv.ParseInt(s[:end], 10, 64)
	return n
}

// floatToInt returns f truncated to an integer.
func floatToInt(f float64) int64 {
	if f >= math.MaxInt64 {
		return math.MaxInt64
	} else if f <= math.MinInt64 {
		return math.MinInt64
	} else if math.IsNaN(f) {
		return 0
	}
	return int64(f)
}

// number is the value of a constant numeric expression.
type number struct {
	i       int64
	f       float64
	isFloat bool
	isNull  bool
}

func (n number) float() float64 {
	if n.isFloat {
		return n.f
	}
	return float64(n.i)
}

func (n number) int() int64 {
	if n.isNull {
		return 0
	} else if n.isFloat {
		return floatToInt(n.f)
	}
	return n.i
}

// constInt returns the integer value of the expression, if the expression is
// made up of only numbers and arithmetic on them.
func constInt(e sql.Expr) (int64, bool) {
	n, ok := constNumber(e)
	return n.int(), ok
}

// constNumber returns the value of the expression, following the rules SQLite
// uses for arithmetic. It returns false if the expression is anything other
// than numbers and arithmetic on them, or if the result would overflow.
func constNumber(e sql.Expr) (number, bool) {
	switch e := e.(type) {
	case *sql.NumberLit:
		if len(e.Value) > 2 && (e.Value[:2] == "0x" || e.Value[:2] == "0X") {
			u, err := strconv.ParseUint(e.Value[2:], 16, 64)
			return number{i: int64(u)}, err == nil
		}
		if i, err := strconv.ParseInt(e.Value, 10, 64); err == nil {
			return number{i: i}, true
		}
		f, err := strconv.ParseFloat(e.Value, 64)
		return number{f: f, isFloat: true}, err == nil
	case *sql.ParenExpr:
		return constNumber(e.X)
	case *sql.UnaryExpr:
		x, ok := constNumber(e.X)
		if !ok {
			return number{}, false
		}
		switch e.Op {
		case sql.PLUS:
			return x, true
		case sql.MINUS:
			if x.isNull {
				return x, true
			} else if !x.isFloat && x.i == math.MinInt64 {
				return number{}, false
			}
			return number{i: -x.i, f: -x.f, isFloat: x.isFloat}, true
		}
		return number{}, false
	case *sql.BinaryExpr:
		x, ok := constNumber(e.X)
		if !ok {
			return number{}, false
		}
		y, ok := constNumber(e.Y)
		if !ok {
			return number{}, false
		}
		switch e.Op {
		case sql.PLUS, sql.MINUS, sql.STAR, sql.SLASH, sql.REM:
		default:
			return number{}, false
		}
		if x.isNull || y.isNull {
			return number{isNull: true}, true
		}
		if e.Op == sql.REM {
			// The operands are converted to integers.
			xi, yi := x.int(), y.int()
			if yi == 0 {
				return number{isNull: true}, true
			} else if yi == -1 {
				return number{}, true
			}
			return number{i: xi % yi}, true
		}
		if x.isFloat || y.isFloat {
			xf, yf := x.float(), y.float()
			switch e.Op {
			case sql.PLUS:
				return number{f: xf + yf, isFloat: true}, true
			case sql.MINUS:
				return number{f: xf - yf, isFloat: true}, true
			case sql.STAR:
				return number{f: xf * yf, isFloat: true}, true
			}
			if yf == 0 {
				return number{isNull: true}, true
			}
			return number{f: xf / yf, isFloat: true}, true
		}
		switch e.Op {
		case sql.PLUS:
			r := x.i + y.i
			return number{i: r}, (r > x.i) == (y.i > 0)
		case sql.MINUS:
			r := x.i - y.i
			return number{i: r}, (r < x.i) == (y.i > 0)
		case sql.STAR:
			r := x.i * y.i
			return number{i: r}, x.i == 0 || (r/x.i == y.i && !(x.i == -1 && y.i == math.MinInt64))
		}
		if y.i == 0 {
			return number{isNull: true}, true
		} else if y.i == -1 {
			return number{i: -x.i}, x.i != math.MinInt64
		}
		return number{i: x.i / y.i}, true
	}
	return number{}, false
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
