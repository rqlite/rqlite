package sql

import (
	"bytes"
	"fmt"
	"strings"
)

type Node interface {
	node()
	fmt.Stringer
}

func (*AlterTableStatement) node()    {}
func (*AnalyzeStatement) node()       {}
func (*Assignment) node()             {}
func (*BeginStatement) node()         {}
func (*BinaryExpr) node()             {}
func (*BindExpr) node()               {}
func (*BlobLit) node()                {}
func (*BoolLit) node()                {}
func (*Call) node()                   {}
func (*CaseBlock) node()              {}
func (*CaseExpr) node()               {}
func (*CastExpr) node()               {}
func (*CheckConstraint) node()        {}
func (*ColumnDefinition) node()       {}
func (*CommitStatement) node()        {}
func (*CreateIndexStatement) node()   {}
func (*CreateTableStatement) node()   {}
func (*CreateTriggerStatement) node() {}
func (*CreateViewStatement) node()    {}
func (*DefaultConstraint) node()      {}
func (*DeleteStatement) node()        {}
func (*DropIndexStatement) node()     {}
func (*DropTableStatement) node()     {}
func (*DropTriggerStatement) node()   {}
func (*DropViewStatement) node()      {}
func (*Exists) node()                 {}
func (*ExplainStatement) node()       {}
func (*ExprList) node()               {}
func (*FilterClause) node()           {}
func (*ForeignKeyArg) node()          {}
func (*ForeignKeyConstraint) node()   {}
func (*FrameSpec) node()              {}
func (*Ident) node()                  {}
func (*IndexedColumn) node()          {}
func (*InsertStatement) node()        {}
func (*JoinClause) node()             {}
func (*JoinOperator) node()           {}
func (*NotNullConstraint) node()      {}
func (*NullLit) node()                {}
func (*NumberLit) node()              {}
func (*OnConstraint) node()           {}
func (*OrderingTerm) node()           {}
func (*OverClause) node()             {}
func (*ParenExpr) node()              {}
func (*ParenSource) node()            {}
func (*PrimaryKeyConstraint) node()   {}
func (*QualifiedRef) node()           {}
func (*QualifiedTableName) node()     {}
func (*Raise) node()                  {}
func (*Range) node()                  {}
func (*ReleaseStatement) node()       {}
func (*ResultColumn) node()           {}
func (*RollbackStatement) node()      {}
func (*SavepointStatement) node()     {}
func (*SelectStatement) node()        {}
func (*StringLit) node()              {}
func (*Type) node()                   {}
func (*UnaryExpr) node()              {}
func (*UniqueConstraint) node()       {}
func (*UpdateStatement) node()        {}
func (*UpsertClause) node()           {}
func (*UsingConstraint) node()        {}
func (*Window) node()                 {}
func (*WindowDefinition) node()       {}
func (*WithClause) node()             {}

type Statement interface {
	Node
	stmt()
}

func (*AlterTableStatement) stmt()    {}
func (*AnalyzeStatement) stmt()       {}
func (*BeginStatement) stmt()         {}
func (*CommitStatement) stmt()        {}
func (*CreateIndexStatement) stmt()   {}
func (*CreateTableStatement) stmt()   {}
func (*CreateTriggerStatement) stmt() {}
func (*CreateViewStatement) stmt()    {}
func (*DeleteStatement) stmt()        {}
func (*DropIndexStatement) stmt()     {}
func (*DropTableStatement) stmt()     {}
func (*DropTriggerStatement) stmt()   {}
func (*DropViewStatement) stmt()      {}
func (*ExplainStatement) stmt()       {}
func (*InsertStatement) stmt()        {}
func (*ReleaseStatement) stmt()       {}
func (*RollbackStatement) stmt()      {}
func (*SavepointStatement) stmt()     {}
func (*SelectStatement) stmt()        {}
func (*UpdateStatement) stmt()        {}

// CloneStatement returns a deep copy stmt.
func CloneStatement(stmt Statement) Statement {
	if stmt == nil {
		return nil
	}

	switch stmt := stmt.(type) {
	case *AlterTableStatement:
		return stmt.Clone()
	case *AnalyzeStatement:
		return stmt.Clone()
	case *BeginStatement:
		return stmt.Clone()
	case *CommitStatement:
		return stmt.Clone()
	case *CreateIndexStatement:
		return stmt.Clone()
	case *CreateTableStatement:
		return stmt.Clone()
	case *CreateTriggerStatement:
		return stmt.Clone()
	case *CreateViewStatement:
		return stmt.Clone()
	case *DeleteStatement:
		return stmt.Clone()
	case *DropIndexStatement:
		return stmt.Clone()
	case *DropTableStatement:
		return stmt.Clone()
	case *DropTriggerStatement:
		return stmt.Clone()
	case *DropViewStatement:
		return stmt.Clone()
	case *ExplainStatement:
		return stmt.Clone()
	case *InsertStatement:
		return stmt.Clone()
	case *ReleaseStatement:
		return stmt.Clone()
	case *RollbackStatement:
		return stmt.Clone()
	case *SavepointStatement:
		return stmt.Clone()
	case *SelectStatement:
		return stmt.Clone()
	case *UpdateStatement:
		return stmt.Clone()
	default:
		panic(fmt.Sprintf("invalid statement type: %T", stmt))
	}
}

func cloneStatements(a []Statement) []Statement {
	if a == nil {
		return nil
	}
	other := make([]Statement, len(a))
	for i := range a {
		other[i] = CloneStatement(a[i])
	}
	return other
}

// StatementSource returns the root statement for a statement.
func StatementSource(stmt Statement) Source {
	switch stmt := stmt.(type) {
	case *SelectStatement:
		return stmt.Source
	case *UpdateStatement:
		return stmt.Table
	case *DeleteStatement:
		return stmt.Table
	default:
		return nil
	}
}

type Expr interface {
	Node
	expr()
}

func (*BinaryExpr) expr()   {}
func (*BindExpr) expr()     {}
func (*BlobLit) expr()      {}
func (*BoolLit) expr()      {}
func (*Call) expr()         {}
func (*CaseExpr) expr()     {}
func (*CastExpr) expr()     {}
func (*Exists) expr()       {}
func (*ExprList) expr()     {}
func (*Ident) expr()        {}
func (*NullLit) expr()      {}
func (*NumberLit) expr()    {}
func (*ParenExpr) expr()    {}
func (*QualifiedRef) expr() {}
func (*Raise) expr()        {}
func (*Range) expr()        {}
func (*StringLit) expr()    {}
func (*UnaryExpr) expr()    {}

// CloneExpr returns a deep copy expr.
func CloneExpr(expr Expr) Expr {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *BinaryExpr:
		return expr.Clone()
	case *BindExpr:
		return expr.Clone()
	case *BlobLit:
		return expr.Clone()
	case *BoolLit:
		return expr.Clone()
	case *Call:
		return expr.Clone()
	case *CaseExpr:
		return expr.Clone()
	case *CastExpr:
		return expr.Clone()
	case *Exists:
		return expr.Clone()
	case *ExprList:
		return expr.Clone()
	case *Ident:
		return expr.Clone()
	case *NullLit:
		return expr.Clone()
	case *NumberLit:
		return expr.Clone()
	case *ParenExpr:
		return expr.Clone()
	case *QualifiedRef:
		return expr.Clone()
	case *Raise:
		return expr.Clone()
	case *Range:
		return expr.Clone()
	case *StringLit:
		return expr.Clone()
	case *UnaryExpr:
		return expr.Clone()
	default:
		panic(fmt.Sprintf("invalid expr type: %T", expr))
	}
}

func cloneExprs(a []Expr) []Expr {
	if a == nil {
		return nil
	}
	other := make([]Expr, len(a))
	for i := range a {
		other[i] = CloneExpr(a[i])
	}
	return other
}

// ExprString returns the string representation of expr.
// Returns a blank string if expr is nil.
func ExprString(expr Expr) string {
	if expr == nil {
		return ""
	}
	return expr.String()
}

// SplitExprTree splits apart expr so it is a list of all AND joined expressions.
// For example, the expression "A AND B AND (C OR (D AND E))" would be split into
// a list of "A", "B", "C OR (D AND E)".
func SplitExprTree(expr Expr) []Expr {
	if expr == nil {
		return nil
	}

	var a []Expr
	splitExprTree(expr, &a)
	return a
}

func splitExprTree(expr Expr, a *[]Expr) {
	switch expr := expr.(type) {
	case *BinaryExpr:
		if expr.Op != AND {
			*a = append(*a, expr)
			return
		}
		splitExprTree(expr.X, a)
		splitExprTree(expr.Y, a)
	case *ParenExpr:
		splitExprTree(expr.X, a)
	default:
		*a = append(*a, expr)
	}
}

// Scope represents a context for name resolution.
// Names can be resolved at the current source or in parent scopes.
type Scope struct {
	Parent *Scope
	Source Source
}

// Source represents a table or subquery.
type Source interface {
	Node
	source()
}

func (*JoinClause) source()         {}
func (*ParenSource) source()        {}
func (*QualifiedTableName) source() {}
func (*SelectStatement) source()    {}

// CloneSource returns a deep copy src.
func CloneSource(src Source) Source {
	if src == nil {
		return nil
	}

	switch src := src.(type) {
	case *JoinClause:
		return src.Clone()
	case *ParenSource:
		return src.Clone()
	case *QualifiedTableName:
		return src.Clone()
	case *SelectStatement:
		return src.Clone()
	default:
		panic(fmt.Sprintf("invalid source type: %T", src))
	}
}

// SourceName returns the name of the source.
// Only returns for QualifiedTableName & ParenSource.
func SourceName(src Source) string {
	switch src := src.(type) {
	case *JoinClause, *SelectStatement:
		return ""
	case *ParenSource:
		return IdentName(src.Alias)
	case *QualifiedTableName:
		return src.TableName()
	default:
		return ""
	}
}

// SourceList returns a list of scopes in the current scope.
func SourceList(src Source) []Source {
	var a []Source
	ForEachSource(src, func(s Source) bool {
		a = append(a, s)
		return true
	})
	return a
}

// ForEachSource calls fn for every source within the current scope.
// Stops iteration if fn returns false.
func ForEachSource(src Source, fn func(Source) bool) {
	forEachSource(src, fn)
}

func forEachSource(src Source, fn func(Source) bool) bool {
	if !fn(src) {
		return false
	}

	switch src := src.(type) {
	case *JoinClause:
		if !forEachSource(src.X, fn) {
			return false
		} else if !forEachSource(src.Y, fn) {
			return false
		}
	case *SelectStatement:
		if !forEachSource(src.Source, fn) {
			return false
		}
	}
	return true
}

// ResolveSource returns a source with the given name.
// This can either be the table name or the alias for a source.
func ResolveSource(root Source, name string) Source {
	var ret Source
	ForEachSource(root, func(src Source) bool {
		switch src := src.(type) {
		case *ParenSource:
			if IdentName(src.Alias) == name {
				ret = src
			}
		case *QualifiedTableName:
			if src.TableName() == name {
				ret = src
			}
		}
		return ret == nil // continue until we find the matching source
	})
	return ret
}

// JoinConstraint represents either an ON or USING join constraint.
type JoinConstraint interface {
	Node
	joinConstraint()
}

func (*OnConstraint) joinConstraint()    {}
func (*UsingConstraint) joinConstraint() {}

// CloneJoinConstraint returns a deep copy cons.
func CloneJoinConstraint(cons JoinConstraint) JoinConstraint {
	if cons == nil {
		return nil
	}

	switch cons := cons.(type) {
	case *OnConstraint:
		return cons.Clone()
	case *UsingConstraint:
		return cons.Clone()
	default:
		panic(fmt.Sprintf("invalid join constraint type: %T", cons))
	}
}

type ExplainStatement struct {
	Explain   Pos       // position of EXPLAIN
	Query     Pos       // position of QUERY (optional)
	QueryPlan Pos       // position of PLAN after QUERY (optional)
	Stmt      Statement // target statement
}

// Clone returns a deep copy of s.
func (s *ExplainStatement) Clone() *ExplainStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Stmt = CloneStatement(s.Stmt)
	return &other
}

// String returns the string representation of the statement.
func (s *ExplainStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("EXPLAIN")
	if s.QueryPlan.IsValid() {
		buf.WriteString(" QUERY PLAN")
	}
	fmt.Fprintf(&buf, " %s", s.Stmt.String())
	return buf.String()
}

type BeginStatement struct {
	Begin       Pos // position of BEGIN
	Deferred    Pos // position of DEFERRED keyword
	Immediate   Pos // position of IMMEDIATE keyword
	Exclusive   Pos // position of EXCLUSIVE keyword
	Transaction Pos // position of TRANSACTION keyword  (optional)
}

// Clone returns a deep copy of s.
func (s *BeginStatement) Clone() *BeginStatement {
	if s == nil {
		return nil
	}
	other := *s
	return &other
}

// String returns the string representation of the statement.
func (s *BeginStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("BEGIN")
	if s.Deferred.IsValid() {
		buf.WriteString(" DEFERRED")
	} else if s.Immediate.IsValid() {
		buf.WriteString(" IMMEDIATE")
	} else if s.Exclusive.IsValid() {
		buf.WriteString(" EXCLUSIVE")
	}
	if s.Transaction.IsValid() {
		buf.WriteString(" TRANSACTION")
	}
	return buf.String()
}

type CommitStatement struct {
	Commit      Pos // position of COMMIT keyword
	End         Pos // position of END keyword
	Transaction Pos // position of TRANSACTION keyword  (optional)
}

// Clone returns a deep copy of s.
func (s *CommitStatement) Clone() *CommitStatement {
	if s == nil {
		return nil
	}
	other := *s
	return &other
}

// String returns the string representation of the statement.
func (s *CommitStatement) String() string {
	var buf bytes.Buffer
	if s.End.IsValid() {
		buf.WriteString("END")
	} else {
		buf.WriteString("COMMIT")
	}

	if s.Transaction.IsValid() {
		buf.WriteString(" TRANSACTION")
	}
	return buf.String()
}

type RollbackStatement struct {
	Rollback      Pos    // position of ROLLBACK keyword
	Transaction   Pos    // position of TRANSACTION keyword  (optional)
	To            Pos    // position of TO keyword  (optional)
	Savepoint     Pos    // position of SAVEPOINT keyword  (optional)
	SavepointName *Ident // name of savepoint
}

// Clone returns a deep copy of s.
func (s *RollbackStatement) Clone() *RollbackStatement {
	if s == nil {
		return s
	}
	other := *s
	other.SavepointName = s.SavepointName.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *RollbackStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("ROLLBACK")
	if s.Transaction.IsValid() {
		buf.WriteString(" TRANSACTION")
	}

	if s.SavepointName != nil {
		buf.WriteString(" TO")
		if s.Savepoint.IsValid() {
			buf.WriteString(" SAVEPOINT")
		}
		fmt.Fprintf(&buf, " %s", s.SavepointName.String())
	}
	return buf.String()
}

type SavepointStatement struct {
	Savepoint Pos    // position of SAVEPOINT keyword
	Name      *Ident // name of savepoint
}

// Clone returns a deep copy of s.
func (s *SavepointStatement) Clone() *SavepointStatement {
	if s == nil {
		return s
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *SavepointStatement) String() string {
	return fmt.Sprintf("SAVEPOINT %s", s.Name.String())
}

type ReleaseStatement struct {
	Release   Pos    // position of RELEASE keyword
	Savepoint Pos    // position of SAVEPOINT keyword (optional)
	Name      *Ident // name of savepoint
}

// Clone returns a deep copy of s.
func (s *ReleaseStatement) Clone() *ReleaseStatement {
	if s == nil {
		return s
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *ReleaseStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("RELEASE")
	if s.Savepoint.IsValid() {
		buf.WriteString(" SAVEPOINT")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type CreateTableStatement struct {
	Create      Pos    // position of CREATE keyword
	Table       Pos    // position of CREATE keyword
	If          Pos    // position of IF keyword (optional)
	IfNot       Pos    // position of NOT keyword (optional)
	IfNotExists Pos    // position of EXISTS keyword (optional)
	Name        *Ident // table name

	Lparen      Pos                 // position of left paren of column list
	Columns     []*ColumnDefinition // column definitions
	Constraints []Constraint        // table constraints
	Rparen      Pos                 // position of right paren of column list

	As     Pos              // position of AS keyword (optional)
	Select *SelectStatement // select stmt to build from
}

// Clone returns a deep copy of s.
func (s *CreateTableStatement) Clone() *CreateTableStatement {
	if s == nil {
		return s
	}
	other := *s
	other.Name = s.Name.Clone()
	other.Columns = cloneColumnDefinitions(s.Columns)
	other.Constraints = cloneConstraints(s.Constraints)
	other.Select = s.Select.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *CreateTableStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE")
	if s.IfNotExists.IsValid() {
		buf.WriteString(" IF NOT EXISTS")
	}
	buf.WriteString(" ")
	buf.WriteString(s.Name.String())

	if s.Select != nil {
		buf.WriteString(" AS ")
		buf.WriteString(s.Select.String())
	} else {
		buf.WriteString(" (")
		for i := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(s.Columns[i].String())
		}
		for i := range s.Constraints {
			buf.WriteString(", ")
			buf.WriteString(s.Constraints[i].String())
		}
		buf.WriteString(")")
	}

	return buf.String()
}

type ColumnDefinition struct {
	Name        *Ident       // column name
	Type        *Type        // data type
	Constraints []Constraint // column constraints
}

// Clone returns a deep copy of d.
func (d *ColumnDefinition) Clone() *ColumnDefinition {
	if d == nil {
		return d
	}
	other := *d
	other.Name = d.Name.Clone()
	other.Type = d.Type.Clone()
	other.Constraints = cloneConstraints(d.Constraints)
	return &other
}

func cloneColumnDefinitions(a []*ColumnDefinition) []*ColumnDefinition {
	if a == nil {
		return nil
	}
	other := make([]*ColumnDefinition, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the statement.
func (c *ColumnDefinition) String() string {
	var buf bytes.Buffer
	buf.WriteString(c.Name.String())
	buf.WriteString(" ")
	buf.WriteString(c.Type.String())
	for i := range c.Constraints {
		buf.WriteString(" ")
		buf.WriteString(c.Constraints[i].String())
	}
	return buf.String()
}

type Constraint interface {
	Node
	constraint()
}

func (*PrimaryKeyConstraint) constraint() {}
func (*NotNullConstraint) constraint()    {}
func (*UniqueConstraint) constraint()     {}
func (*CheckConstraint) constraint()      {}
func (*DefaultConstraint) constraint()    {}
func (*ForeignKeyConstraint) constraint() {}

// CloneConstraint returns a deep copy cons.
func CloneConstraint(cons Constraint) Constraint {
	if cons == nil {
		return nil
	}

	switch cons := cons.(type) {
	case *PrimaryKeyConstraint:
		return cons.Clone()
	case *NotNullConstraint:
		return cons.Clone()
	case *UniqueConstraint:
		return cons.Clone()
	case *CheckConstraint:
		return cons.Clone()
	case *DefaultConstraint:
		return cons.Clone()
	case *ForeignKeyConstraint:
		return cons.Clone()
	default:
		panic(fmt.Sprintf("invalid constraint type: %T", cons))
	}
}

func cloneConstraints(a []Constraint) []Constraint {
	if a == nil {
		return nil
	}
	other := make([]Constraint, len(a))
	for i := range a {
		other[i] = CloneConstraint(a[i])
	}
	return other
}

type PrimaryKeyConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Primary    Pos    // position of PRIMARY keyword
	Key        Pos    // position of KEY keyword

	Lparen  Pos      // position of left paren (table only)
	Columns []*Ident // indexed columns (table only)
	Rparen  Pos      // position of right paren (table only)

	Autoincrement Pos // position of AUTOINCREMENT keyword (column only)
}

// Clone returns a deep copy of c.
func (c *PrimaryKeyConstraint) Clone() *PrimaryKeyConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Columns = cloneIdents(c.Columns)
	return &other
}

// String returns the string representation of the constraint.
func (c *PrimaryKeyConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("PRIMARY KEY")

	if len(c.Columns) > 0 {
		buf.WriteString(" (")
		for i := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Columns[i].String())
		}
		buf.WriteString(")")
	}

	if c.Autoincrement.IsValid() {
		buf.WriteString(" AUTOINCREMENT")
	}
	return buf.String()
}

type NotNullConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Not        Pos    // position of NOT keyword
	Null       Pos    // position of NULL keyword
}

// Clone returns a deep copy of c.
func (c *NotNullConstraint) Clone() *NotNullConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	return &other
}

// String returns the string representation of the constraint.
func (c *NotNullConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("NOT NULL")

	return buf.String()
}

type UniqueConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Unique     Pos    // position of UNIQUE keyword

	Lparen  Pos      // position of left paren (table only)
	Columns []*Ident // indexed columns (table only)
	Rparen  Pos      // position of right paren (table only)
}

// Clone returns a deep copy of c.
func (c *UniqueConstraint) Clone() *UniqueConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Columns = cloneIdents(c.Columns)
	return &other
}

// String returns the string representation of the constraint.
func (c *UniqueConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("UNIQUE")

	if len(c.Columns) > 0 {
		buf.WriteString(" (")
		for i := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Columns[i].String())
		}
		buf.WriteString(")")
	}

	return buf.String()
}

type CheckConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Check      Pos    // position of UNIQUE keyword
	Lparen     Pos    // position of left paren
	Expr       Expr   // check expression
	Rparen     Pos    // position of right paren
}

// Clone returns a deep copy of c.
func (c *CheckConstraint) Clone() *CheckConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Expr = CloneExpr(c.Expr)
	return &other
}

// String returns the string representation of the constraint.
func (c *CheckConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("CHECK (")
	buf.WriteString(c.Expr.String())
	buf.WriteString(")")
	return buf.String()
}

type DefaultConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Default    Pos    // position of DEFAULT keyword
	Lparen     Pos    // position of left paren
	Expr       Expr   // default expression
	Rparen     Pos    // position of right paren
}

// Clone returns a deep copy of c.
func (c *DefaultConstraint) Clone() *DefaultConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Expr = CloneExpr(c.Expr)
	return &other
}

// String returns the string representation of the constraint.
func (c *DefaultConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("DEFAULT ")

	if c.Lparen.IsValid() {
		buf.WriteString("(")
		buf.WriteString(c.Expr.String())
		buf.WriteString(")")
	} else {
		buf.WriteString(c.Expr.String())
	}
	return buf.String()
}

type ForeignKeyConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name

	Foreign    Pos      // position of FOREIGN keyword (table only)
	ForeignKey Pos      // position of KEY keyword after FOREIGN (table only)
	Lparen     Pos      // position of left paren (table only)
	Columns    []*Ident // indexed columns (table only)
	Rparen     Pos      // position of right paren (table only)

	References         Pos              // position of REFERENCES keyword
	ForeignTable       *Ident           // foreign table name
	ForeignLparen      Pos              // position of left paren
	ForeignColumns     []*Ident         // column list
	ForeignRparen      Pos              // position of right paren
	Args               []*ForeignKeyArg // arguments
	Deferrable         Pos              // position of DEFERRABLE keyword
	Not                Pos              // position of NOT keyword
	NotDeferrable      Pos              // position of DEFERRABLE keyword after NOT
	Initially          Pos              // position of INITIALLY keyword
	InitiallyDeferred  Pos              // position of DEFERRED keyword after INITIALLY
	InitiallyImmediate Pos              // position of IMMEDIATE keyword after INITIALLY
}

// Clone returns a deep copy of c.
func (c *ForeignKeyConstraint) Clone() *ForeignKeyConstraint {
	if c == nil {
		return c
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Columns = cloneIdents(c.Columns)
	other.ForeignTable = c.ForeignTable.Clone()
	other.ForeignColumns = cloneIdents(c.ForeignColumns)
	other.Args = cloneForeignKeyArgs(c.Args)
	return &other
}

// String returns the string representation of the constraint.
func (c *ForeignKeyConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	if len(c.Columns) > 0 {
		buf.WriteString("FOREIGN KEY (")
		for i := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Columns[i].String())
		}
		buf.WriteString(") ")
	}

	buf.WriteString("REFERENCES ")
	buf.WriteString(c.ForeignTable.String())
	if len(c.ForeignColumns) > 0 {
		buf.WriteString(" (")
		for i := range c.ForeignColumns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.ForeignColumns[i].String())
		}
		buf.WriteString(")")
	}

	for i := range c.Args {
		buf.WriteString(" ")
		buf.WriteString(c.Args[i].String())
	}

	if c.Deferrable.IsValid() || c.NotDeferrable.IsValid() {
		if c.Deferrable.IsValid() {
			buf.WriteString(" DEFERRABLE")
		} else {
			buf.WriteString(" NOT DEFERRABLE")
		}

		if c.InitiallyDeferred.IsValid() {
			buf.WriteString(" INITIALLY DEFERRED")
		} else if c.InitiallyImmediate.IsValid() {
			buf.WriteString(" INITIALLY IMMEDIATE")
		}
	}

	return buf.String()
}

type ForeignKeyArg struct {
	On         Pos // position of ON keyword
	OnUpdate   Pos // position of the UPDATE keyword
	OnDelete   Pos // position of the DELETE keyword
	Set        Pos // position of the SET keyword
	SetNull    Pos // position of the NULL keyword after SET
	SetDefault Pos // position of the DEFAULT keyword after SET
	Cascade    Pos // position of the CASCADE keyword
	Restrict   Pos // position of the RESTRICT keyword
	No         Pos // position of the NO keyword
	NoAction   Pos // position of the ACTION keyword after NO
}

// Clone returns a deep copy of arg.
func (arg *ForeignKeyArg) Clone() *ForeignKeyArg {
	if arg == nil {
		return nil
	}
	other := *arg
	return &other
}

func cloneForeignKeyArgs(a []*ForeignKeyArg) []*ForeignKeyArg {
	if a == nil {
		return nil
	}
	other := make([]*ForeignKeyArg, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the argument.
func (c *ForeignKeyArg) String() string {
	var buf bytes.Buffer
	buf.WriteString("ON")
	if c.OnUpdate.IsValid() {
		buf.WriteString(" UPDATE")
	} else {
		buf.WriteString(" DELETE")
	}

	if c.SetNull.IsValid() {
		buf.WriteString(" SET NULL")
	} else if c.SetDefault.IsValid() {
		buf.WriteString(" SET DEFAULT")
	} else if c.Cascade.IsValid() {
		buf.WriteString(" CASCADE")
	} else if c.Restrict.IsValid() {
		buf.WriteString(" RESTRICT")
	} else if c.NoAction.IsValid() {
		buf.WriteString(" NO ACTION")
	}
	return buf.String()
}

type AnalyzeStatement struct {
	Analyze Pos    // position of ANALYZE keyword
	Name    *Ident // table name
}

// Clone returns a deep copy of s.
func (s *AnalyzeStatement) Clone() *AnalyzeStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *AnalyzeStatement) String() string {
	return fmt.Sprintf("ANALYZE %s", s.Name.String())
}

type AlterTableStatement struct {
	Alter Pos    // position of ALTER keyword
	Table Pos    // position of TABLE keyword
	Name  *Ident // table name

	Rename   Pos    // position of RENAME keyword
	RenameTo Pos    // position of TO keyword after RENAME
	NewName  *Ident // new table name

	RenameColumn  Pos    // position of COLUMN keyword after RENAME
	ColumnName    *Ident // new column name
	To            Pos    // position of TO keyword
	NewColumnName *Ident // new column name

	Add       Pos               // position of ADD keyword
	AddColumn Pos               // position of COLUMN keyword after ADD
	ColumnDef *ColumnDefinition // new column definition
}

// Clone returns a deep copy of s.
func (s *AlterTableStatement) Clone() *AlterTableStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = other.Name.Clone()
	other.NewName = s.NewName.Clone()
	other.ColumnName = s.ColumnName.Clone()
	other.NewColumnName = s.NewColumnName.Clone()
	other.ColumnDef = s.ColumnDef.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *AlterTableStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE ")
	buf.WriteString(s.Name.String())

	if s.NewName != nil {
		buf.WriteString(" RENAME TO ")
		buf.WriteString(s.NewName.String())
	} else if s.ColumnName != nil {
		buf.WriteString(" RENAME COLUMN ")
		buf.WriteString(s.ColumnName.String())
		buf.WriteString(" TO ")
		buf.WriteString(s.NewColumnName.String())
	} else if s.ColumnDef != nil {
		buf.WriteString(" ADD COLUMN ")
		buf.WriteString(s.ColumnDef.String())
	}

	return buf.String()
}

type Ident struct {
	NamePos Pos    // identifier position
	Name    string // identifier name
	Quoted  bool   // true if double quoted
}

// Clone returns a deep copy of i.
func (i *Ident) Clone() *Ident {
	if i == nil {
		return nil
	}
	other := *i
	return &other
}

func cloneIdents(a []*Ident) []*Ident {
	if a == nil {
		return nil
	}
	other := make([]*Ident, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the expression.
func (i *Ident) String() string {
	return `"` + strings.Replace(i.Name, `"`, `""`, -1) + `"`
}

// IdentName returns the name of ident. Returns a blank string if ident is nil.
func IdentName(ident *Ident) string {
	if ident == nil {
		return ""
	}
	return ident.Name
}

type Type struct {
	Name      *Ident     // type name
	Lparen    Pos        // position of left paren (optional)
	Precision *NumberLit // precision (optional)
	Scale     *NumberLit // scale (optional)
	Rparen    Pos        // position of right paren (optional)
}

// Clone returns a deep copy of t.
func (t *Type) Clone() *Type {
	if t == nil {
		return nil
	}
	other := *t
	other.Name = t.Name.Clone()
	other.Precision = t.Precision.Clone()
	other.Scale = t.Scale.Clone()
	return &other
}

// String returns the string representation of the type.
func (t *Type) String() string {
	if t.Precision != nil && t.Scale != nil {
		return fmt.Sprintf("%s(%s,%s)", t.Name.Name, t.Precision.String(), t.Scale.String())
	} else if t.Precision != nil {
		return fmt.Sprintf("%s(%s)", t.Name.Name, t.Precision.String())
	}
	return t.Name.Name
}

type StringLit struct {
	ValuePos Pos    // literal position
	Value    string // literal value (without quotes)
}

// Clone returns a deep copy of lit.
func (lit *StringLit) Clone() *StringLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *StringLit) String() string {
	return `'` + strings.Replace(lit.Value, `'`, `''`, -1) + `'`
}

type BlobLit struct {
	ValuePos Pos    // literal position
	Value    string // literal value
}

// Clone returns a deep copy of lit.
func (lit *BlobLit) Clone() *BlobLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *BlobLit) String() string {
	return `x'` + lit.Value + `'`
}

type NumberLit struct {
	ValuePos Pos    // literal position
	Value    string // literal value
}

// Clone returns a deep copy of lit.
func (lit *NumberLit) Clone() *NumberLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *NumberLit) String() string {
	return lit.Value
}

type NullLit struct {
	Pos Pos
}

// Clone returns a deep copy of lit.
func (lit *NullLit) Clone() *NullLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *NullLit) String() string {
	return "NULL"
}

type BoolLit struct {
	ValuePos Pos  // literal position
	Value    bool // literal value
}

// Clone returns a deep copy of lit.
func (lit *BoolLit) Clone() *BoolLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *BoolLit) String() string {
	if lit.Value {
		return "TRUE"
	}
	return "FALSE"
}

type BindExpr struct {
	NamePos Pos    // name position
	Name    string // binding name
}

// Clone returns a deep copy of expr.
func (expr *BindExpr) Clone() *BindExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	return &other
}

// String returns the string representation of the expression.
func (expr *BindExpr) String() string {
	// TODO(BBJ): Support all bind characters.
	return "$" + expr.Name
}

type UnaryExpr struct {
	OpPos Pos   // operation position
	Op    Token // operation
	X     Expr  // target expression
}

// Clone returns a deep copy of expr.
func (expr *UnaryExpr) Clone() *UnaryExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	return &other
}

// String returns the string representation of the expression.
func (expr *UnaryExpr) String() string {
	switch expr.Op {
	case PLUS:
		return "+" + expr.X.String()
	case MINUS:
		return "-" + expr.X.String()
	default:
		panic(fmt.Sprintf("sql.UnaryExpr.String(): invalid op %s", expr.Op))
	}
}

type BinaryExpr struct {
	X     Expr  // lhs
	OpPos Pos   // position of Op
	Op    Token // operator
	Y     Expr  // rhs
}

// Clone returns a deep copy of expr.
func (expr *BinaryExpr) Clone() *BinaryExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	other.Y = CloneExpr(expr.Y)
	return &other
}

// String returns the string representation of the expression.
func (expr *BinaryExpr) String() string {
	switch expr.Op {
	case PLUS:
		return expr.X.String() + " + " + expr.Y.String()
	case MINUS:
		return expr.X.String() + " - " + expr.Y.String()
	case STAR:
		return expr.X.String() + " * " + expr.Y.String()
	case SLASH:
		return expr.X.String() + " / " + expr.Y.String()
	case REM:
		return expr.X.String() + " % " + expr.Y.String()
	case CONCAT:
		return expr.X.String() + " || " + expr.Y.String()
	case BETWEEN:
		return expr.X.String() + " BETWEEN " + expr.Y.String()
	case NOTBETWEEN:
		return expr.X.String() + " NOT BETWEEN " + expr.Y.String()
	case LSHIFT:
		return expr.X.String() + " << " + expr.Y.String()
	case RSHIFT:
		return expr.X.String() + " >> " + expr.Y.String()
	case BITAND:
		return expr.X.String() + " & " + expr.Y.String()
	case BITOR:
		return expr.X.String() + " | " + expr.Y.String()
	case LT:
		return expr.X.String() + " < " + expr.Y.String()
	case LE:
		return expr.X.String() + " <= " + expr.Y.String()
	case GT:
		return expr.X.String() + " > " + expr.Y.String()
	case GE:
		return expr.X.String() + " >= " + expr.Y.String()
	case EQ:
		return expr.X.String() + " = " + expr.Y.String()
	case NE:
		return expr.X.String() + " != " + expr.Y.String()
	case IS:
		return expr.X.String() + " IS " + expr.Y.String()
	case ISNOT:
		return expr.X.String() + " IS NOT " + expr.Y.String()
	case IN:
		return expr.X.String() + " IN " + expr.Y.String()
	case NOTIN:
		return expr.X.String() + " NOT IN " + expr.Y.String()
	case LIKE:
		return expr.X.String() + " LIKE " + expr.Y.String()
	case NOTLIKE:
		return expr.X.String() + " NOT LIKE " + expr.Y.String()
	case GLOB:
		return expr.X.String() + " GLOB " + expr.Y.String()
	case NOTGLOB:
		return expr.X.String() + " NOT GLOB " + expr.Y.String()
	case MATCH:
		return expr.X.String() + " MATCH " + expr.Y.String()
	case NOTMATCH:
		return expr.X.String() + " NOT MATCH " + expr.Y.String()
	case REGEXP:
		return expr.X.String() + " REGEXP " + expr.Y.String()
	case NOTREGEXP:
		return expr.X.String() + " NOT REGEXP " + expr.Y.String()
	case AND:
		return expr.X.String() + " AND " + expr.Y.String()
	case OR:
		return expr.X.String() + " OR " + expr.Y.String()
	default:
		panic(fmt.Sprintf("sql.BinaryExpr.String(): invalid op %s", expr.Op))
	}
}

type CastExpr struct {
	Cast   Pos   // position of CAST keyword
	Lparen Pos   // position of left paren
	X      Expr  // target expression
	As     Pos   // position of AS keyword
	Type   *Type // cast type
	Rparen Pos   // position of right paren
}

// Clone returns a deep copy of expr.
func (expr *CastExpr) Clone() *CastExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	other.Type = expr.Type.Clone()
	return &other
}

// String returns the string representation of the expression.
func (expr *CastExpr) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", expr.X.String(), expr.Type.String())
}

type CaseExpr struct {
	Case     Pos          // position of CASE keyword
	Operand  Expr         // optional condition after the CASE keyword
	Blocks   []*CaseBlock // list of WHEN/THEN pairs
	Else     Pos          // position of ELSE keyword
	ElseExpr Expr         // expression used by default case
	End      Pos          // position of END keyword
}

// Clone returns a deep copy of expr.
func (expr *CaseExpr) Clone() *CaseExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.Operand = CloneExpr(expr.Operand)
	other.Blocks = cloneCaseBlocks(expr.Blocks)
	other.ElseExpr = CloneExpr(expr.ElseExpr)
	return &other
}

// String returns the string representation of the expression.
func (expr *CaseExpr) String() string {
	var buf bytes.Buffer
	buf.WriteString("CASE")
	if expr.Operand != nil {
		buf.WriteString(" ")
		buf.WriteString(expr.Operand.String())
	}
	for _, blk := range expr.Blocks {
		buf.WriteString(" ")
		buf.WriteString(blk.String())
	}
	if expr.ElseExpr != nil {
		buf.WriteString(" ELSE ")
		buf.WriteString(expr.ElseExpr.String())
	}
	buf.WriteString(" END")
	return buf.String()
}

type CaseBlock struct {
	When      Pos  // position of WHEN keyword
	Condition Expr // block condition
	Then      Pos  // position of THEN keyword
	Body      Expr // result expression
}

// Clone returns a deep copy of blk.
func (blk *CaseBlock) Clone() *CaseBlock {
	if blk == nil {
		return nil
	}
	other := *blk
	other.Condition = CloneExpr(blk.Condition)
	other.Body = CloneExpr(blk.Body)
	return &other
}

func cloneCaseBlocks(a []*CaseBlock) []*CaseBlock {
	if a == nil {
		return nil
	}
	other := make([]*CaseBlock, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the block.
func (b *CaseBlock) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", b.Condition.String(), b.Body.String())
}

type Raise struct {
	Raise    Pos        // position of RAISE keyword
	Lparen   Pos        // position of left paren
	Ignore   Pos        // position of IGNORE keyword
	Rollback Pos        // position of ROLLBACK keyword
	Abort    Pos        // position of ABORT keyword
	Fail     Pos        // position of FAIL keyword
	Comma    Pos        // position of comma
	Error    *StringLit // error message
	Rparen   Pos        // position of right paren
}

// Clone returns a deep copy of r.
func (r *Raise) Clone() *Raise {
	if r == nil {
		return nil
	}
	other := *r
	other.Error = r.Error.Clone()
	return &other
}

// String returns the string representation of the raise function.
func (r *Raise) String() string {
	var buf bytes.Buffer
	buf.WriteString("RAISE(")
	if r.Rollback.IsValid() {
		fmt.Fprintf(&buf, "ROLLBACK, %s", r.Error.String())
	} else if r.Abort.IsValid() {
		fmt.Fprintf(&buf, "ABORT, %s", r.Error.String())
	} else if r.Fail.IsValid() {
		fmt.Fprintf(&buf, "FAIL, %s", r.Error.String())
	} else {
		buf.WriteString("IGNORE")
	}
	buf.WriteString(")")
	return buf.String()
}

type Exists struct {
	Not    Pos              // position of optional NOT keyword
	Exists Pos              // position of EXISTS keyword
	Lparen Pos              // position of left paren
	Select *SelectStatement // select statement
	Rparen Pos              // position of right paren
}

// Clone returns a deep copy of expr.
func (expr *Exists) Clone() *Exists {
	if expr == nil {
		return nil
	}
	other := *expr
	other.Select = expr.Select.Clone()
	return &other
}

// String returns the string representation of the expression.
func (expr *Exists) String() string {
	if expr.Not.IsValid() {
		return fmt.Sprintf("NOT EXISTS (%s)", expr.Select.String())
	}
	return fmt.Sprintf("EXISTS (%s)", expr.Select.String())
}

type ExprList struct {
	Lparen Pos    // position of left paren
	Exprs  []Expr // list of expressions
	Rparen Pos    // position of right paren
}

// Clone returns a deep copy of l.
func (l *ExprList) Clone() *ExprList {
	if l == nil {
		return nil
	}
	other := *l
	other.Exprs = cloneExprs(l.Exprs)
	return &other
}

func cloneExprLists(a []*ExprList) []*ExprList {
	if a == nil {
		return nil
	}
	other := make([]*ExprList, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the expression.
func (l *ExprList) String() string {
	var buf bytes.Buffer
	buf.WriteString("(")
	for i, expr := range l.Exprs {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(expr.String())
	}
	buf.WriteString(")")
	return buf.String()
}

type Range struct {
	X   Expr // lhs expression
	And Pos  // position of AND keyword
	Y   Expr // rhs expression
}

// Clone returns a deep copy of r.
func (r *Range) Clone() *Range {
	if r == nil {
		return nil
	}
	other := *r
	other.X = CloneExpr(r.X)
	other.Y = CloneExpr(r.Y)
	return &other
}

// String returns the string representation of the expression.
func (r *Range) String() string {
	return fmt.Sprintf("%s AND %s", r.X.String(), r.Y.String())
}

type QualifiedRef struct {
	Table  *Ident // table name
	Dot    Pos    // position of dot
	Star   Pos    // position of * (result column only)
	Column *Ident // column name
}

// Clone returns a deep copy of r.
func (r *QualifiedRef) Clone() *QualifiedRef {
	if r == nil {
		return nil
	}
	other := *r
	other.Table = r.Table.Clone()
	other.Column = r.Column.Clone()
	return &other
}

// String returns the string representation of the expression.
func (r *QualifiedRef) String() string {
	if r.Star.IsValid() {
		return fmt.Sprintf("%s.*", r.Table.String())
	}
	return fmt.Sprintf("%s.%s", r.Table.String(), r.Column.String())
}

type Call struct {
	Name     *Ident        // function name
	Lparen   Pos           // position of left paren
	Star     Pos           // position of *
	Distinct Pos           // position of DISTINCT keyword
	Args     []Expr        // argument list
	Rparen   Pos           // position of right paren
	Filter   *FilterClause // filter clause
	Over     *OverClause   // over clause
}

// Clone returns a deep copy of c.
func (c *Call) Clone() *Call {
	if c == nil {
		return nil
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Args = cloneExprs(c.Args)
	other.Filter = c.Filter.Clone()
	other.Over = c.Over.Clone()
	return &other
}

// String returns the string representation of the expression.
func (c *Call) String() string {
	var buf bytes.Buffer
	buf.WriteString(c.Name.Name)
	buf.WriteString("(")
	if c.Star.IsValid() {
		buf.WriteString("*")
	} else {
		if c.Distinct.IsValid() {
			buf.WriteString("DISTINCT")
			if len(c.Args) != 0 {
				buf.WriteString(" ")
			}
		}
		for i, arg := range c.Args {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(arg.String())
		}
	}
	buf.WriteString(")")

	if c.Filter != nil {
		buf.WriteString(" ")
		buf.WriteString(c.Filter.String())
	}

	if c.Over != nil {
		buf.WriteString(" ")
		buf.WriteString(c.Over.String())
	}

	return buf.String()
}

type FilterClause struct {
	Filter Pos  // position of FILTER keyword
	Lparen Pos  // position of left paren
	Where  Pos  // position of WHERE keyword
	X      Expr // filter expression
	Rparen Pos  // position of right paren
}

// Clone returns a deep copy of c.
func (c *FilterClause) Clone() *FilterClause {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneExpr(c.X)
	return &other
}

// String returns the string representation of the clause.
func (c *FilterClause) String() string {
	return fmt.Sprintf("FILTER (WHERE %s)", c.X.String())
}

type OverClause struct {
	Over       Pos               // position of OVER keyword
	Name       *Ident            // window name
	Definition *WindowDefinition // window definition
}

// Clone returns a deep copy of c.
func (c *OverClause) Clone() *OverClause {
	if c == nil {
		return nil
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Definition = c.Definition.Clone()
	return &other
}

// String returns the string representation of the clause.
func (c *OverClause) String() string {
	if c.Name != nil {
		return fmt.Sprintf("OVER %s", c.Name.String())
	}
	return fmt.Sprintf("OVER %s", c.Definition.String())
}

type OrderingTerm struct {
	X Expr // ordering expression

	Asc  Pos // position of ASC keyword
	Desc Pos // position of DESC keyword

	Nulls      Pos // position of NULLS keyword
	NullsFirst Pos // position of FIRST keyword
	NullsLast  Pos // position of LAST keyword
}

// Clone returns a deep copy of t.
func (t *OrderingTerm) Clone() *OrderingTerm {
	if t == nil {
		return nil
	}
	other := *t
	other.X = CloneExpr(t.X)
	return &other
}

func cloneOrderingTerms(a []*OrderingTerm) []*OrderingTerm {
	if a == nil {
		return nil
	}
	other := make([]*OrderingTerm, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the term.
func (t *OrderingTerm) String() string {
	var buf bytes.Buffer
	buf.WriteString(t.X.String())

	if t.Asc.IsValid() {
		buf.WriteString(" ASC")
	} else if t.Desc.IsValid() {
		buf.WriteString(" DESC")
	}

	if t.NullsFirst.IsValid() {
		buf.WriteString(" NULLS FIRST")
	} else if t.NullsLast.IsValid() {
		buf.WriteString(" NULLS LAST")
	}

	return buf.String()
}

type FrameSpec struct {
	Range  Pos // position of RANGE keyword
	Rows   Pos // position of ROWS keyword
	Groups Pos // position of GROUPS keyword

	Between Pos // position of BETWEEN keyword

	X           Expr // lhs expression
	UnboundedX  Pos  // position of lhs UNBOUNDED keyword
	PrecedingX  Pos  // position of lhs PRECEDING keyword
	CurrentX    Pos  // position of lhs CURRENT keyword
	CurrentRowX Pos  // position of lhs ROW keyword
	FollowingX  Pos  // position of lhs FOLLOWING keyword

	And Pos // position of AND keyword

	Y           Expr // lhs expression
	UnboundedY  Pos  // position of rhs UNBOUNDED keyword
	FollowingY  Pos  // position of rhs FOLLOWING keyword
	CurrentY    Pos  // position of rhs CURRENT keyword
	CurrentRowY Pos  // position of rhs ROW keyword
	PrecedingY  Pos  // position of rhs PRECEDING keyword

	Exclude           Pos // position of EXCLUDE keyword
	ExcludeNo         Pos // position of NO keyword after EXCLUDE
	ExcludeNoOthers   Pos // position of OTHERS keyword after EXCLUDE NO
	ExcludeCurrent    Pos // position of CURRENT keyword after EXCLUDE
	ExcludeCurrentRow Pos // position of ROW keyword after EXCLUDE CURRENT
	ExcludeGroup      Pos // position of GROUP keyword after EXCLUDE
	ExcludeTies       Pos // position of TIES keyword after EXCLUDE
}

// Clone returns a deep copy of s.
func (s *FrameSpec) Clone() *FrameSpec {
	if s == nil {
		return nil
	}
	other := *s
	other.X = CloneExpr(s.X)
	other.X = CloneExpr(s.Y)
	return &other
}

// String returns the string representation of the frame spec.
func (s *FrameSpec) String() string {
	var buf bytes.Buffer
	if s.Range.IsValid() {
		buf.WriteString("RANGE")
	} else if s.Rows.IsValid() {
		buf.WriteString("ROWS")
	} else if s.Groups.IsValid() {
		buf.WriteString("GROUPS")
	}

	if s.Between.IsValid() {
		buf.WriteString(" BETWEEN")
		if s.UnboundedX.IsValid() && s.PrecedingX.IsValid() {
			buf.WriteString(" UNBOUNDED PRECEDING")
		} else if s.X != nil && s.PrecedingX.IsValid() {
			fmt.Fprintf(&buf, " %s PRECEDING", s.X.String())
		} else if s.CurrentRowX.IsValid() {
			buf.WriteString(" CURRENT ROW")
		} else if s.X != nil && s.FollowingX.IsValid() {
			fmt.Fprintf(&buf, " %s FOLLOWING", s.X.String())
		}

		buf.WriteString(" AND")

		if s.Y != nil && s.PrecedingY.IsValid() {
			fmt.Fprintf(&buf, " %s PRECEDING", s.Y.String())
		} else if s.CurrentRowY.IsValid() {
			buf.WriteString(" CURRENT ROW")
		} else if s.Y != nil && s.FollowingY.IsValid() {
			fmt.Fprintf(&buf, " %s FOLLOWING", s.Y.String())
		} else if s.UnboundedY.IsValid() && s.FollowingY.IsValid() {
			buf.WriteString(" UNBOUNDED FOLLOWING")
		}
	} else {
		if s.UnboundedX.IsValid() && s.PrecedingX.IsValid() {
			buf.WriteString(" UNBOUNDED PRECEDING")
		} else if s.X != nil && s.PrecedingX.IsValid() {
			fmt.Fprintf(&buf, " %s PRECEDING", s.X.String())
		} else if s.CurrentRowX.IsValid() {
			buf.WriteString(" CURRENT ROW")
		}
	}

	if s.ExcludeNoOthers.IsValid() {
		buf.WriteString(" EXCLUDE NO OTHERS")
	} else if s.ExcludeCurrentRow.IsValid() {
		buf.WriteString(" EXCLUDE CURRENT ROW")
	} else if s.ExcludeGroup.IsValid() {
		buf.WriteString(" EXCLUDE GROUP")
	} else if s.ExcludeTies.IsValid() {
		buf.WriteString(" EXCLUDE TIES")
	}

	return buf.String()
}

type ColumnArg interface {
	Node
	columnArg()
}

type DropTableStatement struct {
	Drop     Pos    // position of DROP keyword
	Table    Pos    // position of TABLE keyword
	If       Pos    // position of IF keyword
	IfExists Pos    // position of EXISTS keyword after IF
	Name     *Ident // view name
}

// Clone returns a deep copy of s.
func (s *DropTableStatement) Clone() *DropTableStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *DropTableStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP TABLE")
	if s.IfExists.IsValid() {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type CreateViewStatement struct {
	Create      Pos              // position of CREATE keyword
	View        Pos              // position of VIEW keyword
	If          Pos              // position of IF keyword
	IfNot       Pos              // position of NOT keyword after IF
	IfNotExists Pos              // position of EXISTS keyword after IF NOT
	Name        *Ident           // view name
	Lparen      Pos              // position of column list left paren
	Columns     []*Ident         // column list
	Rparen      Pos              // position of column list right paren
	As          Pos              // position of AS keyword
	Select      *SelectStatement // source statement
}

// Clone returns a deep copy of s.
func (s *CreateViewStatement) Clone() *CreateViewStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	other.Columns = cloneIdents(s.Columns)
	other.Select = s.Select.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *CreateViewStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE VIEW")
	if s.IfNotExists.IsValid() {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())

	if len(s.Columns) > 0 {
		buf.WriteString(" (")
		for i, col := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " AS %s", s.Select.String())

	return buf.String()
}

type DropViewStatement struct {
	Drop     Pos    // position of DROP keyword
	View     Pos    // position of VIEW keyword
	If       Pos    // position of IF keyword
	IfExists Pos    // position of EXISTS keyword after IF
	Name     *Ident // view name
}

// Clone returns a deep copy of s.
func (s *DropViewStatement) Clone() *DropViewStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *DropViewStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP VIEW")
	if s.IfExists.IsValid() {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type CreateIndexStatement struct {
	Create      Pos              // position of CREATE keyword
	Unique      Pos              // position of optional UNIQUE keyword
	Index       Pos              // position of INDEX keyword
	If          Pos              // position of IF keyword
	IfNot       Pos              // position of NOT keyword after IF
	IfNotExists Pos              // position of EXISTS keyword after IF NOT
	Name        *Ident           // index name
	On          Pos              // position of ON keyword
	Table       *Ident           // index name
	Lparen      Pos              // position of column list left paren
	Columns     []*IndexedColumn // column list
	Rparen      Pos              // position of column list right paren
	Where       Pos              // position of WHERE keyword
	WhereExpr   Expr             // conditional expression
}

// Clone returns a deep copy of s.
func (s *CreateIndexStatement) Clone() *CreateIndexStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	other.Table = s.Table.Clone()
	other.Columns = cloneIndexedColumns(s.Columns)
	other.WhereExpr = CloneExpr(s.WhereExpr)
	return &other
}

// String returns the string representation of the statement.
func (s *CreateIndexStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE")
	if s.Unique.IsValid() {
		buf.WriteString(" UNIQUE")
	}
	buf.WriteString(" INDEX")
	if s.IfNotExists.IsValid() {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s ON %s ", s.Name.String(), s.Table.String())

	buf.WriteString("(")
	for i, col := range s.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	buf.WriteString(")")

	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
	}

	return buf.String()
}

type DropIndexStatement struct {
	Drop     Pos    // position of DROP keyword
	Index    Pos    // position of INDEX keyword
	If       Pos    // position of IF keyword
	IfExists Pos    // position of EXISTS keyword after IF
	Name     *Ident // index name
}

// Clone returns a deep copy of s.
func (s *DropIndexStatement) Clone() *DropIndexStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *DropIndexStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP INDEX")
	if s.IfExists.IsValid() {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type CreateTriggerStatement struct {
	Create      Pos    // position of CREATE keyword
	Trigger     Pos    // position of TRIGGER keyword
	If          Pos    // position of IF keyword
	IfNot       Pos    // position of NOT keyword after IF
	IfNotExists Pos    // position of EXISTS keyword after IF NOT
	Name        *Ident // index name

	Before    Pos // position of BEFORE keyword
	After     Pos // position of AFTER keyword
	Instead   Pos // position of INSTEAD keyword
	InsteadOf Pos // position of OF keyword after INSTEAD

	Delete          Pos      // position of DELETE keyword
	Insert          Pos      // position of INSERT keyword
	Update          Pos      // position of UPDATE keyword
	UpdateOf        Pos      // position of OF keyword after UPDATE
	UpdateOfColumns []*Ident // columns list for UPDATE OF
	On              Pos      // position of ON keyword
	Table           *Ident   // table name

	For        Pos // position of FOR keyword
	ForEach    Pos // position of EACH keyword after FOR
	ForEachRow Pos // position of ROW keyword after FOR EACH

	When     Pos  // position of WHEN keyword
	WhenExpr Expr // conditional expression

	Begin Pos         // position of BEGIN keyword
	Body  []Statement // trigger body
	End   Pos         // position of END keyword
}

// Clone returns a deep copy of s.
func (s *CreateTriggerStatement) Clone() *CreateTriggerStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	other.UpdateOfColumns = cloneIdents(s.UpdateOfColumns)
	other.Table = s.Table.Clone()
	other.WhenExpr = CloneExpr(s.WhenExpr)
	other.Body = cloneStatements(s.Body)
	return &other
}

// String returns the string representation of the statement.
func (s *CreateTriggerStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE TRIGGER")
	if s.IfNotExists.IsValid() {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())

	if s.Before.IsValid() {
		buf.WriteString(" BEFORE")
	} else if s.After.IsValid() {
		buf.WriteString(" AFTER")
	} else if s.InsteadOf.IsValid() {
		buf.WriteString(" INSTEAD OF")
	}

	if s.Delete.IsValid() {
		buf.WriteString(" DELETE")
	} else if s.Insert.IsValid() {
		buf.WriteString(" INSERT")
	} else if s.Update.IsValid() {
		buf.WriteString(" UPDATE")
		if s.UpdateOf.IsValid() {
			buf.WriteString(" OF ")
			for i, col := range s.UpdateOfColumns {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(col.String())
			}
		}
	}

	fmt.Fprintf(&buf, " ON %s", s.Table.String())

	if s.ForEachRow.IsValid() {
		buf.WriteString(" FOR EACH ROW")
	}

	if s.WhenExpr != nil {
		fmt.Fprintf(&buf, " WHEN %s", s.WhenExpr.String())
	}

	buf.WriteString(" BEGIN")
	for i := range s.Body {
		fmt.Fprintf(&buf, " %s;", s.Body[i].String())
	}
	buf.WriteString(" END")

	return buf.String()
}

type DropTriggerStatement struct {
	Drop     Pos    // position of DROP keyword
	Trigger  Pos    // position of TRIGGER keyword
	If       Pos    // position of IF keyword
	IfExists Pos    // position of EXISTS keyword after IF
	Name     *Ident // trigger name
}

// Clone returns a deep copy of s.
func (s *DropTriggerStatement) Clone() *DropTriggerStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *DropTriggerStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP TRIGGER")
	if s.IfExists.IsValid() {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type InsertStatement struct {
	WithClause *WithClause // clause containing CTEs

	Insert           Pos // position of INSERT keyword
	Replace          Pos // position of REPLACE keyword
	InsertOr         Pos // position of OR keyword after INSERT
	InsertOrReplace  Pos // position of REPLACE keyword after INSERT OR
	InsertOrRollback Pos // position of ROLLBACK keyword after INSERT OR
	InsertOrAbort    Pos // position of ABORT keyword after INSERT OR
	InsertOrFail     Pos // position of FAIL keyword after INSERT OR
	InsertOrIgnore   Pos // position of IGNORE keyword after INSERT OR
	Into             Pos // position of INTO keyword

	Table *Ident // table name
	As    Pos    // position of AS keyword
	Alias *Ident // optional alias

	ColumnsLparen Pos      // position of column list left paren
	Columns       []*Ident // optional column list
	ColumnsRparen Pos      // position of column list right paren

	Values     Pos         // position of VALUES keyword
	ValueLists []*ExprList // lists of lists of values

	Select *SelectStatement // SELECT statement

	Default       Pos // position of DEFAULT keyword
	DefaultValues Pos // position of VALUES keyword after DEFAULT

	UpsertClause *UpsertClause // optional upsert clause
}

// Clone returns a deep copy of s.
func (s *InsertStatement) Clone() *InsertStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.WithClause = s.WithClause.Clone()
	other.Table = s.Table.Clone()
	other.Alias = s.Alias.Clone()
	other.Columns = cloneIdents(s.Columns)
	other.ValueLists = cloneExprLists(s.ValueLists)
	other.Select = s.Select.Clone()
	other.UpsertClause = s.UpsertClause.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *InsertStatement) String() string {
	var buf bytes.Buffer
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	if s.Replace.IsValid() {
		buf.WriteString("REPLACE")
	} else {
		buf.WriteString("INSERT")
		if s.InsertOrReplace.IsValid() {
			buf.WriteString(" OR REPLACE")
		} else if s.InsertOrRollback.IsValid() {
			buf.WriteString(" OR ROLLBACK")
		} else if s.InsertOrAbort.IsValid() {
			buf.WriteString(" OR ABORT")
		} else if s.InsertOrFail.IsValid() {
			buf.WriteString(" OR FAIL")
		} else if s.InsertOrIgnore.IsValid() {
			buf.WriteString(" OR IGNORE")
		}
	}

	fmt.Fprintf(&buf, " INTO %s", s.Table.String())
	if s.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", s.Alias.String())
	}

	if len(s.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	if s.DefaultValues.IsValid() {
		buf.WriteString(" DEFAULT VALUES")
	} else if s.Select != nil {
		fmt.Fprintf(&buf, " %s", s.Select.String())
	} else {
		buf.WriteString(" VALUES")
		for i := range s.ValueLists {
			if i != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(" (")
			for j, expr := range s.ValueLists[i].Exprs {
				if j != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(expr.String())
			}
			buf.WriteString(")")
		}
	}

	if s.UpsertClause != nil {
		fmt.Fprintf(&buf, " %s", s.UpsertClause.String())
	}

	return buf.String()
}

type UpsertClause struct {
	On         Pos // position of ON keyword
	OnConflict Pos // position of CONFLICT keyword after ON

	Lparen    Pos              // position of column list left paren
	Columns   []*IndexedColumn // optional indexed column list
	Rparen    Pos              // position of column list right paren
	Where     Pos              // position of WHERE keyword
	WhereExpr Expr             // optional conditional expression

	Do              Pos           // position of DO keyword
	DoNothing       Pos           // position of NOTHING keyword after DO
	DoUpdate        Pos           // position of UPDATE keyword after DO
	DoUpdateSet     Pos           // position of SET keyword after DO UPDATE
	Assignments     []*Assignment // list of column assignments
	UpdateWhere     Pos           // position of WHERE keyword for DO UPDATE SET
	UpdateWhereExpr Expr          // optional conditional expression for DO UPDATE SET
}

// Clone returns a deep copy of c.
func (c *UpsertClause) Clone() *UpsertClause {
	if c == nil {
		return nil
	}
	other := *c
	other.Columns = cloneIndexedColumns(c.Columns)
	other.WhereExpr = CloneExpr(c.WhereExpr)
	other.Assignments = cloneAssignments(c.Assignments)
	other.UpdateWhereExpr = CloneExpr(c.UpdateWhereExpr)
	return &other
}

// String returns the string representation of the clause.
func (c *UpsertClause) String() string {
	var buf bytes.Buffer
	buf.WriteString("ON CONFLICT")

	if len(c.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")

		if c.WhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", c.WhereExpr.String())
		}
	}

	buf.WriteString(" DO")
	if c.DoNothing.IsValid() {
		buf.WriteString(" NOTHING")
	} else {
		buf.WriteString(" UPDATE SET ")
		for i := range c.Assignments {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Assignments[i].String())
		}

		if c.UpdateWhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", c.UpdateWhereExpr.String())
		}
	}

	return buf.String()
}

type UpdateStatement struct {
	WithClause *WithClause // clause containing CTEs

	Update           Pos // position of UPDATE keyword
	UpdateOr         Pos // position of OR keyword after UPDATE
	UpdateOrReplace  Pos // position of REPLACE keyword after UPDATE OR
	UpdateOrRollback Pos // position of ROLLBACK keyword after UPDATE OR
	UpdateOrAbort    Pos // position of ABORT keyword after UPDATE OR
	UpdateOrFail     Pos // position of FAIL keyword after UPDATE OR
	UpdateOrIgnore   Pos // position of IGNORE keyword after UPDATE OR

	Table *QualifiedTableName // table name

	Set         Pos           // position of SET keyword
	Assignments []*Assignment // list of column assignments
	Where       Pos           // position of WHERE keyword
	WhereExpr   Expr          // conditional expression
}

// Clone returns a deep copy of s.
func (s *UpdateStatement) Clone() *UpdateStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.WithClause = s.WithClause.Clone()
	other.Table = s.Table.Clone()
	other.Assignments = cloneAssignments(s.Assignments)
	other.WhereExpr = CloneExpr(s.WhereExpr)
	return &other
}

// String returns the string representation of the clause.
func (s *UpdateStatement) String() string {
	var buf bytes.Buffer
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	buf.WriteString("UPDATE")
	if s.UpdateOrRollback.IsValid() {
		buf.WriteString(" OR ROLLBACK")
	} else if s.UpdateOrAbort.IsValid() {
		buf.WriteString(" OR ABORT")
	} else if s.UpdateOrReplace.IsValid() {
		buf.WriteString(" OR REPLACE")
	} else if s.UpdateOrFail.IsValid() {
		buf.WriteString(" OR FAIL")
	} else if s.UpdateOrIgnore.IsValid() {
		buf.WriteString(" OR IGNORE")
	}

	fmt.Fprintf(&buf, " %s ", s.Table.String())

	buf.WriteString("SET ")
	for i := range s.Assignments {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(s.Assignments[i].String())
	}

	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
	}

	return buf.String()
}

type DeleteStatement struct {
	WithClause *WithClause         // clause containing CTEs
	Delete     Pos                 // position of UPDATE keyword
	From       Pos                 // position of FROM keyword
	Table      *QualifiedTableName // table name

	Where     Pos  // position of WHERE keyword
	WhereExpr Expr // conditional expression

	Order         Pos             // position of ORDER keyword
	OrderBy       Pos             // position of BY keyword after ORDER
	OrderingTerms []*OrderingTerm // terms of ORDER BY clause

	Limit       Pos  // position of LIMIT keyword
	LimitExpr   Expr // limit expression
	Offset      Pos  // position of OFFSET keyword
	OffsetComma Pos  // position of COMMA (instead of OFFSET)
	OffsetExpr  Expr // offset expression
}

// Clone returns a deep copy of s.
func (s *DeleteStatement) Clone() *DeleteStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.WithClause = s.WithClause.Clone()
	other.Table = s.Table.Clone()
	other.WhereExpr = CloneExpr(s.WhereExpr)
	other.OrderingTerms = cloneOrderingTerms(s.OrderingTerms)
	other.LimitExpr = CloneExpr(s.LimitExpr)
	other.OffsetExpr = CloneExpr(s.OffsetExpr)
	return &other
}

// String returns the string representation of the clause.
func (s *DeleteStatement) String() string {
	var buf bytes.Buffer
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	fmt.Fprintf(&buf, "DELETE FROM %s", s.Table.String())
	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
	}

	// Write ORDER BY.
	if len(s.OrderingTerms) != 0 {
		buf.WriteString(" ORDER BY ")
		for i, term := range s.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	// Write LIMIT/OFFSET.
	if s.LimitExpr != nil {
		fmt.Fprintf(&buf, " LIMIT %s", s.LimitExpr.String())
		if s.OffsetExpr != nil {
			fmt.Fprintf(&buf, " OFFSET %s", s.OffsetExpr.String())
		}
	}

	return buf.String()
}

// Assignment is used within the UPDATE statement & upsert clause.
// It is similiar to an expression except that it must be an equality.
type Assignment struct {
	Lparen  Pos      // position of column list left paren
	Columns []*Ident // column list
	Rparen  Pos      // position of column list right paren
	Eq      Pos      // position of =
	Expr    Expr     // assigned expression
}

// Clone returns a deep copy of a.
func (a *Assignment) Clone() *Assignment {
	if a == nil {
		return nil
	}
	other := *a
	other.Columns = cloneIdents(a.Columns)
	other.Expr = CloneExpr(a.Expr)
	return &other
}

func cloneAssignments(a []*Assignment) []*Assignment {
	if a == nil {
		return nil
	}
	other := make([]*Assignment, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the clause.
func (a *Assignment) String() string {
	var buf bytes.Buffer
	if len(a.Columns) == 1 {
		buf.WriteString(a.Columns[0].String())
	} else if len(a.Columns) > 1 {
		buf.WriteString("(")
		for i, col := range a.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " = %s", a.Expr.String())
	return buf.String()
}

type IndexedColumn struct {
	X    Expr // column expression
	Asc  Pos  // position of optional ASC keyword
	Desc Pos  // position of optional DESC keyword
}

// Clone returns a deep copy of c.
func (c *IndexedColumn) Clone() *IndexedColumn {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneExpr(c.X)
	return &other
}

func cloneIndexedColumns(a []*IndexedColumn) []*IndexedColumn {
	if a == nil {
		return nil
	}
	other := make([]*IndexedColumn, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the column.
func (c *IndexedColumn) String() string {
	if c.Asc.IsValid() {
		return fmt.Sprintf("%s ASC", c.X.String())
	} else if c.Desc.IsValid() {
		return fmt.Sprintf("%s DESC", c.X.String())
	}
	return c.X.String()
}

type SelectStatement struct {
	WithClause *WithClause // clause containing CTEs

	Values     Pos         // position of VALUES keyword
	ValueLists []*ExprList // lists of lists of values

	Select   Pos             // position of SELECT keyword
	Distinct Pos             // position of DISTINCT keyword
	All      Pos             // position of ALL keyword
	Columns  []*ResultColumn // list of result columns in the SELECT clause

	From   Pos    // position of FROM keyword
	Source Source // chain of tables & subqueries in FROM clause

	Where     Pos  // position of WHERE keyword
	WhereExpr Expr // condition for WHERE clause

	Group        Pos    // position of GROUP keyword
	GroupBy      Pos    // position of BY keyword after GROUP
	GroupByExprs []Expr // group by expression list
	Having       Pos    // position of HAVING keyword
	HavingExpr   Expr   // HAVING expression

	Window  Pos       // position of WINDOW keyword
	Windows []*Window // window list

	Union     Pos              // position of UNION keyword
	UnionAll  Pos              // position of ALL keyword after UNION
	Intersect Pos              // position of INTERSECT keyword
	Except    Pos              // position of EXCEPT keyword
	Compound  *SelectStatement // compounded SELECT statement

	Order         Pos             // position of ORDER keyword
	OrderBy       Pos             // position of BY keyword after ORDER
	OrderingTerms []*OrderingTerm // terms of ORDER BY clause

	Limit       Pos  // position of LIMIT keyword
	LimitExpr   Expr // limit expression
	Offset      Pos  // position of OFFSET keyword
	OffsetComma Pos  // position of COMMA (instead of OFFSET)
	OffsetExpr  Expr // offset expression
}

// Clone returns a deep copy of s.
func (s *SelectStatement) Clone() *SelectStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.WithClause = s.WithClause.Clone()
	other.ValueLists = cloneExprLists(s.ValueLists)
	other.Columns = cloneResultColumns(s.Columns)
	other.Source = CloneSource(s.Source)
	other.WhereExpr = CloneExpr(s.WhereExpr)
	other.GroupByExprs = cloneExprs(s.GroupByExprs)
	other.HavingExpr = CloneExpr(s.HavingExpr)
	other.Windows = cloneWindows(s.Windows)
	other.Compound = s.Compound.Clone()
	other.OrderingTerms = cloneOrderingTerms(s.OrderingTerms)
	other.LimitExpr = CloneExpr(s.LimitExpr)
	other.OffsetExpr = CloneExpr(s.OffsetExpr)
	return &other
}

// String returns the string representation of the statement.
func (s *SelectStatement) String() string {
	var buf bytes.Buffer
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	if len(s.ValueLists) > 0 {
		buf.WriteString("VALUES ")
		for i, exprs := range s.ValueLists {
			if i != 0 {
				buf.WriteString(", ")
			}

			buf.WriteString("(")
			for j, expr := range exprs.Exprs {
				if j != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(expr.String())
			}
			buf.WriteString(")")
		}
	} else {
		buf.WriteString("SELECT ")
		if s.Distinct.IsValid() {
			buf.WriteString("DISTINCT ")
		} else if s.All.IsValid() {
			buf.WriteString("ALL ")
		}

		for i, col := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}

		if s.Source != nil {
			fmt.Fprintf(&buf, " FROM %s", s.Source.String())
		}

		if s.WhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
		}

		if len(s.GroupByExprs) != 0 {
			buf.WriteString(" GROUP BY ")
			for i, expr := range s.GroupByExprs {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(expr.String())
			}

			if s.HavingExpr != nil {
				fmt.Fprintf(&buf, " HAVING %s", s.HavingExpr.String())
			}
		}

		if len(s.Windows) != 0 {
			buf.WriteString(" WINDOW ")
			for i, window := range s.Windows {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(window.String())
			}
		}
	}

	// Write compound operator.
	if s.Compound != nil {
		switch {
		case s.Union.IsValid():
			buf.WriteString(" UNION")
			if s.UnionAll.IsValid() {
				buf.WriteString(" ALL")
			}
		case s.Intersect.IsValid():
			buf.WriteString(" INTERSECT")
		case s.Except.IsValid():
			buf.WriteString(" EXCEPT")
		}

		fmt.Fprintf(&buf, " %s", s.Compound.String())
	}

	// Write ORDER BY.
	if len(s.OrderingTerms) != 0 {
		buf.WriteString(" ORDER BY ")
		for i, term := range s.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	// Write LIMIT/OFFSET.
	if s.LimitExpr != nil {
		fmt.Fprintf(&buf, " LIMIT %s", s.LimitExpr.String())
		if s.OffsetExpr != nil {
			fmt.Fprintf(&buf, " OFFSET %s", s.OffsetExpr.String())
		}
	}

	return buf.String()
}

type ResultColumn struct {
	Star  Pos    // position of *
	Expr  Expr   // column expression (may be "tbl.*")
	As    Pos    // position of AS keyword
	Alias *Ident // alias name
}

// Clone returns a deep copy of c.
func (c *ResultColumn) Clone() *ResultColumn {
	if c == nil {
		return nil
	}
	other := *c
	other.Expr = CloneExpr(c.Expr)
	other.Alias = c.Alias.Clone()
	return &other
}

func cloneResultColumns(a []*ResultColumn) []*ResultColumn {
	if a == nil {
		return nil
	}
	other := make([]*ResultColumn, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the column.
func (c *ResultColumn) String() string {
	if c.Star.IsValid() {
		return "*"
	} else if c.Alias != nil {
		return fmt.Sprintf("%s AS %s", c.Expr.String(), c.Alias.String())
	}
	return c.Expr.String()
}

type QualifiedTableName struct {
	Name       *Ident // table name
	As         Pos    // position of AS keyword
	Alias      *Ident // optional table alias
	Indexed    Pos    // position of INDEXED keyword
	IndexedBy  Pos    // position of BY keyword after INDEXED
	Not        Pos    // position of NOT keyword before INDEXED
	NotIndexed Pos    // position of NOT keyword before INDEXED
	Index      *Ident // name of index
}

// TableName returns the name used to identify n.
// Returns the alias, if one is specified. Otherwise returns the name.
func (n *QualifiedTableName) TableName() string {
	if s := IdentName(n.Alias); s != "" {
		return s
	}
	return IdentName(n.Name)
}

// Clone returns a deep copy of n.
func (n *QualifiedTableName) Clone() *QualifiedTableName {
	if n == nil {
		return nil
	}
	other := *n
	other.Name = n.Name.Clone()
	other.Alias = n.Alias.Clone()
	other.Index = n.Index.Clone()
	return &other
}

// String returns the string representation of the table name.
func (n *QualifiedTableName) String() string {
	var buf bytes.Buffer
	buf.WriteString(n.Name.String())
	if n.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", n.Alias.String())
	}

	if n.Index != nil {
		fmt.Fprintf(&buf, " INDEXED BY %s", n.Index.String())
	} else if n.NotIndexed.IsValid() {
		buf.WriteString(" NOT INDEXED")
	}
	return buf.String()
}

type ParenSource struct {
	Lparen Pos    // position of left paren
	X      Source // nested source
	Rparen Pos    // position of right paren
	As     Pos    // position of AS keyword (select source only)
	Alias  *Ident // optional table alias (select source only)
}

// Clone returns a deep copy of s.
func (s *ParenSource) Clone() *ParenSource {
	if s == nil {
		return nil
	}
	other := *s
	other.X = CloneSource(s.X)
	other.Alias = s.Alias.Clone()
	return &other
}

// String returns the string representation of the source.
func (s *ParenSource) String() string {
	if s.Alias != nil {
		return fmt.Sprintf("(%s) AS %s", s.X.String(), s.Alias.String())
	}
	return fmt.Sprintf("(%s)", s.X.String())
}

type JoinClause struct {
	X          Source         // lhs source
	Operator   *JoinOperator  // join operator
	Y          Source         // rhs source
	Constraint JoinConstraint // join constraint
}

// Clone returns a deep copy of c.
func (c *JoinClause) Clone() *JoinClause {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneSource(c.X)
	other.Y = CloneSource(c.Y)
	other.Constraint = CloneJoinConstraint(c.Constraint)
	return &other
}

// String returns the string representation of the clause.
func (c *JoinClause) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s%s%s", c.X.String(), c.Operator.String(), c.Y.String())
	if c.Constraint != nil {
		fmt.Fprintf(&buf, " %s", c.Constraint.String())
	}
	return buf.String()
}

type JoinOperator struct {
	Comma   Pos // position of comma
	Natural Pos // position of NATURAL keyword
	Left    Pos // position of LEFT keyword
	Outer   Pos // position of OUTER keyword
	Inner   Pos // position of INNER keyword
	Cross   Pos // position of CROSS keyword
	Join    Pos // position of JOIN keyword
}

// Clone returns a deep copy of op.
func (op *JoinOperator) Clone() *JoinOperator {
	if op == nil {
		return nil
	}
	other := *op
	return &other
}

// String returns the string representation of the operator.
func (op *JoinOperator) String() string {
	if op.Comma.IsValid() {
		return ", "
	}

	var buf bytes.Buffer
	if op.Natural.IsValid() {
		buf.WriteString(" NATURAL")
	}
	if op.Left.IsValid() {
		buf.WriteString(" LEFT")
		if op.Outer.IsValid() {
			buf.WriteString(" OUTER")
		}
	} else if op.Inner.IsValid() {
		buf.WriteString(" INNER")
	} else if op.Cross.IsValid() {
		buf.WriteString(" CROSS")
	}
	buf.WriteString(" JOIN ")

	return buf.String()
}

type OnConstraint struct {
	On Pos  // position of ON keyword
	X  Expr // constraint expression
}

// Clone returns a deep copy of c.
func (c *OnConstraint) Clone() *OnConstraint {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneExpr(c.X)
	return &other
}

// String returns the string representation of the constraint.
func (c *OnConstraint) String() string {
	return "ON " + c.X.String()
}

type UsingConstraint struct {
	Using   Pos      // position of USING keyword
	Lparen  Pos      // position of left paren
	Columns []*Ident // column list
	Rparen  Pos      // position of right paren
}

// Clone returns a deep copy of c.
func (c *UsingConstraint) Clone() *UsingConstraint {
	if c == nil {
		return nil
	}
	other := *c
	other.Columns = cloneIdents(c.Columns)
	return &other
}

// String returns the string representation of the constraint.
func (c *UsingConstraint) String() string {
	var buf bytes.Buffer
	buf.WriteString("USING (")
	for i, col := range c.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	buf.WriteString(")")
	return buf.String()
}

type WithClause struct {
	With      Pos    // position of WITH keyword
	Recursive Pos    // position of RECURSIVE keyword
	CTEs      []*CTE // common table expressions
}

// Clone returns a deep copy of c.
func (c *WithClause) Clone() *WithClause {
	if c == nil {
		return nil
	}
	other := *c
	other.CTEs = cloneCTEs(c.CTEs)
	return &other
}

// String returns the string representation of the clause.
func (c *WithClause) String() string {
	var buf bytes.Buffer
	buf.WriteString("WITH ")
	if c.Recursive.IsValid() {
		buf.WriteString("RECURSIVE ")
	}

	for i, cte := range c.CTEs {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(cte.String())
	}

	return buf.String()
}

// CTE represents an AST node for a common table expression.
type CTE struct {
	TableName     *Ident           // table name
	ColumnsLparen Pos              // position of column list left paren
	Columns       []*Ident         // optional column list
	ColumnsRparen Pos              // position of column list right paren
	As            Pos              // position of AS keyword
	SelectLparen  Pos              // position of select left paren
	Select        *SelectStatement // select statement
	SelectRparen  Pos              // position of select right paren
}

// Clone returns a deep copy of cte.
func (cte *CTE) Clone() *CTE {
	if cte == nil {
		return nil
	}
	other := *cte
	other.TableName = cte.TableName.Clone()
	other.Columns = cloneIdents(cte.Columns)
	other.Select = cte.Select.Clone()
	return &other
}

func cloneCTEs(a []*CTE) []*CTE {
	if a == nil {
		return nil
	}
	other := make([]*CTE, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the CTE.
func (cte *CTE) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s", cte.TableName.String())

	if len(cte.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range cte.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " AS (%s)", cte.Select.String())

	return buf.String()
}

type ParenExpr struct {
	Lparen Pos  // position of left paren
	X      Expr // parenthesized expression
	Rparen Pos  // position of right paren
}

// Clone returns a deep copy of expr.
func (expr *ParenExpr) Clone() *ParenExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	return &other
}

// String returns the string representation of the expression.
func (expr *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", expr.X.String())
}

type Window struct {
	Name       *Ident            // name of window
	As         Pos               // position of AS keyword
	Definition *WindowDefinition // window definition
}

// Clone returns a deep copy of w.
func (w *Window) Clone() *Window {
	if w == nil {
		return nil
	}
	other := *w
	other.Name = w.Name.Clone()
	other.Definition = w.Definition.Clone()
	return &other
}

func cloneWindows(a []*Window) []*Window {
	if a == nil {
		return nil
	}
	other := make([]*Window, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the window.
func (w *Window) String() string {
	return fmt.Sprintf("%s AS %s", w.Name.String(), w.Definition.String())
}

type WindowDefinition struct {
	Lparen        Pos             // position of left paren
	Base          *Ident          // base window name
	Partition     Pos             // position of PARTITION keyword
	PartitionBy   Pos             // position of BY keyword (after PARTITION)
	Partitions    []Expr          // partition expressions
	Order         Pos             // position of ORDER keyword
	OrderBy       Pos             // position of BY keyword (after ORDER)
	OrderingTerms []*OrderingTerm // ordering terms
	Frame         *FrameSpec      // frame
	Rparen        Pos             // position of right paren
}

// Clone returns a deep copy of d.
func (d *WindowDefinition) Clone() *WindowDefinition {
	if d == nil {
		return nil
	}
	other := *d
	other.Base = d.Base.Clone()
	other.Partitions = cloneExprs(d.Partitions)
	other.OrderingTerms = cloneOrderingTerms(d.OrderingTerms)
	other.Frame = d.Frame.Clone()
	return &other
}

// String returns the string representation of the window definition.
func (d *WindowDefinition) String() string {
	var buf bytes.Buffer
	buf.WriteString("(")
	if d.Base != nil {
		buf.WriteString(d.Base.String())
	}

	if len(d.Partitions) != 0 {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString("PARTITION BY ")

		for i, p := range d.Partitions {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(p.String())
		}
	}

	if len(d.OrderingTerms) != 0 {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString("ORDER BY ")

		for i, term := range d.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	if d.Frame != nil {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString(d.Frame.String())
	}

	buf.WriteString(")")

	return buf.String()
}
