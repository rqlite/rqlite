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

func (*ExplainStatement) node()       {}
func (*BeginStatement) node()         {}
func (*CommitStatement) node()        {}
func (*RollbackStatement) node()      {}
func (*SavepointStatement) node()     {}
func (*ReleaseStatement) node()       {}
func (*CreateTableStatement) node()   {}
func (*AlterTableStatement) node()    {}
func (*AnalyzeStatement) node()       {}
func (*CreateViewStatement) node()    {}
func (*DropTableStatement) node()     {}
func (*DropViewStatement) node()      {}
func (*DropIndexStatement) node()     {}
func (*DropTriggerStatement) node()   {}
func (*CreateIndexStatement) node()   {}
func (*CreateTriggerStatement) node() {}
func (*SelectStatement) node()        {}
func (*InsertStatement) node()        {}
func (*UpdateStatement) node()        {}
func (*DeleteStatement) node()        {}
func (*PrimaryKeyConstraint) node()   {}
func (*NotNullConstraint) node()      {}
func (*UniqueConstraint) node()       {}
func (*CheckConstraint) node()        {}
func (*DefaultConstraint) node()      {}
func (*CollateConstraint) node()      {}
func (*ForeignKeyConstraint) node()   {}
func (*Ident) node()                  {}
func (*StringLit) node()              {}
func (*NumberLit) node()              {}
func (*BlobLit) node()                {}
func (*BoolLit) node()                {}
func (*NullLit) node()                {}
func (*BindExpr) node()               {}
func (*ParenExpr) node()              {}
func (*UnaryExpr) node()              {}
func (*BinaryExpr) node()             {}
func (*CastExpr) node()               {}
func (*CaseExpr) node()               {}
func (*ExprList) node()               {}
func (*QualifiedRef) node()           {}
func (*Call) node()                   {}
func (*FilterClause) node()           {}
func (*OverClause) node()             {}
func (*OrderingTerm) node()           {}
func (*FrameSpec) node()              {}
func (*CollateExpr) node()            {}
func (*Range) node()                  {}
func (*Raise) node()                  {}
func (*Exists) node()                 {}
func (*ParenSource) node()            {}
func (*QualifiedTableName) node()     {}
func (*JoinClause) node()             {}
func (*JoinOperator) node()           {}
func (*OnConstraint) node()           {}
func (*UsingConstraint) node()        {}

type Statement interface {
	Node
	stmt()
}

func (*ExplainStatement) stmt()       {}
func (*BeginStatement) stmt()         {}
func (*CommitStatement) stmt()        {}
func (*RollbackStatement) stmt()      {}
func (*SavepointStatement) stmt()     {}
func (*ReleaseStatement) stmt()       {}
func (*CreateTableStatement) stmt()   {}
func (*AnalyzeStatement) stmt()       {}
func (*AlterTableStatement) stmt()    {}
func (*CreateViewStatement) stmt()    {}
func (*DropTableStatement) stmt()     {}
func (*DropViewStatement) stmt()      {}
func (*DropIndexStatement) stmt()     {}
func (*DropTriggerStatement) stmt()   {}
func (*CreateIndexStatement) stmt()   {}
func (*CreateTriggerStatement) stmt() {}
func (*SelectStatement) stmt()        {}
func (*InsertStatement) stmt()        {}
func (*UpdateStatement) stmt()        {}
func (*DeleteStatement) stmt()        {}

type Expr interface {
	Node
	expr()
}

func (*Ident) expr()        {}
func (*StringLit) expr()    {}
func (*NumberLit) expr()    {}
func (*BlobLit) expr()      {}
func (*BoolLit) expr()      {}
func (*NullLit) expr()      {}
func (*BindExpr) expr()     {}
func (*ParenExpr) expr()    {}
func (*UnaryExpr) expr()    {}
func (*BinaryExpr) expr()   {}
func (*CastExpr) expr()     {}
func (*CaseExpr) expr()     {}
func (*ExprList) expr()     {}
func (*QualifiedRef) expr() {}
func (*Call) expr()         {}
func (*CollateExpr) expr()  {}
func (*Range) expr()        {}
func (*Raise) expr()        {}
func (*Exists) expr()       {}

// ExprString returns the string representation of expr.
// Returns a blank string if expr is nil.
func ExprString(expr Expr) string {
	if expr == nil {
		return ""
	}
	return expr.String()
}

// Source represents a table or subquery.
type Source interface {
	Node
	source()
}

func (*QualifiedTableName) source() {}
func (*SelectStatement) source()    {}
func (*ParenSource) source()        {}
func (*JoinClause) source()         {}

// JoinConstraint represents either an ON or USING join constraint.
type JoinConstraint interface {
	Node
	joinConstraint()
}

func (*OnConstraint) joinConstraint()    {}
func (*UsingConstraint) joinConstraint() {}

type ExplainStatement struct {
	Explain   Pos       // position of EXPLAIN
	Query     Pos       // position of QUERY (optional)
	QueryPlan Pos       // position of PLAN after QUERY (optional)
	Stmt      Statement // target statement
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

// String returns the string representation of the statement.
func (s *SavepointStatement) String() string {
	return fmt.Sprintf("SAVEPOINT %s", s.Name.String())
}

type ReleaseStatement struct {
	Release   Pos    // position of RELEASE keyword
	Savepoint Pos    // position of SAVEPOINT keyword (optional)
	Name      *Ident // name of savepoint
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
func (*CollateConstraint) constraint()    {}
func (*ForeignKeyConstraint) constraint() {}

type PrimaryKeyConstraint struct {
	Constraint Pos             // position of CONSTRAINT keyword
	Name       *Ident          // constraint name
	Primary    Pos             // position of PRIMARY keyword
	Key        Pos             // position of KEY keyword
	OnConflict *ConflictClause // conflict clause

	Lparen  Pos      // position of left paren (table only)
	Columns []*Ident // indexed columns (table only)
	Rparen  Pos      // position of right paren (table only)

	Asc           Pos // position of ASC keyword (column only)
	Desc          Pos // position of ASC keyword (column only)
	Autoincrement Pos // position of AUTOINCREMENT keyword (column only)
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

	if c.Asc.IsValid() {
		buf.WriteString(" ASC")
	} else if c.Desc.IsValid() {
		buf.WriteString(" DESC")
	}

	if c.OnConflict != nil {
		buf.WriteString(" ")
		buf.WriteString(c.OnConflict.String())
	}

	if c.Autoincrement.IsValid() {
		buf.WriteString(" AUTOINCREMENT")
	}
	return buf.String()
}

type NotNullConstraint struct {
	Constraint Pos             // position of CONSTRAINT keyword
	Name       *Ident          // constraint name
	Not        Pos             // position of NOT keyword
	Null       Pos             // position of NULL keyword
	OnConflict *ConflictClause // conflict clause
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

	if c.OnConflict != nil {
		buf.WriteString(" ")
		buf.WriteString(c.OnConflict.String())
	}
	return buf.String()
}

type UniqueConstraint struct {
	Constraint Pos    // position of CONSTRAINT keyword
	Name       *Ident // constraint name
	Unique     Pos    // position of UNIQUE keyword

	Lparen  Pos      // position of left paren (table only)
	Columns []*Ident // indexed columns (table only)
	Rparen  Pos      // position of right paren (table only)

	OnConflict *ConflictClause // conflict clause
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

	if c.OnConflict != nil {
		buf.WriteString(" ")
		buf.WriteString(c.OnConflict.String())
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

type CollateConstraint struct {
	Constraint    Pos    // position of CONSTRAINT keyword
	Name          *Ident // constraint name
	Collate       Pos    // position of COLLATE keyword
	CollationName *Ident // name of collation
}

// String returns the string representation of the constraint.
func (c *CollateConstraint) String() string {
	var buf bytes.Buffer
	if c.Name != nil {
		buf.WriteString("CONSTRAINT ")
		buf.WriteString(c.Name.String())
		buf.WriteString(" ")
	}

	buf.WriteString("COLLATE ")
	buf.WriteString(c.CollationName.String())
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

type ConflictClause struct {
	On       Pos // position of ON keyword
	Conflict Pos // position of CONFLICT keyword
	Rollback Pos // position of ROLLBACK keyword
	Abort    Pos // position of ABORT keyword
	Fail     Pos // position of FAIL keyword
	Ignore   Pos // position of IGNORE keyword
	Replace  Pos // position of REPLACE keyword
}

// String returns the string representation of the clause.
func (c *ConflictClause) String() string {
	var buf bytes.Buffer
	buf.WriteString("ON CONFLICT")

	if c.Rollback.IsValid() {
		buf.WriteString(" ROLLBACK")
	} else if c.Abort.IsValid() {
		buf.WriteString(" ABORT")
	} else if c.Fail.IsValid() {
		buf.WriteString(" FAIL")
	} else if c.Ignore.IsValid() {
		buf.WriteString(" IGNORE")
	} else if c.Replace.IsValid() {
		buf.WriteString(" REPLACE")
	}
	return buf.String()
}

type AnalyzeStatement struct {
	Analyze Pos    // position of ANALYZE keyword
	Name    *Ident // table name
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

// String returns the string representation of the expression.
func (i *Ident) String() string {
	return `"` + strings.Replace(i.Name, `"`, `""`, -1) + `"`
}

type Type struct {
	Name      *Ident     // type name
	Lparen    Pos        // position of left paren (optional)
	Precision *NumberLit // precision (optional)
	Scale     *NumberLit // scale (optional)
	Rparen    Pos        // position of right paren (optional)
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

// String returns the string representation of the expression.
func (lit *StringLit) String() string {
	return `'` + strings.Replace(lit.Value, `'`, `''`, -1) + `'`
}

type BlobLit struct {
	ValuePos Pos    // literal position
	Value    string // literal value
}

// String returns the string representation of the expression.
func (lit *BlobLit) String() string {
	return `x'` + lit.Value + `'`
}

type NumberLit struct {
	ValuePos Pos    // literal position
	Value    string // literal value
}

// String returns the string representation of the expression.
func (lit *NumberLit) String() string {
	return lit.Value
}

type NullLit struct {
	Pos Pos
}

// String returns the string representation of the expression.
func (lit *NullLit) String() string {
	return "NULL"
}

type BoolLit struct {
	ValuePos Pos  // literal position
	Value    bool // literal value
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

// String returns the string representation of the raise function.
func (b *Raise) String() string {
	var buf bytes.Buffer
	buf.WriteString("RAISE(")
	if b.Rollback.IsValid() {
		fmt.Fprintf(&buf, "ROLLBACK, %s", b.Error.String())
	} else if b.Abort.IsValid() {
		fmt.Fprintf(&buf, "ABORT, %s", b.Error.String())
	} else if b.Fail.IsValid() {
		fmt.Fprintf(&buf, "FAIL, %s", b.Error.String())
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

// String returns the string representation of the clause.
func (c *FilterClause) String() string {
	return fmt.Sprintf("FILTER (WHERE %s)", c.X.String())
}

type OverClause struct {
	Over       Pos               // position of OVER keyword
	Name       *Ident            // window name
	Definition *WindowDefinition // window definition
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

type CollateExpr struct {
	X       Expr   // expression
	Collate Pos    // position of COLLATE keyword
	Name    *Ident // collation name
}

// String returns the string representation of the expression.
func (expr *CollateExpr) String() string {
	return fmt.Sprintf("%s COLLATE %s", expr.X, expr.Name.String())
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

// String returns the string representation of the expression.
func (expr *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", expr.X.String())
}

type Window struct {
	Name       *Ident            // name of window
	As         Pos               // position of AS keyword
	Definition *WindowDefinition // window definition
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
