package sql_test

import (
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/rqlite/rqlite/rsql"
)

func TestParser_ParseStatement(t *testing.T) {
	t.Run("ErrNoStatement", func(t *testing.T) {
		AssertParseStatementError(t, `123`, `1:1: expected statement, found 123`)
	})

	t.Run("Explain", func(t *testing.T) {
		t.Run("", func(t *testing.T) {
			AssertParseStatement(t, `EXPLAIN BEGIN`, &sql.ExplainStatement{
				Explain: pos(0),
				Stmt: &sql.BeginStatement{
					Begin: pos(8),
				},
			})
		})
		t.Run("QueryPlan", func(t *testing.T) {
			AssertParseStatement(t, `EXPLAIN QUERY PLAN BEGIN`, &sql.ExplainStatement{
				Explain:   pos(0),
				Query:     pos(8),
				QueryPlan: pos(14),
				Stmt: &sql.BeginStatement{
					Begin: pos(19),
				},
			})
		})
		t.Run("ErrNoPlan", func(t *testing.T) {
			AssertParseStatementError(t, `EXPLAIN QUERY`, `1:13: expected PLAN, found 'EOF'`)
		})
		t.Run("ErrStmt", func(t *testing.T) {
			AssertParseStatementError(t, `EXPLAIN CREATE`, `1:9: expected TABLE, VIEW, INDEX, TRIGGER`)
		})
	})

	t.Run("Begin", func(t *testing.T) {
		t.Run("", func(t *testing.T) {
			AssertParseStatement(t, `BEGIN`, &sql.BeginStatement{
				Begin: pos(0),
			})
		})
		t.Run("Transaction", func(t *testing.T) {
			AssertParseStatement(t, `BEGIN TRANSACTION`, &sql.BeginStatement{
				Begin:       pos(0),
				Transaction: pos(6),
			})
		})
		t.Run("DeferredTransaction", func(t *testing.T) {
			AssertParseStatement(t, `BEGIN DEFERRED TRANSACTION`, &sql.BeginStatement{
				Begin:       pos(0),
				Deferred:    pos(6),
				Transaction: pos(15),
			})
		})
		t.Run("Immediate", func(t *testing.T) {
			AssertParseStatement(t, `BEGIN IMMEDIATE;`, &sql.BeginStatement{
				Begin:     pos(0),
				Immediate: pos(6),
			})
		})
		t.Run("Exclusive", func(t *testing.T) {
			AssertParseStatement(t, `BEGIN EXCLUSIVE`, &sql.BeginStatement{
				Begin:     pos(0),
				Exclusive: pos(6),
			})
		})
		t.Run("ErrOverrun", func(t *testing.T) {
			AssertParseStatementError(t, `BEGIN COMMIT`, `1:7: expected semicolon or EOF, found 'COMMIT'`)
		})
	})

	t.Run("Commit", func(t *testing.T) {
		t.Run("", func(t *testing.T) {
			AssertParseStatement(t, `COMMIT`, &sql.CommitStatement{
				Commit: pos(0),
			})
		})
		t.Run("Transaction", func(t *testing.T) {
			AssertParseStatement(t, `COMMIT TRANSACTION`, &sql.CommitStatement{
				Commit:      pos(0),
				Transaction: pos(7),
			})
		})
	})

	t.Run("End", func(t *testing.T) {
		t.Run("", func(t *testing.T) {
			AssertParseStatement(t, `END`, &sql.CommitStatement{
				End: pos(0),
			})
		})
		t.Run("Transaction", func(t *testing.T) {
			AssertParseStatement(t, `END TRANSACTION`, &sql.CommitStatement{
				End:         pos(0),
				Transaction: pos(4),
			})
		})
	})

	t.Run("Rollback", func(t *testing.T) {
		t.Run("", func(t *testing.T) {
			AssertParseStatement(t, `ROLLBACK`, &sql.RollbackStatement{
				Rollback: pos(0),
			})
		})
		t.Run("Transaction", func(t *testing.T) {
			AssertParseStatement(t, `ROLLBACK TRANSACTION`, &sql.RollbackStatement{
				Rollback:    pos(0),
				Transaction: pos(9),
			})
		})
		t.Run("To", func(t *testing.T) {
			AssertParseStatement(t, `ROLLBACK TO svpt`, &sql.RollbackStatement{
				Rollback: pos(0),
				To:       pos(9),
				SavepointName: &sql.Ident{
					Name:    "svpt",
					NamePos: pos(12),
				},
			})
		})
		t.Run("TransactionToSavepoint", func(t *testing.T) {
			AssertParseStatement(t, `ROLLBACK TRANSACTION TO SAVEPOINT "svpt"`, &sql.RollbackStatement{
				Rollback:    pos(0),
				Transaction: pos(9),
				To:          pos(21),
				Savepoint:   pos(24),
				SavepointName: &sql.Ident{
					Name:    "svpt",
					NamePos: pos(34),
					Quoted:  true,
				},
			})
		})
		t.Run("ErrSavepointName", func(t *testing.T) {
			AssertParseStatementError(t, `ROLLBACK TO SAVEPOINT 123`, `1:23: expected savepoint name, found 123`)
		})
	})

	t.Run("Savepoint", func(t *testing.T) {
		t.Run("Ident", func(t *testing.T) {
			AssertParseStatement(t, `SAVEPOINT svpt`, &sql.SavepointStatement{
				Savepoint: pos(0),
				Name: &sql.Ident{
					Name:    "svpt",
					NamePos: pos(10),
				},
			})
		})
		t.Run("String", func(t *testing.T) {
			AssertParseStatement(t, `SAVEPOINT "svpt"`, &sql.SavepointStatement{
				Savepoint: pos(0),
				Name: &sql.Ident{
					Name:    "svpt",
					NamePos: pos(10),
					Quoted:  true,
				},
			})
		})
		t.Run("ErrSavepointName", func(t *testing.T) {
			AssertParseStatementError(t, `SAVEPOINT 123`, `1:11: expected savepoint name, found 123`)
		})
	})

	t.Run("Release", func(t *testing.T) {
		t.Run("Ident", func(t *testing.T) {
			AssertParseStatement(t, `RELEASE svpt`, &sql.ReleaseStatement{
				Release: pos(0),
				Name: &sql.Ident{
					Name:    "svpt",
					NamePos: pos(8),
				},
			})
		})
		t.Run("String", func(t *testing.T) {
			AssertParseStatement(t, `RELEASE "svpt"`, &sql.ReleaseStatement{
				Release: pos(0),
				Name: &sql.Ident{
					Name:    "svpt",
					NamePos: pos(8),
					Quoted:  true,
				},
			})
		})
		t.Run("SavepointIdent", func(t *testing.T) {
			AssertParseStatement(t, `RELEASE SAVEPOINT svpt`, &sql.ReleaseStatement{
				Release:   pos(0),
				Savepoint: pos(8),
				Name: &sql.Ident{
					Name:    "svpt",
					NamePos: pos(18),
				},
			})
		})
		t.Run("ErrSavepointName", func(t *testing.T) {
			AssertParseStatementError(t, `RELEASE 123`, `1:9: expected savepoint name, found 123`)
		})
	})

	t.Run("CreateTable", func(t *testing.T) {
		AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, col2 DECIMAL(10,5))`, &sql.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &sql.Ident{
				Name:    "tbl",
				NamePos: pos(13),
			},
			Lparen: pos(17),
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{NamePos: pos(18), Name: "col1"},
					Type: &sql.Type{
						Name: &sql.Ident{NamePos: pos(23), Name: "TEXT"},
					},
				},
				{
					Name: &sql.Ident{NamePos: pos(29), Name: "col2"},
					Type: &sql.Type{
						Name:      &sql.Ident{NamePos: pos(34), Name: "DECIMAL"},
						Lparen:    pos(41),
						Precision: &sql.NumberLit{ValuePos: pos(42), Value: "10"},
						Scale:     &sql.NumberLit{ValuePos: pos(45), Value: "5"},
						Rparen:    pos(46),
					},
				},
			},
			Rparen: pos(47),
		})

		AssertParseStatementError(t, `CREATE TABLE`, `1:12: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl `, `1:17: expected AS or left paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (`, `1:18: expected column name, CONSTRAINT, or right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT`, `1:27: expected column name, CONSTRAINT, or right paren, found 'EOF'`)

		AssertParseStatement(t, `CREATE TABLE IF NOT EXISTS tbl (col1 TEXT)`, &sql.CreateTableStatement{
			Create:      pos(0),
			Table:       pos(7),
			If:          pos(13),
			IfNot:       pos(16),
			IfNotExists: pos(20),
			Name: &sql.Ident{
				Name:    "tbl",
				NamePos: pos(27),
			},
			Lparen: pos(31),
			Columns: []*sql.ColumnDefinition{
				{
					Name: &sql.Ident{
						NamePos: pos(32),
						Name:    "col1",
					},
					Type: &sql.Type{
						Name: &sql.Ident{
							NamePos: pos(37),
							Name:    "TEXT",
						},
					},
				},
			},
			Rparen: pos(41),
		})
		AssertParseStatementError(t, `CREATE TABLE IF`, `1:15: expected NOT, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE IF NOT`, `1:19: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1`, `1:22: expected type name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(`, `1:31: expected precision, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(-12,`, `1:35: expected scale, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(1,2`, `1:34: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(1`, `1:32: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT CONSTRAINT`, `1:38: expected constraint name, found 'EOF'`)

		AssertParseStatement(t, `CREATE TABLE tbl AS SELECT foo`, &sql.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &sql.Ident{
				Name:    "tbl",
				NamePos: pos(13),
			},
			As: pos(17),
			Select: &sql.SelectStatement{
				Select: pos(20),
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{NamePos: pos(27), Name: "foo"}},
				},
			},
		})
		AssertParseStatement(t, `CREATE TABLE tbl AS WITH cte (x) AS (SELECT y) SELECT foo`, &sql.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &sql.Ident{
				Name:    "tbl",
				NamePos: pos(13),
			},
			As: pos(17),
			Select: &sql.SelectStatement{
				WithClause: &sql.WithClause{
					With: pos(20),
					CTEs: []*sql.CTE{
						{
							TableName:     &sql.Ident{NamePos: pos(25), Name: "cte"},
							ColumnsLparen: pos(29),
							Columns: []*sql.Ident{
								{NamePos: pos(30), Name: "x"},
							},
							ColumnsRparen: pos(31),
							As:            pos(33),
							SelectLparen:  pos(36),
							Select: &sql.SelectStatement{
								Select: pos(37),
								Columns: []*sql.ResultColumn{
									{Expr: &sql.Ident{NamePos: pos(44), Name: "y"}},
								},
							},
							SelectRparen: pos(45),
						},
					},
				},
				Select: pos(47),
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{NamePos: pos(54), Name: "foo"}},
				},
			},
		})
		AssertParseStatementError(t, `CREATE TABLE tbl AS`, `1:19: expected SELECT or VALUES, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl AS WITH`, `1:24: expected table name, found 'EOF'`)

		t.Run("ColumnConstraint", func(t *testing.T) {
			t.Run("PrimaryKey", func(t *testing.T) {
				t.Run("Simple", func(t *testing.T) {
					AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT PRIMARY KEY)`, &sql.CreateTableStatement{
						Create: pos(0),
						Table:  pos(7),
						Name:   &sql.Ident{Name: "tbl", NamePos: pos(13)},
						Lparen: pos(17),
						Columns: []*sql.ColumnDefinition{
							{
								Name: &sql.Ident{Name: "col1", NamePos: pos(18)},
								Type: &sql.Type{
									Name: &sql.Ident{Name: "TEXT", NamePos: pos(23)},
								},
								Constraints: []sql.Constraint{
									&sql.PrimaryKeyConstraint{
										Primary: pos(28),
										Key:     pos(36),
									},
								},
							},
						},
						Rparen: pos(39),
					})
				})
				t.Run("Full", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT CONSTRAINT cons1 PRIMARY KEY AUTOINCREMENT)`).(*sql.CreateTableStatement)
					if diff := deep.Equal(stmt.Columns[0].Constraints[0], &sql.PrimaryKeyConstraint{
						Constraint:    pos(28),
						Name:          &sql.Ident{Name: "cons1", NamePos: pos(39)},
						Primary:       pos(45),
						Key:           pos(53),
						Autoincrement: pos(57),
					}); diff != nil {
						t.Fatal(diff)
					}
				})
				t.Run("ErrNoKey", func(t *testing.T) {
					AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT PRIMARY`, `1:35: expected KEY, found 'EOF'`)
				})
			})

			t.Run("NotNull", func(t *testing.T) {
				t.Run("ErrNoKey", func(t *testing.T) {
					AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT NOT`, `1:31: expected NULL, found 'EOF'`)
				})
				t.Run("Simple", func(t *testing.T) {
					AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT CONSTRAINT con1 NOT NULL)`, &sql.CreateTableStatement{
						Create: pos(0),
						Table:  pos(7),
						Name:   &sql.Ident{Name: "tbl", NamePos: pos(13)},
						Lparen: pos(17),
						Columns: []*sql.ColumnDefinition{
							{
								Name: &sql.Ident{Name: "col1", NamePos: pos(18)},
								Type: &sql.Type{
									Name: &sql.Ident{Name: "TEXT", NamePos: pos(23)},
								},
								Constraints: []sql.Constraint{
									&sql.NotNullConstraint{
										Constraint: pos(28),
										Name:       &sql.Ident{Name: "con1", NamePos: pos(39)},
										Not:        pos(44),
										Null:       pos(48),
									},
								},
							},
						},
						Rparen: pos(52),
					})
				})
			})

			t.Run("Unique", func(t *testing.T) {
				t.Run("Simple", func(t *testing.T) {
					AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT CONSTRAINT con1 UNIQUE)`, &sql.CreateTableStatement{
						Create: pos(0),
						Table:  pos(7),
						Name:   &sql.Ident{Name: "tbl", NamePos: pos(13)},
						Lparen: pos(17),
						Columns: []*sql.ColumnDefinition{
							{
								Name: &sql.Ident{Name: "col1", NamePos: pos(18)},
								Type: &sql.Type{
									Name: &sql.Ident{Name: "TEXT", NamePos: pos(23)},
								},
								Constraints: []sql.Constraint{
									&sql.UniqueConstraint{
										Constraint: pos(28),
										Name:       &sql.Ident{Name: "con1", NamePos: pos(39)},
										Unique:     pos(44),
									},
								},
							},
						},
						Rparen: pos(50),
					})
				})
			})
			t.Run("Check", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT CHECK (col1 > 1))`, &sql.CreateTableStatement{
					Create: pos(0),
					Table:  pos(7),
					Name:   &sql.Ident{Name: "tbl", NamePos: pos(13)},
					Lparen: pos(17),
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1", NamePos: pos(18)},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT", NamePos: pos(23)},
							},
							Constraints: []sql.Constraint{
								&sql.CheckConstraint{
									Check:  pos(28),
									Lparen: pos(34),
									Expr: &sql.BinaryExpr{
										X:  &sql.Ident{Name: "col1", NamePos: pos(35)},
										Op: sql.GT, OpPos: pos(40),
										Y: &sql.NumberLit{Value: "1", ValuePos: pos(42)},
									},
									Rparen: pos(43),
								},
							},
						},
					},
					Rparen: pos(44),
				})
			})
			t.Run("Default", func(t *testing.T) {
				t.Run("Expr", func(t *testing.T) {
					AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT DEFAULT (1))`, &sql.CreateTableStatement{
						Create: pos(0),
						Table:  pos(7),
						Name:   &sql.Ident{Name: "tbl", NamePos: pos(13)},
						Lparen: pos(17),
						Columns: []*sql.ColumnDefinition{
							{
								Name: &sql.Ident{Name: "col1", NamePos: pos(18)},
								Type: &sql.Type{
									Name: &sql.Ident{Name: "TEXT", NamePos: pos(23)},
								},
								Constraints: []sql.Constraint{
									&sql.DefaultConstraint{
										Default: pos(28),
										Lparen:  pos(36),
										Expr:    &sql.NumberLit{Value: "1", ValuePos: pos(37)},
										Rparen:  pos(38),
									},
								},
							},
						},
						Rparen: pos(39),
					})
				})
				t.Run("String", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT 'foo')`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Default: pos(28),
						Expr:    &sql.StringLit{Value: "foo", ValuePos: pos(36)},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Blob", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT x'0F0F')`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Default: pos(28),
						Expr:    &sql.BlobLit{Value: "0F0F", ValuePos: pos(36)},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Number", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT 1)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Default: pos(28),
						Expr:    &sql.NumberLit{Value: "1", ValuePos: pos(36)},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Null", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT NULL)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Default: pos(28),
						Expr:    &sql.NullLit{Pos: pos(36)},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Bool", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT DEFAULT true)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.DefaultConstraint{
						Default: pos(28),
						Expr:    &sql.BoolLit{Value: true, ValuePos: pos(36)},
					}); diff != "" {
						t.Fatal(diff)
					}
				})

				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT DEFAULT +`, `1:37: expected signed number, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT DEFAULT -`, `1:37: expected signed number, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT DEFAULT `, `1:36: expected literal value or left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT DEFAULT (TABLE`, `1:38: expected expression, found 'TABLE'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT DEFAULT (true`, `1:41: expected right paren, found 'EOF'`)
			})
			t.Run("ForeignKey", func(t *testing.T) {
				t.Run("Simple", func(t *testing.T) {
					AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2))`, &sql.CreateTableStatement{
						Create: pos(0),
						Table:  pos(7),
						Name:   &sql.Ident{Name: "tbl", NamePos: pos(13)},
						Lparen: pos(17),
						Columns: []*sql.ColumnDefinition{
							{
								Name: &sql.Ident{Name: "col1", NamePos: pos(18)},
								Type: &sql.Type{
									Name: &sql.Ident{Name: "TEXT", NamePos: pos(23)},
								},
								Constraints: []sql.Constraint{
									&sql.ForeignKeyConstraint{
										References:    pos(28),
										ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
										ForeignLparen: pos(43),
										ForeignColumns: []*sql.Ident{
											&sql.Ident{Name: "col2", NamePos: pos(44)},
										},
										ForeignRparen: pos(48),
									},
								},
							},
						},
						Rparen: pos(49),
					})
				})
				t.Run("OnDeleteSetNull", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE SET NULL)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						References:    pos(28),
						ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
						ForeignLparen: pos(43),
						ForeignColumns: []*sql.Ident{
							&sql.Ident{Name: "col2", NamePos: pos(44)},
						},
						ForeignRparen: pos(48),
						Args: []*sql.ForeignKeyArg{
							&sql.ForeignKeyArg{
								On:       pos(50),
								OnDelete: pos(53),
								Set:      pos(60),
								SetNull:  pos(64),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("OnDeleteSetDefault", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE SET DEFAULT)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						References:    pos(28),
						ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
						ForeignLparen: pos(43),
						ForeignColumns: []*sql.Ident{
							&sql.Ident{Name: "col2", NamePos: pos(44)},
						},
						ForeignRparen: pos(48),
						Args: []*sql.ForeignKeyArg{
							&sql.ForeignKeyArg{
								On:         pos(50),
								OnDelete:   pos(53),
								Set:        pos(60),
								SetDefault: pos(64),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("OnDeleteSetDefault", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE CASCADE)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						References:    pos(28),
						ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
						ForeignLparen: pos(43),
						ForeignColumns: []*sql.Ident{
							&sql.Ident{Name: "col2", NamePos: pos(44)},
						},
						ForeignRparen: pos(48),
						Args: []*sql.ForeignKeyArg{
							&sql.ForeignKeyArg{
								On:       pos(50),
								OnDelete: pos(53),
								Cascade:  pos(60),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("OnDeleteSetRestrict", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE RESTRICT)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						References:    pos(28),
						ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
						ForeignLparen: pos(43),
						ForeignColumns: []*sql.Ident{
							&sql.Ident{Name: "col2", NamePos: pos(44)},
						},
						ForeignRparen: pos(48),
						Args: []*sql.ForeignKeyArg{
							&sql.ForeignKeyArg{
								On:       pos(50),
								OnDelete: pos(53),
								Restrict: pos(60),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("OnDeleteSetNoAction", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE NO ACTION)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						References:    pos(28),
						ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
						ForeignLparen: pos(43),
						ForeignColumns: []*sql.Ident{
							&sql.Ident{Name: "col2", NamePos: pos(44)},
						},
						ForeignRparen: pos(48),
						Args: []*sql.ForeignKeyArg{
							&sql.ForeignKeyArg{
								On:       pos(50),
								OnDelete: pos(53),
								No:       pos(60),
								NoAction: pos(63),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Multiple", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) ON DELETE CASCADE ON UPDATE RESTRICT)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						References:    pos(28),
						ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
						ForeignLparen: pos(43),
						ForeignColumns: []*sql.Ident{
							&sql.Ident{Name: "col2", NamePos: pos(44)},
						},
						ForeignRparen: pos(48),
						Args: []*sql.ForeignKeyArg{
							&sql.ForeignKeyArg{
								On:       pos(50),
								OnDelete: pos(53),
								Cascade:  pos(60),
							},
							&sql.ForeignKeyArg{
								On:       pos(68),
								OnUpdate: pos(71),
								Restrict: pos(78),
							},
						},
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("Deferrable", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) DEFERRABLE)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						References:    pos(28),
						ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
						ForeignLparen: pos(43),
						ForeignColumns: []*sql.Ident{
							&sql.Ident{Name: "col2", NamePos: pos(44)},
						},
						ForeignRparen: pos(48),
						Deferrable:    pos(50),
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("NotDeferrable", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) NOT DEFERRABLE)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						References:    pos(28),
						ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
						ForeignLparen: pos(43),
						ForeignColumns: []*sql.Ident{
							&sql.Ident{Name: "col2", NamePos: pos(44)},
						},
						ForeignRparen: pos(48),
						Not:           pos(50),
						NotDeferrable: pos(54),
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("InitiallyDeferred", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) DEFERRABLE INITIALLY DEFERRED)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						References:    pos(28),
						ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
						ForeignLparen: pos(43),
						ForeignColumns: []*sql.Ident{
							&sql.Ident{Name: "col2", NamePos: pos(44)},
						},
						ForeignRparen:     pos(48),
						Deferrable:        pos(50),
						Initially:         pos(61),
						InitiallyDeferred: pos(71),
					}); diff != "" {
						t.Fatal(diff)
					}
				})
				t.Run("InitiallyImmediate", func(t *testing.T) {
					stmt := ParseStatementOrFail(t, `CREATE TABLE tbl (col1 TEXT REFERENCES foo (col2) DEFERRABLE INITIALLY IMMEDIATE)`).(*sql.CreateTableStatement)
					if diff := deepEqual(stmt.Columns[0].Constraints[0], &sql.ForeignKeyConstraint{
						References:    pos(28),
						ForeignTable:  &sql.Ident{Name: "foo", NamePos: pos(39)},
						ForeignLparen: pos(43),
						ForeignColumns: []*sql.Ident{
							&sql.Ident{Name: "col2", NamePos: pos(44)},
						},
						ForeignRparen:      pos(48),
						Deferrable:         pos(50),
						Initially:          pos(61),
						InitiallyImmediate: pos(71),
					}); diff != "" {
						t.Fatal(diff)
					}
				})
			})
		})

		t.Run("TableConstraint", func(t *testing.T) {
			t.Run("PrimaryKey", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY KEY (col1, col2))`, &sql.CreateTableStatement{
					Create: pos(0),
					Table:  pos(7),
					Name:   &sql.Ident{Name: "tbl", NamePos: pos(13)},
					Lparen: pos(17),
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1", NamePos: pos(18)},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT", NamePos: pos(23)},
							},
						},
					},
					Constraints: []sql.Constraint{
						&sql.PrimaryKeyConstraint{
							Primary: pos(29),
							Key:     pos(37),
							Lparen:  pos(41),
							Columns: []*sql.Ident{
								&sql.Ident{Name: "col1", NamePos: pos(42)},
								&sql.Ident{Name: "col2", NamePos: pos(48)},
							},
							Rparen: pos(52),
						},
					},
					Rparen: pos(53),
				})

				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY`, `1:36: expected KEY, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY KEY`, `1:40: expected left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY KEY (col1)`, `1:47: expected right paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY KEY (1`, `1:43: expected column name, found 1`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, PRIMARY KEY (foo x`, `1:47: expected comma or right paren, found x`)
			})
			t.Run("Unique", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, CONSTRAINT con1 UNIQUE (col1, col2))`, &sql.CreateTableStatement{
					Create: pos(0),
					Table:  pos(7),
					Name:   &sql.Ident{Name: "tbl", NamePos: pos(13)},
					Lparen: pos(17),
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1", NamePos: pos(18)},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT", NamePos: pos(23)},
							},
						},
					},
					Constraints: []sql.Constraint{
						&sql.UniqueConstraint{
							Constraint: pos(29),
							Name:       &sql.Ident{Name: "con1", NamePos: pos(40)},
							Unique:     pos(45),
							Lparen:     pos(52),
							Columns: []*sql.Ident{
								&sql.Ident{Name: "col1", NamePos: pos(53)},
								&sql.Ident{Name: "col2", NamePos: pos(59)},
							},
							Rparen: pos(63),
						},
					},
					Rparen: pos(64),
				})
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, UNIQUE`, `1:35: expected left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, UNIQUE (1`, `1:38: expected column name, found 1`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, UNIQUE (x y`, `1:40: expected comma or right paren, found y`)
			})
			t.Run("Check", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, CHECK(foo = bar))`, &sql.CreateTableStatement{
					Create: pos(0),
					Table:  pos(7),
					Name:   &sql.Ident{Name: "tbl", NamePos: pos(13)},
					Lparen: pos(17),
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1", NamePos: pos(18)},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT", NamePos: pos(23)},
							},
						},
					},
					Constraints: []sql.Constraint{
						&sql.CheckConstraint{
							Check:  pos(29),
							Lparen: pos(34),
							Expr: &sql.BinaryExpr{
								X:  &sql.Ident{Name: "foo", NamePos: pos(35)},
								Op: sql.EQ, OpPos: pos(39),
								Y: &sql.Ident{Name: "bar", NamePos: pos(41)},
							},
							Rparen: pos(44),
						},
					},
					Rparen: pos(45),
				})

				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, CHECK`, `1:34: expected left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, CHECK (TABLE`, `1:37: expected expression, found 'TABLE'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, CHECK (true`, `1:40: expected right paren, found 'EOF'`)
			})
			t.Run("ForeignKey", func(t *testing.T) {
				AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (col1, col2) REFERENCES tbl2 (x, y))`, &sql.CreateTableStatement{
					Create: pos(0),
					Table:  pos(7),
					Name:   &sql.Ident{Name: "tbl", NamePos: pos(13)},
					Lparen: pos(17),
					Columns: []*sql.ColumnDefinition{
						{
							Name: &sql.Ident{Name: "col1", NamePos: pos(18)},
							Type: &sql.Type{
								Name: &sql.Ident{Name: "TEXT", NamePos: pos(23)},
							},
						},
					},
					Constraints: []sql.Constraint{
						&sql.ForeignKeyConstraint{
							Foreign:    pos(29),
							ForeignKey: pos(37),
							Lparen:     pos(41),
							Columns: []*sql.Ident{
								&sql.Ident{Name: "col1", NamePos: pos(42)},
								&sql.Ident{Name: "col2", NamePos: pos(48)},
							},
							Rparen:        pos(52),
							References:    pos(54),
							ForeignTable:  &sql.Ident{Name: "tbl2", NamePos: pos(65)},
							ForeignLparen: pos(70),
							ForeignColumns: []*sql.Ident{
								&sql.Ident{Name: "x", NamePos: pos(71)},
								&sql.Ident{Name: "y", NamePos: pos(74)},
							},
							ForeignRparen: pos(75),
						},
					},
					Rparen: pos(76),
				})

				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN`, `1:36: expected KEY, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY`, `1:40: expected left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (`, `1:42: expected column name, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x`, `1:43: expected comma or right paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x)`, `1:44: expected REFERENCES, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES`, `1:55: expected foreign table name, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl`, `1:59: expected left paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (`, `1:61: expected foreign column name, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x`, `1:62: expected comma or right paren, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x) ON`, `1:66: expected UPDATE or DELETE, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x) ON UPDATE SET`, `1:77: expected NULL or DEFAULT, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x) ON UPDATE NO`, `1:76: expected ACTION, found 'EOF'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x) ON UPDATE TABLE`, `1:75: expected SET NULL, SET DEFAULT, CASCADE, RESTRICT, or NO ACTION, found 'TABLE'`)
				AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT, FOREIGN KEY (x) REFERENCES tbl (x) ON UPDATE CASCADE NOT`, `1:85: expected DEFERRABLE, found 'EOF'`)
			})
		})
	})

	t.Run("DropTable", func(t *testing.T) {
		AssertParseStatement(t, `DROP TABLE vw`, &sql.DropTableStatement{
			Drop:  pos(0),
			Table: pos(5),
			Name:  &sql.Ident{NamePos: pos(11), Name: "vw"},
		})
		AssertParseStatement(t, `DROP TABLE IF EXISTS vw`, &sql.DropTableStatement{
			Drop:     pos(0),
			Table:    pos(5),
			If:       pos(11),
			IfExists: pos(14),
			Name:     &sql.Ident{NamePos: pos(21), Name: "vw"},
		})
		AssertParseStatementError(t, `DROP TABLE`, `1:10: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `DROP TABLE IF`, `1:13: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `DROP TABLE IF EXISTS`, `1:20: expected table name, found 'EOF'`)
	})

	t.Run("CreateView", func(t *testing.T) {
		AssertParseStatement(t, `CREATE VIEW vw (col1, col2) AS SELECT x, y`, &sql.CreateViewStatement{
			Create: pos(0),
			View:   pos(7),
			Name:   &sql.Ident{NamePos: pos(12), Name: "vw"},
			Lparen: pos(15),
			Columns: []*sql.Ident{
				&sql.Ident{NamePos: pos(16), Name: "col1"},
				&sql.Ident{NamePos: pos(22), Name: "col2"},
			},
			Rparen: pos(26),
			As:     pos(28),
			Select: &sql.SelectStatement{
				Select: pos(31),
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{NamePos: pos(38), Name: "x"}},
					{Expr: &sql.Ident{NamePos: pos(41), Name: "y"}},
				},
			},
		})
		AssertParseStatement(t, `CREATE VIEW vw AS SELECT x`, &sql.CreateViewStatement{
			Create: pos(0),
			View:   pos(7),
			Name:   &sql.Ident{NamePos: pos(12), Name: "vw"},
			As:     pos(15),
			Select: &sql.SelectStatement{
				Select: pos(18),
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{NamePos: pos(25), Name: "x"}},
				},
			},
		})
		AssertParseStatement(t, `CREATE VIEW IF NOT EXISTS vw AS SELECT x`, &sql.CreateViewStatement{
			Create:      pos(0),
			View:        pos(7),
			If:          pos(12),
			IfNot:       pos(15),
			IfNotExists: pos(19),
			Name:        &sql.Ident{NamePos: pos(26), Name: "vw"},
			As:          pos(29),
			Select: &sql.SelectStatement{
				Select: pos(32),
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{NamePos: pos(39), Name: "x"}},
				},
			},
		})
		AssertParseStatementError(t, `CREATE VIEW`, `1:11: expected view name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW IF`, `1:14: expected NOT, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW IF NOT`, `1:18: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW vw`, `1:14: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW vw (`, `1:16: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW vw (x`, `1:17: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW vw AS`, `1:17: expected SELECT or VALUES, found 'EOF'`)
		AssertParseStatementError(t, `CREATE VIEW vw AS SELECT`, `1:24: expected expression, found 'EOF'`)
	})

	t.Run("DropView", func(t *testing.T) {
		AssertParseStatement(t, `DROP VIEW vw`, &sql.DropViewStatement{
			Drop: pos(0),
			View: pos(5),
			Name: &sql.Ident{NamePos: pos(10), Name: "vw"},
		})
		AssertParseStatement(t, `DROP VIEW IF EXISTS vw`, &sql.DropViewStatement{
			Drop:     pos(0),
			View:     pos(5),
			If:       pos(10),
			IfExists: pos(13),
			Name:     &sql.Ident{NamePos: pos(20), Name: "vw"},
		})
		AssertParseStatementError(t, `DROP`, `1:1: expected TABLE, VIEW, INDEX, or TRIGGER`)
		AssertParseStatementError(t, `DROP VIEW`, `1:9: expected view name, found 'EOF'`)
		AssertParseStatementError(t, `DROP VIEW IF`, `1:12: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `DROP VIEW IF EXISTS`, `1:19: expected view name, found 'EOF'`)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		AssertParseStatement(t, `CREATE INDEX idx ON tbl (x ASC, y DESC, z)`, &sql.CreateIndexStatement{
			Create: pos(0),
			Index:  pos(7),
			Name:   &sql.Ident{NamePos: pos(13), Name: "idx"},
			On:     pos(17),
			Table:  &sql.Ident{NamePos: pos(20), Name: "tbl"},
			Lparen: pos(24),
			Columns: []*sql.IndexedColumn{
				{X: &sql.Ident{NamePos: pos(25), Name: "x"}, Asc: pos(27)},
				{X: &sql.Ident{NamePos: pos(32), Name: "y"}, Desc: pos(34)},
				{X: &sql.Ident{NamePos: pos(40), Name: "z"}},
			},
			Rparen: pos(41),
		})
		AssertParseStatement(t, `CREATE UNIQUE INDEX idx ON tbl (x)`, &sql.CreateIndexStatement{
			Create: pos(0),
			Unique: pos(7),
			Index:  pos(14),
			Name:   &sql.Ident{NamePos: pos(20), Name: "idx"},
			On:     pos(24),
			Table:  &sql.Ident{NamePos: pos(27), Name: "tbl"},
			Lparen: pos(31),
			Columns: []*sql.IndexedColumn{
				{X: &sql.Ident{NamePos: pos(32), Name: "x"}},
			},
			Rparen: pos(33),
		})
		AssertParseStatement(t, `CREATE INDEX idx ON tbl (x) WHERE true`, &sql.CreateIndexStatement{
			Create: pos(0),
			Index:  pos(7),
			Name:   &sql.Ident{NamePos: pos(13), Name: "idx"},
			On:     pos(17),
			Table:  &sql.Ident{NamePos: pos(20), Name: "tbl"},
			Lparen: pos(24),
			Columns: []*sql.IndexedColumn{
				{X: &sql.Ident{NamePos: pos(25), Name: "x"}},
			},
			Rparen:    pos(26),
			Where:     pos(28),
			WhereExpr: &sql.BoolLit{ValuePos: pos(34), Value: true},
		})
		AssertParseStatement(t, `CREATE INDEX IF NOT EXISTS idx ON tbl (x)`, &sql.CreateIndexStatement{
			Create:      pos(0),
			Index:       pos(7),
			If:          pos(13),
			IfNot:       pos(16),
			IfNotExists: pos(20),
			Name:        &sql.Ident{NamePos: pos(27), Name: "idx"},
			On:          pos(31),
			Table:       &sql.Ident{NamePos: pos(34), Name: "tbl"},
			Lparen:      pos(38),
			Columns: []*sql.IndexedColumn{
				{X: &sql.Ident{NamePos: pos(39), Name: "x"}},
			},
			Rparen: pos(40),
		})
		AssertParseStatementError(t, `CREATE UNIQUE`, `1:13: expected INDEX, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX`, `1:12: expected index name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX IF`, `1:15: expected NOT, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX IF NOT`, `1:19: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx`, `1:16: expected ON, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx ON`, `1:19: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx ON tbl`, `1:23: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx ON tbl (`, `1:25: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx ON tbl (x`, `1:26: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE INDEX idx ON tbl (x) WHERE`, `1:33: expected expression, found 'EOF'`)
	})

	t.Run("DropIndex", func(t *testing.T) {
		AssertParseStatement(t, `DROP INDEX idx`, &sql.DropIndexStatement{
			Drop:  pos(0),
			Index: pos(5),
			Name:  &sql.Ident{NamePos: pos(11), Name: "idx"},
		})
		AssertParseStatement(t, `DROP INDEX IF EXISTS idx`, &sql.DropIndexStatement{
			Drop:     pos(0),
			Index:    pos(5),
			If:       pos(11),
			IfExists: pos(14),
			Name:     &sql.Ident{NamePos: pos(21), Name: "idx"},
		})
		AssertParseStatementError(t, `DROP INDEX`, `1:10: expected index name, found 'EOF'`)
		AssertParseStatementError(t, `DROP INDEX IF`, `1:13: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `DROP INDEX IF EXISTS`, `1:20: expected index name, found 'EOF'`)
	})

	t.Run("CreateTrigger", func(t *testing.T) {
		AssertParseStatement(t, `CREATE TRIGGER trig DELETE ON tbl BEGIN INSERT INTO new DEFAULT VALUES; UPDATE new SET x = 1; END`, &sql.CreateTriggerStatement{
			Create:  pos(0),
			Trigger: pos(7),
			Name:    &sql.Ident{NamePos: pos(15), Name: "trig"},
			Delete:  pos(20),
			On:      pos(27),
			Table:   &sql.Ident{NamePos: pos(30), Name: "tbl"},
			Begin:   pos(34),
			Body: []sql.Statement{
				&sql.InsertStatement{
					Insert:        pos(40),
					Into:          pos(47),
					Table:         &sql.Ident{NamePos: pos(52), Name: "new"},
					Default:       pos(56),
					DefaultValues: pos(64),
				},
				&sql.UpdateStatement{
					Update: pos(72),
					Table: &sql.QualifiedTableName{
						Name: &sql.Ident{NamePos: pos(79), Name: "new"},
					},
					Set: pos(83),
					Assignments: []*sql.Assignment{{
						Columns: []*sql.Ident{{NamePos: pos(87), Name: "x"}},
						Eq:      pos(89),
						Expr:    &sql.NumberLit{ValuePos: pos(91), Value: "1"},
					}},
				},
			},
			End: pos(94),
		})
		AssertParseStatement(t, `CREATE TRIGGER IF NOT EXISTS trig BEFORE INSERT ON tbl BEGIN DELETE FROM new; END`, &sql.CreateTriggerStatement{
			Create:      pos(0),
			Trigger:     pos(7),
			If:          pos(15),
			IfNot:       pos(18),
			IfNotExists: pos(22),
			Name:        &sql.Ident{NamePos: pos(29), Name: "trig"},
			Before:      pos(34),
			Insert:      pos(41),
			On:          pos(48),
			Table:       &sql.Ident{NamePos: pos(51), Name: "tbl"},
			Begin:       pos(55),
			Body: []sql.Statement{
				&sql.DeleteStatement{
					Delete: pos(61),
					From:   pos(68),
					Table: &sql.QualifiedTableName{
						Name: &sql.Ident{NamePos: pos(73), Name: "new"},
					},
				},
			},
			End: pos(78),
		})
		AssertParseStatement(t, `CREATE TRIGGER trig INSTEAD OF UPDATE ON tbl BEGIN SELECT *; END`, &sql.CreateTriggerStatement{
			Create:    pos(0),
			Trigger:   pos(7),
			Name:      &sql.Ident{NamePos: pos(15), Name: "trig"},
			Instead:   pos(20),
			InsteadOf: pos(28),
			Update:    pos(31),
			On:        pos(38),
			Table:     &sql.Ident{NamePos: pos(41), Name: "tbl"},
			Begin:     pos(45),
			Body: []sql.Statement{
				&sql.SelectStatement{
					Select:  pos(51),
					Columns: []*sql.ResultColumn{{Star: pos(58)}},
				},
			},
			End: pos(61),
		})
		AssertParseStatement(t, `CREATE TRIGGER trig INSTEAD OF UPDATE OF x, y ON tbl FOR EACH ROW WHEN true BEGIN SELECT *; END`, &sql.CreateTriggerStatement{
			Create:    pos(0),
			Trigger:   pos(7),
			Name:      &sql.Ident{NamePos: pos(15), Name: "trig"},
			Instead:   pos(20),
			InsteadOf: pos(28),
			Update:    pos(31),
			UpdateOf:  pos(38),
			UpdateOfColumns: []*sql.Ident{
				{NamePos: pos(41), Name: "x"},
				{NamePos: pos(44), Name: "y"},
			},
			On:         pos(46),
			Table:      &sql.Ident{NamePos: pos(49), Name: "tbl"},
			For:        pos(53),
			ForEach:    pos(57),
			ForEachRow: pos(62),
			When:       pos(66),
			WhenExpr:   &sql.BoolLit{ValuePos: pos(71), Value: true},
			Begin:      pos(76),
			Body: []sql.Statement{
				&sql.SelectStatement{
					Select:  pos(82),
					Columns: []*sql.ResultColumn{{Star: pos(89)}},
				},
			},
			End: pos(92),
		})
		AssertParseStatement(t, `CREATE TRIGGER trig AFTER UPDATE ON tbl BEGIN WITH cte (x) AS (SELECT y) SELECT *; END`, &sql.CreateTriggerStatement{
			Create:  pos(0),
			Trigger: pos(7),
			Name:    &sql.Ident{NamePos: pos(15), Name: "trig"},
			After:   pos(20),
			Update:  pos(26),
			On:      pos(33),
			Table:   &sql.Ident{NamePos: pos(36), Name: "tbl"},
			Begin:   pos(40),
			Body: []sql.Statement{
				&sql.SelectStatement{
					WithClause: &sql.WithClause{
						With: pos(46),
						CTEs: []*sql.CTE{
							{
								TableName:     &sql.Ident{NamePos: pos(51), Name: "cte"},
								ColumnsLparen: pos(55),
								Columns: []*sql.Ident{
									{NamePos: pos(56), Name: "x"},
								},
								ColumnsRparen: pos(57),
								As:            pos(59),
								SelectLparen:  pos(62),
								Select: &sql.SelectStatement{
									Select: pos(63),
									Columns: []*sql.ResultColumn{
										{Expr: &sql.Ident{NamePos: pos(70), Name: "y"}},
									},
								},
								SelectRparen: pos(71),
							},
						},
					},
					Select:  pos(73),
					Columns: []*sql.ResultColumn{{Star: pos(80)}},
				},
			},
			End: pos(83),
		})

		AssertParseStatementError(t, `CREATE TRIGGER`, `1:14: expected index name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER IF`, `1:17: expected NOT, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER IF NOT`, `1:21: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig INSTEAD`, `1:27: expected OF, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER`, `1:25: expected DELETE, INSERT, or UPDATE, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig UPDATE OF`, `1:29: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig UPDATE OF x,`, `1:32: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT`, `1:32: expected ON, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON `, `1:36: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl FOR`, `1:43: expected EACH, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl FOR EACH`, `1:48: expected ROW, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl WHEN`, `1:44: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl`, `1:39: expected BEGIN, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl BEGIN`, `1:45: expected statement, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl BEGIN SELECT`, `1:52: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl BEGIN SELECT *`, `1:54: expected semicolon, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TRIGGER trig AFTER INSERT ON tbl BEGIN SELECT *;`, `1:55: expected statement, found 'EOF'`)
	})

	t.Run("DropTrigger", func(t *testing.T) {
		AssertParseStatement(t, `DROP TRIGGER trig`, &sql.DropTriggerStatement{
			Drop:    pos(0),
			Trigger: pos(5),
			Name:    &sql.Ident{NamePos: pos(13), Name: "trig"},
		})
		AssertParseStatement(t, `DROP TRIGGER IF EXISTS trig`, &sql.DropTriggerStatement{
			Drop:     pos(0),
			Trigger:  pos(5),
			If:       pos(13),
			IfExists: pos(16),
			Name:     &sql.Ident{NamePos: pos(23), Name: "trig"},
		})
		AssertParseStatementError(t, `DROP TRIGGER`, `1:12: expected trigger name, found 'EOF'`)
		AssertParseStatementError(t, `DROP TRIGGER IF`, `1:15: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `DROP TRIGGER IF EXISTS`, `1:22: expected trigger name, found 'EOF'`)
	})

	t.Run("Select", func(t *testing.T) {
		AssertParseStatement(t, `SELECT * FROM tbl`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(14), Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT DISTINCT * FROM tbl`, &sql.SelectStatement{
			Select:   pos(0),
			Distinct: pos(7),
			Columns: []*sql.ResultColumn{
				{Star: pos(16)},
			},
			From: pos(18),
			Source: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(23), Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT ALL * FROM tbl`, &sql.SelectStatement{
			Select: pos(0),
			All:    pos(7),
			Columns: []*sql.ResultColumn{
				{Star: pos(11)},
			},
			From: pos(13),
			Source: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(18), Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT foo AS FOO, bar baz, tbl.* FROM tbl`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{
					Expr:  &sql.Ident{NamePos: pos(7), Name: "foo"},
					As:    pos(11),
					Alias: &sql.Ident{NamePos: pos(14), Name: "FOO"},
				},
				{
					Expr:  &sql.Ident{NamePos: pos(19), Name: "bar"},
					Alias: &sql.Ident{NamePos: pos(23), Name: "baz"},
				},
				{
					Expr: &sql.QualifiedRef{
						Table: &sql.Ident{NamePos: pos(28), Name: "tbl"},
						Dot:   pos(31),
						Star:  pos(32),
					},
				},
			},
			From: pos(34),
			Source: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(39), Name: "tbl"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl tbl2`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.QualifiedTableName{
				Name:  &sql.Ident{NamePos: pos(14), Name: "tbl"},
				Alias: &sql.Ident{NamePos: pos(18), Name: "tbl2"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl AS tbl2`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.QualifiedTableName{
				Name:  &sql.Ident{NamePos: pos(14), Name: "tbl"},
				As:    pos(18),
				Alias: &sql.Ident{NamePos: pos(21), Name: "tbl2"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl INDEXED BY idx`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.QualifiedTableName{
				Name:      &sql.Ident{NamePos: pos(14), Name: "tbl"},
				Indexed:   pos(18),
				IndexedBy: pos(26),
				Index:     &sql.Ident{NamePos: pos(29), Name: "idx"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl NOT INDEXED`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.QualifiedTableName{
				Name:       &sql.Ident{NamePos: pos(14), Name: "tbl"},
				Not:        pos(18),
				NotIndexed: pos(22),
			},
		})

		AssertParseStatement(t, `SELECT * FROM (SELECT *) AS tbl`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.ParenSource{
				Lparen: pos(14),
				X: &sql.SelectStatement{
					Select: pos(15),
					Columns: []*sql.ResultColumn{
						{Star: pos(22)},
					},
				},
				Rparen: pos(23),
				As:     pos(25),
				Alias:  &sql.Ident{NamePos: pos(28), Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT * FROM foo, bar`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.JoinClause{
				X: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &sql.JoinOperator{Comma: pos(17)},
				Y: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(19), Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo JOIN bar`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.JoinClause{
				X: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &sql.JoinOperator{Join: pos(18)},
				Y: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(23), Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo NATURAL JOIN bar`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.JoinClause{
				X: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &sql.JoinOperator{Natural: pos(18), Join: pos(26)},
				Y: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(31), Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo INNER JOIN bar ON true`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.JoinClause{
				X: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &sql.JoinOperator{Inner: pos(18), Join: pos(24)},
				Y: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(29), Name: "bar"},
				},
				Constraint: &sql.OnConstraint{
					On: pos(33),
					X:  &sql.BoolLit{ValuePos: pos(36), Value: true},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo LEFT JOIN bar USING (x, y)`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.JoinClause{
				X: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &sql.JoinOperator{Left: pos(18), Join: pos(23)},
				Y: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(28), Name: "bar"},
				},
				Constraint: &sql.UsingConstraint{
					Using:  pos(32),
					Lparen: pos(38),
					Columns: []*sql.Ident{
						{NamePos: pos(39), Name: "x"},
						{NamePos: pos(42), Name: "y"},
					},
					Rparen: pos(43),
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM X INNER JOIN Y ON true INNER JOIN Z ON false`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.JoinClause{
				X: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(14), Name: "X"},
				},
				Operator: &sql.JoinOperator{Inner: pos(16), Join: pos(22)},
				Y: &sql.JoinClause{
					X: &sql.QualifiedTableName{
						Name: &sql.Ident{NamePos: pos(27), Name: "Y"},
					},
					Operator: &sql.JoinOperator{Inner: pos(37), Join: pos(43)},
					Y: &sql.QualifiedTableName{
						Name: &sql.Ident{NamePos: pos(48), Name: "Z"},
					},
					Constraint: &sql.OnConstraint{
						On: pos(50),
						X:  &sql.BoolLit{ValuePos: pos(53), Value: false},
					},
				},
				Constraint: &sql.OnConstraint{
					On: pos(29),
					X:  &sql.BoolLit{ValuePos: pos(32), Value: true},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo LEFT OUTER JOIN bar`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.JoinClause{
				X: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &sql.JoinOperator{Left: pos(18), Outer: pos(23), Join: pos(29)},
				Y: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(34), Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo CROSS JOIN bar`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &sql.JoinClause{
				X: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &sql.JoinOperator{Cross: pos(18), Join: pos(24)},
				Y: &sql.QualifiedTableName{
					Name: &sql.Ident{NamePos: pos(29), Name: "bar"},
				},
			},
		})

		AssertParseStatement(t, `WITH cte (foo, bar) AS (SELECT baz), xxx AS (SELECT yyy) SELECT bat`, &sql.SelectStatement{
			WithClause: &sql.WithClause{
				With: pos(0),
				CTEs: []*sql.CTE{
					{
						TableName:     &sql.Ident{NamePos: pos(5), Name: "cte"},
						ColumnsLparen: pos(9),
						Columns: []*sql.Ident{
							{NamePos: pos(10), Name: "foo"},
							{NamePos: pos(15), Name: "bar"},
						},
						ColumnsRparen: pos(18),
						As:            pos(20),
						SelectLparen:  pos(23),
						Select: &sql.SelectStatement{
							Select: pos(24),
							Columns: []*sql.ResultColumn{
								{Expr: &sql.Ident{NamePos: pos(31), Name: "baz"}},
							},
						},
						SelectRparen: pos(34),
					},
					{
						TableName:    &sql.Ident{NamePos: pos(37), Name: "xxx"},
						As:           pos(41),
						SelectLparen: pos(44),
						Select: &sql.SelectStatement{
							Select: pos(45),
							Columns: []*sql.ResultColumn{
								{Expr: &sql.Ident{NamePos: pos(52), Name: "yyy"}},
							},
						},
						SelectRparen: pos(55),
					},
				},
			},
			Select: pos(57),
			Columns: []*sql.ResultColumn{
				{Expr: &sql.Ident{NamePos: pos(64), Name: "bat"}},
			},
		})
		AssertParseStatement(t, `WITH RECURSIVE cte AS (SELECT foo) SELECT bar`, &sql.SelectStatement{
			WithClause: &sql.WithClause{
				With:      pos(0),
				Recursive: pos(5),
				CTEs: []*sql.CTE{
					{
						TableName:    &sql.Ident{NamePos: pos(15), Name: "cte"},
						As:           pos(19),
						SelectLparen: pos(22),
						Select: &sql.SelectStatement{
							Select: pos(23),
							Columns: []*sql.ResultColumn{
								{Expr: &sql.Ident{NamePos: pos(30), Name: "foo"}},
							},
						},
						SelectRparen: pos(33),
					},
				},
			},
			Select: pos(35),
			Columns: []*sql.ResultColumn{
				{Expr: &sql.Ident{NamePos: pos(42), Name: "bar"}},
			},
		})

		AssertParseStatement(t, `SELECT * WHERE true`, &sql.SelectStatement{
			Select:    pos(0),
			Columns:   []*sql.ResultColumn{{Star: pos(7)}},
			Where:     pos(9),
			WhereExpr: &sql.BoolLit{ValuePos: pos(15), Value: true},
		})

		AssertParseStatement(t, `SELECT * GROUP BY foo, bar`, &sql.SelectStatement{
			Select:  pos(0),
			Columns: []*sql.ResultColumn{{Star: pos(7)}},
			Group:   pos(9),
			GroupBy: pos(15),
			GroupByExprs: []sql.Expr{
				&sql.Ident{NamePos: pos(18), Name: "foo"},
				&sql.Ident{NamePos: pos(23), Name: "bar"},
			},
		})
		AssertParseStatement(t, `SELECT * GROUP BY foo HAVING true`, &sql.SelectStatement{
			Select:  pos(0),
			Columns: []*sql.ResultColumn{{Star: pos(7)}},
			Group:   pos(9),
			GroupBy: pos(15),
			GroupByExprs: []sql.Expr{
				&sql.Ident{NamePos: pos(18), Name: "foo"},
			},
			Having:     pos(22),
			HavingExpr: &sql.BoolLit{ValuePos: pos(29), Value: true},
		})
		AssertParseStatement(t, `SELECT * WINDOW win1 AS (), win2 AS ()`, &sql.SelectStatement{
			Select:  pos(0),
			Columns: []*sql.ResultColumn{{Star: pos(7)}},
			Window:  pos(9),
			Windows: []*sql.Window{
				{
					Name: &sql.Ident{NamePos: pos(16), Name: "win1"},
					As:   pos(21),
					Definition: &sql.WindowDefinition{
						Lparen: pos(24),
						Rparen: pos(25),
					},
				},
				{
					Name: &sql.Ident{NamePos: pos(28), Name: "win2"},
					As:   pos(33),
					Definition: &sql.WindowDefinition{
						Lparen: pos(36),
						Rparen: pos(37),
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT * ORDER BY foo ASC, bar DESC`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Order:   pos(9),
			OrderBy: pos(15),
			OrderingTerms: []*sql.OrderingTerm{
				&sql.OrderingTerm{X: &sql.Ident{NamePos: pos(18), Name: "foo"}, Asc: pos(22)},
				&sql.OrderingTerm{X: &sql.Ident{NamePos: pos(27), Name: "bar"}, Desc: pos(31)},
			},
		})

		AssertParseStatement(t, `SELECT * LIMIT 1`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Limit:     pos(9),
			LimitExpr: &sql.NumberLit{ValuePos: pos(15), Value: "1"},
		})
		AssertParseStatement(t, `SELECT * LIMIT 1 OFFSET 2`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Limit:      pos(9),
			LimitExpr:  &sql.NumberLit{ValuePos: pos(15), Value: "1"},
			Offset:     pos(17),
			OffsetExpr: &sql.NumberLit{ValuePos: pos(24), Value: "2"},
		})
		AssertParseStatement(t, `SELECT * LIMIT 1, 2`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Limit:       pos(9),
			LimitExpr:   &sql.NumberLit{ValuePos: pos(15), Value: "1"},
			OffsetComma: pos(16),
			OffsetExpr:  &sql.NumberLit{ValuePos: pos(18), Value: "2"},
		})
		AssertParseStatement(t, `SELECT * UNION SELECT * ORDER BY foo`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Union: pos(9),
			Compound: &sql.SelectStatement{
				Select: pos(15),
				Columns: []*sql.ResultColumn{
					{Star: pos(22)},
				},
			},
			Order:   pos(24),
			OrderBy: pos(30),
			OrderingTerms: []*sql.OrderingTerm{
				&sql.OrderingTerm{X: &sql.Ident{NamePos: pos(33), Name: "foo"}},
			},
		})
		AssertParseStatement(t, `SELECT * UNION ALL SELECT *`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Union:    pos(9),
			UnionAll: pos(15),
			Compound: &sql.SelectStatement{
				Select: pos(19),
				Columns: []*sql.ResultColumn{
					{Star: pos(26)},
				},
			},
		})
		AssertParseStatement(t, `SELECT * INTERSECT SELECT *`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Intersect: pos(9),
			Compound: &sql.SelectStatement{
				Select: pos(19),
				Columns: []*sql.ResultColumn{
					{Star: pos(26)},
				},
			},
		})
		AssertParseStatement(t, `SELECT * EXCEPT SELECT *`, &sql.SelectStatement{
			Select: pos(0),
			Columns: []*sql.ResultColumn{
				{Star: pos(7)},
			},
			Except: pos(9),
			Compound: &sql.SelectStatement{
				Select: pos(16),
				Columns: []*sql.ResultColumn{
					{Star: pos(23)},
				},
			},
		})

		AssertParseStatement(t, `VALUES (1, 2), (3, 4)`, &sql.SelectStatement{
			Values: pos(0),
			ValueLists: []*sql.ExprList{
				{
					Lparen: pos(7),
					Exprs: []sql.Expr{
						&sql.NumberLit{ValuePos: pos(8), Value: "1"},
						&sql.NumberLit{ValuePos: pos(11), Value: "2"},
					},
					Rparen: pos(12),
				},
				{
					Lparen: pos(15),
					Exprs: []sql.Expr{
						&sql.NumberLit{ValuePos: pos(16), Value: "3"},
						&sql.NumberLit{ValuePos: pos(19), Value: "4"},
					},
					Rparen: pos(20),
				},
			},
		})

		AssertParseStatementError(t, `WITH `, `1:5: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte`, `1:8: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte (`, `1:10: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte (foo`, `1:13: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte (foo)`, `1:14: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS`, `1:11: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS (`, `1:13: expected SELECT or VALUES, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS (SELECT foo`, `1:23: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS (SELECT foo)`, `1:24: expected SELECT, VALUES, INSERT, REPLACE, UPDATE, or DELETE, found 'EOF'`)
		AssertParseStatementError(t, `SELECT `, `1:7: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT 1+`, `1:9: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo,`, `1:11: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo AS`, `1:13: expected column alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo.* AS`, `1:14: expected semicolon or EOF, found 'AS'`)
		AssertParseStatementError(t, `SELECT foo FROM`, `1:15: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo FROM foo INDEXED`, `1:27: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo FROM foo INDEXED BY`, `1:30: expected index name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo FROM foo NOT`, `1:23: expected INDEXED, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo INNER`, `1:23: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo CROSS`, `1:23: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo NATURAL`, `1:25: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo LEFT`, `1:22: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo LEFT OUTER`, `1:28: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo,`, `1:18: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar ON`, `1:29: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING`, `1:32: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (`, `1:34: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (x`, `1:35: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (x,`, `1:36: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (`, `1:15: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM ((`, `1:16: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (SELECT`, `1:21: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (tbl`, `1:18: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (SELECT *) AS`, `1:27: expected table alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo AS`, `1:20: expected table alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo WHERE`, `1:16: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP`, `1:14: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP BY`, `1:17: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP BY foo bar`, `1:23: expected semicolon or EOF, found bar`)
		AssertParseStatementError(t, `SELECT * GROUP BY foo HAVING`, `1:28: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW`, `1:15: expected window name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1`, `1:20: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1 AS`, `1:23: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1 AS (`, `1:25: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1 AS () win2`, `1:28: expected semicolon or EOF, found win2`)
		AssertParseStatementError(t, `SELECT * ORDER`, `1:14: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * ORDER BY`, `1:17: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * ORDER BY 1,`, `1:20: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT`, `1:14: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT 1,`, `1:17: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT 1 OFFSET`, `1:23: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `VALUES`, `1:6: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `VALUES (`, `1:8: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `VALUES (1`, `1:9: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `VALUES (1,`, `1:10: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * UNION`, `1:14: expected SELECT or VALUES, found 'EOF'`)
	})

	t.Run("Insert", func(t *testing.T) {
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (1, 2)`, &sql.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &sql.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*sql.Ident{
				{NamePos: pos(17), Name: "x"},
				{NamePos: pos(20), Name: "y"},
			},
			ColumnsRparen: pos(21),
			Values:        pos(23),
			ValueLists: []*sql.ExprList{{
				Lparen: pos(30),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(31), Value: "1"},
					&sql.NumberLit{ValuePos: pos(34), Value: "2"},
				},
				Rparen: pos(35),
			}},
		})
		AssertParseStatement(t, `REPLACE INTO tbl (x, y) VALUES (1, 2), (3, 4)`, &sql.InsertStatement{
			Replace:       pos(0),
			Into:          pos(8),
			Table:         &sql.Ident{NamePos: pos(13), Name: "tbl"},
			ColumnsLparen: pos(17),
			Columns: []*sql.Ident{
				{NamePos: pos(18), Name: "x"},
				{NamePos: pos(21), Name: "y"},
			},
			ColumnsRparen: pos(22),
			Values:        pos(24),
			ValueLists: []*sql.ExprList{
				{
					Lparen: pos(31),
					Exprs: []sql.Expr{
						&sql.NumberLit{ValuePos: pos(32), Value: "1"},
						&sql.NumberLit{ValuePos: pos(35), Value: "2"},
					},
					Rparen: pos(36),
				},
				{
					Lparen: pos(39),
					Exprs: []sql.Expr{
						&sql.NumberLit{ValuePos: pos(40), Value: "3"},
						&sql.NumberLit{ValuePos: pos(43), Value: "4"},
					},
					Rparen: pos(44),
				},
			},
		})
		AssertParseStatement(t, `INSERT OR REPLACE INTO tbl (x) VALUES (1)`, &sql.InsertStatement{
			Insert:          pos(0),
			InsertOr:        pos(7),
			InsertOrReplace: pos(10),
			Into:            pos(18),
			Table:           &sql.Ident{NamePos: pos(23), Name: "tbl"},
			ColumnsLparen:   pos(27),
			Columns: []*sql.Ident{
				{NamePos: pos(28), Name: "x"},
			},
			ColumnsRparen: pos(29),
			Values:        pos(31),
			ValueLists: []*sql.ExprList{{
				Lparen: pos(38),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(39), Value: "1"},
				},
				Rparen: pos(40),
			}},
		})
		AssertParseStatement(t, `INSERT OR ROLLBACK INTO tbl (x) VALUES (1)`, &sql.InsertStatement{
			Insert:           pos(0),
			InsertOr:         pos(7),
			InsertOrRollback: pos(10),
			Into:             pos(19),
			Table:            &sql.Ident{NamePos: pos(24), Name: "tbl"},
			ColumnsLparen:    pos(28),
			Columns: []*sql.Ident{
				{NamePos: pos(29), Name: "x"},
			},
			ColumnsRparen: pos(30),
			Values:        pos(32),
			ValueLists: []*sql.ExprList{{
				Lparen: pos(39),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(40), Value: "1"},
				},
				Rparen: pos(41),
			}},
		})
		AssertParseStatement(t, `INSERT OR ABORT INTO tbl (x) VALUES (1)`, &sql.InsertStatement{
			Insert:        pos(0),
			InsertOr:      pos(7),
			InsertOrAbort: pos(10),
			Into:          pos(16),
			Table:         &sql.Ident{NamePos: pos(21), Name: "tbl"},
			ColumnsLparen: pos(25),
			Columns: []*sql.Ident{
				{NamePos: pos(26), Name: "x"},
			},
			ColumnsRparen: pos(27),
			Values:        pos(29),
			ValueLists: []*sql.ExprList{{
				Lparen: pos(36),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(37), Value: "1"},
				},
				Rparen: pos(38),
			}},
		})
		AssertParseStatement(t, `INSERT OR FAIL INTO tbl VALUES (1)`, &sql.InsertStatement{
			Insert:       pos(0),
			InsertOr:     pos(7),
			InsertOrFail: pos(10),
			Into:         pos(15),
			Table:        &sql.Ident{NamePos: pos(20), Name: "tbl"},
			Values:       pos(24),
			ValueLists: []*sql.ExprList{{
				Lparen: pos(31),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(32), Value: "1"},
				},
				Rparen: pos(33),
			}},
		})
		AssertParseStatement(t, `INSERT OR IGNORE INTO tbl AS tbl2 VALUES (1)`, &sql.InsertStatement{
			Insert:         pos(0),
			InsertOr:       pos(7),
			InsertOrIgnore: pos(10),
			Into:           pos(17),
			Table:          &sql.Ident{NamePos: pos(22), Name: "tbl"},
			As:             pos(26),
			Alias:          &sql.Ident{NamePos: pos(29), Name: "tbl2"},
			Values:         pos(34),
			ValueLists: []*sql.ExprList{{
				Lparen: pos(41),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(42), Value: "1"},
				},
				Rparen: pos(43),
			}},
		})

		AssertParseStatement(t, `WITH cte (foo) AS (SELECT bar) INSERT INTO tbl VALUES (1)`, &sql.InsertStatement{
			WithClause: &sql.WithClause{
				With: pos(0),
				CTEs: []*sql.CTE{{
					TableName:     &sql.Ident{NamePos: pos(5), Name: "cte"},
					ColumnsLparen: pos(9),
					Columns: []*sql.Ident{
						{NamePos: pos(10), Name: "foo"},
					},
					ColumnsRparen: pos(13),
					As:            pos(15),
					SelectLparen:  pos(18),
					Select: &sql.SelectStatement{
						Select: pos(19),
						Columns: []*sql.ResultColumn{
							{Expr: &sql.Ident{NamePos: pos(26), Name: "bar"}},
						},
					},
					SelectRparen: pos(29),
				}},
			},
			Insert: pos(31),
			Into:   pos(38),
			Table:  &sql.Ident{NamePos: pos(43), Name: "tbl"},
			Values: pos(47),
			ValueLists: []*sql.ExprList{{
				Lparen: pos(54),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(55), Value: "1"},
				},
				Rparen: pos(56),
			}},
		})
		AssertParseStatement(t, `WITH cte (foo) AS (SELECT bar) INSERT INTO tbl VALUES (1)`, &sql.InsertStatement{
			WithClause: &sql.WithClause{
				With: pos(0),
				CTEs: []*sql.CTE{{
					TableName:     &sql.Ident{NamePos: pos(5), Name: "cte"},
					ColumnsLparen: pos(9),
					Columns: []*sql.Ident{
						{NamePos: pos(10), Name: "foo"},
					},
					ColumnsRparen: pos(13),
					As:            pos(15),
					SelectLparen:  pos(18),
					Select: &sql.SelectStatement{
						Select: pos(19),
						Columns: []*sql.ResultColumn{
							{Expr: &sql.Ident{NamePos: pos(26), Name: "bar"}},
						},
					},
					SelectRparen: pos(29),
				}},
			},
			Insert: pos(31),
			Into:   pos(38),
			Table:  &sql.Ident{NamePos: pos(43), Name: "tbl"},
			Values: pos(47),
			ValueLists: []*sql.ExprList{{
				Lparen: pos(54),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(55), Value: "1"},
				},
				Rparen: pos(56),
			}},
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) SELECT y`, &sql.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &sql.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*sql.Ident{
				{NamePos: pos(17), Name: "x"},
			},
			ColumnsRparen: pos(18),
			Select: &sql.SelectStatement{
				Select: pos(20),
				Columns: []*sql.ResultColumn{
					{Expr: &sql.Ident{NamePos: pos(27), Name: "y"}},
				},
			},
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) DEFAULT VALUES`, &sql.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &sql.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*sql.Ident{
				{NamePos: pos(17), Name: "x"},
			},
			ColumnsRparen: pos(18),
			Default:       pos(20),
			DefaultValues: pos(28),
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (y ASC, z DESC) DO NOTHING`, &sql.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &sql.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*sql.Ident{
				{NamePos: pos(17), Name: "x"},
			},
			ColumnsRparen: pos(18),
			Values:        pos(20),
			ValueLists: []*sql.ExprList{{
				Lparen: pos(27),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(28), Value: "1"},
				},
				Rparen: pos(29),
			}},
			UpsertClause: &sql.UpsertClause{
				On:         pos(31),
				OnConflict: pos(34),
				Lparen:     pos(43),
				Columns: []*sql.IndexedColumn{
					{X: &sql.Ident{NamePos: pos(44), Name: "y"}, Asc: pos(46)},
					{X: &sql.Ident{NamePos: pos(51), Name: "z"}, Desc: pos(53)},
				},
				Rparen:    pos(57),
				Do:        pos(59),
				DoNothing: pos(62),
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (y) WHERE true DO UPDATE SET foo = 1, (bar, baz) = 2 WHERE false`, &sql.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &sql.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*sql.Ident{
				{NamePos: pos(17), Name: "x"},
			},
			ColumnsRparen: pos(18),
			Values:        pos(20),
			ValueLists: []*sql.ExprList{{
				Lparen: pos(27),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(28), Value: "1"},
				},
				Rparen: pos(29),
			}},
			UpsertClause: &sql.UpsertClause{
				On:         pos(31),
				OnConflict: pos(34),
				Lparen:     pos(43),
				Columns: []*sql.IndexedColumn{
					{X: &sql.Ident{NamePos: pos(44), Name: "y"}},
				},
				Rparen:      pos(45),
				Where:       pos(47),
				WhereExpr:   &sql.BoolLit{ValuePos: pos(53), Value: true},
				Do:          pos(58),
				DoUpdate:    pos(61),
				DoUpdateSet: pos(68),
				Assignments: []*sql.Assignment{
					{
						Columns: []*sql.Ident{
							{NamePos: pos(72), Name: "foo"},
						},
						Eq:   pos(76),
						Expr: &sql.NumberLit{ValuePos: pos(78), Value: "1"},
					},
					{
						Lparen: pos(81),
						Columns: []*sql.Ident{
							{NamePos: pos(82), Name: "bar"},
							{NamePos: pos(87), Name: "baz"},
						},
						Rparen: pos(90),
						Eq:     pos(92),
						Expr:   &sql.NumberLit{ValuePos: pos(94), Value: "2"},
					},
				},
				UpdateWhere:     pos(96),
				UpdateWhereExpr: &sql.BoolLit{ValuePos: pos(102), Value: false},
			},
		})

		AssertParseStatementError(t, `INSERT`, `1:6: expected INTO, found 'EOF'`)
		AssertParseStatementError(t, `INSERT OR`, `1:9: expected ROLLBACK, REPLACE, ABORT, FAIL, or IGNORE, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO`, `1:11: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl AS`, `1:18: expected alias, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl `, `1:16: expected VALUES, SELECT, or DEFAULT VALUES, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (`, `1:17: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x`, `1:18: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x)`, `1:19: expected VALUES, SELECT, or DEFAULT VALUES, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES`, `1:26: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (`, `1:28: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1`, `1:29: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) SELECT`, `1:26: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) DEFAULT`, `1:27: expected VALUES, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON`, `1:33: expected CONFLICT, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (`, `1:44: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x`, `1:45: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) WHERE`, `1:52: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x)`, `1:46: expected DO, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO`, `1:49: expected NOTHING or UPDATE SET, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE`, `1:56: expected SET, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo`, `1:64: expected =, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo =`, `1:66: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo = 1 WHERE`, `1:74: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET (`, `1:62: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET (foo`, `1:65: expected comma or right paren, found 'EOF'`)
	})

	t.Run("Update", func(t *testing.T) {
		AssertParseStatement(t, `UPDATE tbl SET x = 1, y = 2`, &sql.UpdateStatement{
			Update: pos(0),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(7), Name: "tbl"},
			},
			Set: pos(11),
			Assignments: []*sql.Assignment{
				{
					Columns: []*sql.Ident{{NamePos: pos(15), Name: "x"}},
					Eq:      pos(17),
					Expr:    &sql.NumberLit{ValuePos: pos(19), Value: "1"},
				},
				{
					Columns: []*sql.Ident{{NamePos: pos(22), Name: "y"}},
					Eq:      pos(24),
					Expr:    &sql.NumberLit{ValuePos: pos(26), Value: "2"},
				},
			},
		})
		AssertParseStatement(t, `UPDATE tbl SET x = 1 WHERE y = 2`, &sql.UpdateStatement{
			Update: pos(0),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(7), Name: "tbl"},
			},
			Set: pos(11),
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{NamePos: pos(15), Name: "x"}},
				Eq:      pos(17),
				Expr:    &sql.NumberLit{ValuePos: pos(19), Value: "1"},
			}},
			Where: pos(21),
			WhereExpr: &sql.BinaryExpr{
				X:     &sql.Ident{NamePos: pos(27), Name: "y"},
				OpPos: pos(29), Op: sql.EQ,
				Y: &sql.NumberLit{ValuePos: pos(31), Value: "2"},
			},
		})
		AssertParseStatement(t, `UPDATE OR ROLLBACK tbl SET x = 1`, &sql.UpdateStatement{
			Update:           pos(0),
			UpdateOr:         pos(7),
			UpdateOrRollback: pos(10),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(19), Name: "tbl"},
			},
			Set: pos(23),
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{NamePos: pos(27), Name: "x"}},
				Eq:      pos(29),
				Expr:    &sql.NumberLit{ValuePos: pos(31), Value: "1"},
			}},
		})
		AssertParseStatement(t, `UPDATE OR ABORT tbl SET x = 1`, &sql.UpdateStatement{
			Update:        pos(0),
			UpdateOr:      pos(7),
			UpdateOrAbort: pos(10),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(16), Name: "tbl"},
			},
			Set: pos(20),
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{NamePos: pos(24), Name: "x"}},
				Eq:      pos(26),
				Expr:    &sql.NumberLit{ValuePos: pos(28), Value: "1"},
			}},
		})
		AssertParseStatement(t, `UPDATE OR REPLACE tbl SET x = 1`, &sql.UpdateStatement{
			Update:          pos(0),
			UpdateOr:        pos(7),
			UpdateOrReplace: pos(10),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(18), Name: "tbl"},
			},
			Set: pos(22),
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{NamePos: pos(26), Name: "x"}},
				Eq:      pos(28),
				Expr:    &sql.NumberLit{ValuePos: pos(30), Value: "1"},
			}},
		})
		AssertParseStatement(t, `UPDATE OR FAIL tbl SET x = 1`, &sql.UpdateStatement{
			Update:       pos(0),
			UpdateOr:     pos(7),
			UpdateOrFail: pos(10),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(15), Name: "tbl"},
			},
			Set: pos(19),
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{NamePos: pos(23), Name: "x"}},
				Eq:      pos(25),
				Expr:    &sql.NumberLit{ValuePos: pos(27), Value: "1"},
			}},
		})
		AssertParseStatement(t, `UPDATE OR IGNORE tbl SET x = 1`, &sql.UpdateStatement{
			Update:         pos(0),
			UpdateOr:       pos(7),
			UpdateOrIgnore: pos(10),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(17), Name: "tbl"},
			},
			Set: pos(21),
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{NamePos: pos(25), Name: "x"}},
				Eq:      pos(27),
				Expr:    &sql.NumberLit{ValuePos: pos(29), Value: "1"},
			}},
		})
		AssertParseStatement(t, `WITH cte (x) AS (SELECT y) UPDATE tbl SET x = 1`, &sql.UpdateStatement{
			WithClause: &sql.WithClause{
				With: pos(0),
				CTEs: []*sql.CTE{
					{
						TableName:     &sql.Ident{NamePos: pos(5), Name: "cte"},
						ColumnsLparen: pos(9),
						Columns: []*sql.Ident{
							{NamePos: pos(10), Name: "x"},
						},
						ColumnsRparen: pos(11),
						As:            pos(13),
						SelectLparen:  pos(16),
						Select: &sql.SelectStatement{
							Select: pos(17),
							Columns: []*sql.ResultColumn{
								{Expr: &sql.Ident{NamePos: pos(24), Name: "y"}},
							},
						},
						SelectRparen: pos(25),
					},
				},
			},
			Update: pos(27),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(34), Name: "tbl"},
			},
			Set: pos(38),
			Assignments: []*sql.Assignment{{
				Columns: []*sql.Ident{{NamePos: pos(42), Name: "x"}},
				Eq:      pos(44),
				Expr:    &sql.NumberLit{ValuePos: pos(46), Value: "1"},
			}},
		})

		AssertParseStatementError(t, `UPDATE`, `1:6: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE OR`, `1:9: expected ROLLBACK, REPLACE, ABORT, FAIL, or IGNORE, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl`, `1:10: expected SET, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET`, `1:14: expected column name or column list, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET x = `, `1:19: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET x = 1 WHERE`, `1:26: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET x = 1 WHERE y =`, `1:30: expected expression, found 'EOF'`)
	})

	t.Run("Delete", func(t *testing.T) {
		AssertParseStatement(t, `DELETE FROM tbl`, &sql.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(12), Name: "tbl"},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl WHERE x = 1`, &sql.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(12), Name: "tbl"},
			},
			Where: pos(16),
			WhereExpr: &sql.BinaryExpr{
				X:     &sql.Ident{NamePos: pos(22), Name: "x"},
				OpPos: pos(24), Op: sql.EQ,
				Y: &sql.NumberLit{ValuePos: pos(26), Value: "1"},
			},
		})
		AssertParseStatement(t, `WITH cte (x) AS (SELECT y) DELETE FROM tbl`, &sql.DeleteStatement{
			WithClause: &sql.WithClause{
				With: pos(0),
				CTEs: []*sql.CTE{
					{
						TableName:     &sql.Ident{NamePos: pos(5), Name: "cte"},
						ColumnsLparen: pos(9),
						Columns: []*sql.Ident{
							{NamePos: pos(10), Name: "x"},
						},
						ColumnsRparen: pos(11),
						As:            pos(13),
						SelectLparen:  pos(16),
						Select: &sql.SelectStatement{
							Select: pos(17),
							Columns: []*sql.ResultColumn{
								{Expr: &sql.Ident{NamePos: pos(24), Name: "y"}},
							},
						},
						SelectRparen: pos(25),
					},
				},
			},
			Delete: pos(27),
			From:   pos(34),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(39), Name: "tbl"},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl ORDER BY x, y LIMIT 1 OFFSET 2`, &sql.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(12), Name: "tbl"},
			},
			Order:   pos(16),
			OrderBy: pos(22),
			OrderingTerms: []*sql.OrderingTerm{
				{X: &sql.Ident{NamePos: pos(25), Name: "x"}},
				{X: &sql.Ident{NamePos: pos(28), Name: "y"}},
			},
			Limit:      pos(30),
			LimitExpr:  &sql.NumberLit{ValuePos: pos(36), Value: "1"},
			Offset:     pos(38),
			OffsetExpr: &sql.NumberLit{ValuePos: pos(45), Value: "2"},
		})
		AssertParseStatement(t, `DELETE FROM tbl LIMIT 1`, &sql.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(12), Name: "tbl"},
			},
			Limit:     pos(16),
			LimitExpr: &sql.NumberLit{ValuePos: pos(22), Value: "1"},
		})
		AssertParseStatement(t, `DELETE FROM tbl LIMIT 1, 2`, &sql.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &sql.QualifiedTableName{
				Name: &sql.Ident{NamePos: pos(12), Name: "tbl"},
			},
			Limit:       pos(16),
			LimitExpr:   &sql.NumberLit{ValuePos: pos(22), Value: "1"},
			OffsetComma: pos(23),
			OffsetExpr:  &sql.NumberLit{ValuePos: pos(25), Value: "2"},
		})

		AssertParseStatementError(t, `DELETE`, `1:6: expected FROM, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM`, `1:11: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl WHERE`, `1:21: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl ORDER `, `1:22: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl ORDER BY`, `1:24: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl ORDER BY x`, `1:26: expected LIMIT, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl LIMIT`, `1:21: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl LIMIT 1,`, `1:24: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl LIMIT 1 OFFSET`, `1:30: expected expression, found 'EOF'`)
	})

	t.Run("AlterTable", func(t *testing.T) {
		AssertParseStatement(t, `ALTER TABLE tbl RENAME TO new_tbl`, &sql.AlterTableStatement{
			Alter:    pos(0),
			Table:    pos(6),
			Name:     &sql.Ident{NamePos: pos(12), Name: "tbl"},
			Rename:   pos(16),
			RenameTo: pos(23),
			NewName:  &sql.Ident{NamePos: pos(26), Name: "new_tbl"},
		})
		AssertParseStatement(t, `ALTER TABLE tbl RENAME COLUMN col TO new_col`, &sql.AlterTableStatement{
			Alter:         pos(0),
			Table:         pos(6),
			Name:          &sql.Ident{NamePos: pos(12), Name: "tbl"},
			Rename:        pos(16),
			RenameColumn:  pos(23),
			ColumnName:    &sql.Ident{NamePos: pos(30), Name: "col"},
			To:            pos(34),
			NewColumnName: &sql.Ident{NamePos: pos(37), Name: "new_col"},
		})
		AssertParseStatement(t, `ALTER TABLE tbl RENAME col TO new_col`, &sql.AlterTableStatement{
			Alter:         pos(0),
			Table:         pos(6),
			Name:          &sql.Ident{NamePos: pos(12), Name: "tbl"},
			Rename:        pos(16),
			ColumnName:    &sql.Ident{NamePos: pos(23), Name: "col"},
			To:            pos(27),
			NewColumnName: &sql.Ident{NamePos: pos(30), Name: "new_col"},
		})
		AssertParseStatement(t, `ALTER TABLE tbl ADD COLUMN col TEXT PRIMARY KEY`, &sql.AlterTableStatement{
			Alter:     pos(0),
			Table:     pos(6),
			Name:      &sql.Ident{NamePos: pos(12), Name: "tbl"},
			Add:       pos(16),
			AddColumn: pos(20),
			ColumnDef: &sql.ColumnDefinition{
				Name: &sql.Ident{Name: "col", NamePos: pos(27)},
				Type: &sql.Type{
					Name: &sql.Ident{Name: "TEXT", NamePos: pos(31)},
				},
				Constraints: []sql.Constraint{
					&sql.PrimaryKeyConstraint{
						Primary: pos(36),
						Key:     pos(44),
					},
				},
			},
		})
		AssertParseStatement(t, `ALTER TABLE tbl ADD col TEXT`, &sql.AlterTableStatement{
			Alter: pos(0),
			Table: pos(6),
			Name:  &sql.Ident{NamePos: pos(12), Name: "tbl"},
			Add:   pos(16),
			ColumnDef: &sql.ColumnDefinition{
				Name: &sql.Ident{Name: "col", NamePos: pos(20)},
				Type: &sql.Type{
					Name: &sql.Ident{Name: "TEXT", NamePos: pos(24)},
				},
			},
		})

		AssertParseStatementError(t, `ALTER`, `1:5: expected TABLE, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE`, `1:11: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl`, `1:15: expected ADD or RENAME, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl RENAME`, `1:22: expected COLUMN keyword or column name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl RENAME TO`, `1:25: expected new table name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl RENAME COLUMN`, `1:29: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl RENAME COLUMN col`, `1:33: expected TO, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl RENAME COLUMN col TO`, `1:36: expected new column name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl ADD`, `1:19: expected COLUMN keyword or column name, found 'EOF'`)
		AssertParseStatementError(t, `ALTER TABLE tbl ADD COLUMN`, `1:26: expected column name, found 'EOF'`)
	})

	t.Run("Analyze", func(t *testing.T) {
		AssertParseStatement(t, `ANALYZE tbl`, &sql.AnalyzeStatement{
			Analyze: pos(0),
			Name:    &sql.Ident{NamePos: pos(8), Name: "tbl"},
		})
		AssertParseStatementError(t, `ANALYZE`, `1:7: expected table or index name, found 'EOF'`)
	})
}

func TestParser_ParseExpr(t *testing.T) {
	t.Run("Ident", func(t *testing.T) {
		AssertParseExpr(t, `fooBAR_123'`, &sql.Ident{NamePos: pos(0), Name: `fooBAR_123`})
	})
	t.Run("StringLit", func(t *testing.T) {
		AssertParseExpr(t, `'foo bar'`, &sql.StringLit{ValuePos: pos(0), Value: `foo bar`})
	})
	t.Run("BlobLit", func(t *testing.T) {
		AssertParseExpr(t, `x'0123'`, &sql.BlobLit{ValuePos: pos(0), Value: `0123`})
	})
	t.Run("Integer", func(t *testing.T) {
		AssertParseExpr(t, `123`, &sql.NumberLit{ValuePos: pos(0), Value: `123`})
	})
	t.Run("Float", func(t *testing.T) {
		AssertParseExpr(t, `123.456`, &sql.NumberLit{ValuePos: pos(0), Value: `123.456`})
	})
	t.Run("Null", func(t *testing.T) {
		AssertParseExpr(t, `NULL`, &sql.NullLit{Pos: pos(0)})
	})
	t.Run("Bool", func(t *testing.T) {
		AssertParseExpr(t, `true`, &sql.BoolLit{ValuePos: pos(0), Value: true})
		AssertParseExpr(t, `false`, &sql.BoolLit{ValuePos: pos(0), Value: false})
	})
	t.Run("Bind", func(t *testing.T) {
		AssertParseExpr(t, `$bar`, &sql.BindExpr{NamePos: pos(0), Name: "$bar"})
	})
	t.Run("UnaryExpr", func(t *testing.T) {
		AssertParseExpr(t, `-123`, &sql.UnaryExpr{OpPos: pos(0), Op: sql.MINUS, X: &sql.NumberLit{ValuePos: pos(1), Value: `123`}})
		AssertParseExprError(t, `-`, `1:1: expected expression, found 'EOF'`)
	})
	t.Run("QualifiedRef", func(t *testing.T) {
		AssertParseExpr(t, `tbl.col`, &sql.QualifiedRef{
			Table:  &sql.Ident{NamePos: pos(0), Name: "tbl"},
			Dot:    pos(3),
			Column: &sql.Ident{NamePos: pos(4), Name: "col"},
		})
		AssertParseExpr(t, `"tbl"."col"`, &sql.QualifiedRef{
			Table:  &sql.Ident{NamePos: pos(0), Name: "tbl", Quoted: true},
			Dot:    pos(5),
			Column: &sql.Ident{NamePos: pos(6), Name: "col", Quoted: true},
		})
		AssertParseExprError(t, `tbl.`, `1:4: expected column name, found 'EOF'`)
	})
	t.Run("Exists", func(t *testing.T) {
		AssertParseExpr(t, `EXISTS (SELECT *)`, &sql.Exists{
			Exists: pos(0),
			Lparen: pos(7),
			Select: &sql.SelectStatement{
				Select: pos(8),
				Columns: []*sql.ResultColumn{
					{Star: pos(15)},
				},
			},
			Rparen: pos(16),
		})
		AssertParseExpr(t, `NOT EXISTS (SELECT *)`, &sql.Exists{
			Not:    pos(0),
			Exists: pos(4),
			Lparen: pos(11),
			Select: &sql.SelectStatement{
				Select: pos(12),
				Columns: []*sql.ResultColumn{
					{Star: pos(19)},
				},
			},
			Rparen: pos(20),
		})
		AssertParseExprError(t, `NOT`, `1:3: expected EXISTS, found 'EOF'`)
		AssertParseExprError(t, `EXISTS`, `1:6: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `EXISTS (`, `1:8: expected SELECT or VALUES, found 'EOF'`)
		AssertParseExprError(t, `EXISTS (SELECT`, `1:14: expected expression, found 'EOF'`)
		AssertParseExprError(t, `EXISTS (SELECT *`, `1:16: expected right paren, found 'EOF'`)
	})
	t.Run("BinaryExpr", func(t *testing.T) {
		AssertParseExpr(t, `1 + 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.PLUS,
			Y: &sql.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 - 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.MINUS,
			Y: &sql.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 * 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.STAR,
			Y: &sql.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 / 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.SLASH,
			Y: &sql.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 % 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.REM,
			Y: &sql.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 || 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.CONCAT,
			Y: &sql.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 << 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.LSHIFT,
			Y: &sql.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 >> 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.RSHIFT,
			Y: &sql.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 & 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.BITAND,
			Y: &sql.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 | 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.BITOR,
			Y: &sql.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 < 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.LT,
			Y: &sql.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 <= 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.LE,
			Y: &sql.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 > 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.GT,
			Y: &sql.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 >= 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.GE,
			Y: &sql.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 = 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.EQ,
			Y: &sql.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 != 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.NE,
			Y: &sql.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `(1 + 2)'`, &sql.ParenExpr{
			Lparen: pos(0),
			X: &sql.BinaryExpr{
				X:     &sql.NumberLit{ValuePos: pos(1), Value: "1"},
				OpPos: pos(3), Op: sql.PLUS,
				Y: &sql.NumberLit{ValuePos: pos(5), Value: "2"},
			},
			Rparen: pos(6),
		})
		AssertParseExprError(t, `(`, `1:1: expected expression, found 'EOF'`)
		AssertParseExpr(t, `1 IS 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.IS,
			Y: &sql.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 IS NOT 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.ISNOT,
			Y: &sql.NumberLit{ValuePos: pos(9), Value: "2"},
		})
		AssertParseExpr(t, `1 LIKE 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.LIKE,
			Y: &sql.NumberLit{ValuePos: pos(7), Value: "2"},
		})
		AssertParseExpr(t, `1 NOT LIKE 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.NOTLIKE,
			Y: &sql.NumberLit{ValuePos: pos(11), Value: "2"},
		})
		AssertParseExpr(t, `1 GLOB 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.GLOB,
			Y: &sql.NumberLit{ValuePos: pos(7), Value: "2"},
		})
		AssertParseExpr(t, `1 NOT GLOB 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.NOTGLOB,
			Y: &sql.NumberLit{ValuePos: pos(11), Value: "2"},
		})
		AssertParseExpr(t, `1 REGEXP 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.REGEXP,
			Y: &sql.NumberLit{ValuePos: pos(9), Value: "2"},
		})
		AssertParseExpr(t, `1 NOT REGEXP 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.NOTREGEXP,
			Y: &sql.NumberLit{ValuePos: pos(13), Value: "2"},
		})
		AssertParseExpr(t, `1 MATCH 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.MATCH,
			Y: &sql.NumberLit{ValuePos: pos(8), Value: "2"},
		})
		AssertParseExpr(t, `1 NOT MATCH 2'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.NOTMATCH,
			Y: &sql.NumberLit{ValuePos: pos(12), Value: "2"},
		})
		AssertParseExprError(t, `1 NOT TABLE`, `1:7: expected IN, LIKE, GLOB, REGEXP, MATCH, or BETWEEN, found 'TABLE'`)
		AssertParseExpr(t, `1 IN (2, 3)'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.IN,
			Y: &sql.ExprList{
				Lparen: pos(5),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(6), Value: "2"},
					&sql.NumberLit{ValuePos: pos(9), Value: "3"},
				},
				Rparen: pos(10),
			},
		})
		AssertParseExpr(t, `1 NOT IN (2, 3)'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.NOTIN,
			Y: &sql.ExprList{
				Lparen: pos(9),
				Exprs: []sql.Expr{
					&sql.NumberLit{ValuePos: pos(10), Value: "2"},
					&sql.NumberLit{ValuePos: pos(13), Value: "3"},
				},
				Rparen: pos(14),
			},
		})
		AssertParseExprError(t, `1 IN 2`, `1:6: expected left paren, found 2`)
		AssertParseExprError(t, `1 IN (`, `1:6: expected expression, found 'EOF'`)
		AssertParseExprError(t, `1 IN (2 3`, `1:9: expected comma or right paren, found 3`)
		AssertParseExpr(t, `1 BETWEEN 2 AND 3'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.BETWEEN,
			Y: &sql.Range{
				X:   &sql.NumberLit{ValuePos: pos(10), Value: "2"},
				And: pos(12),
				Y:   &sql.NumberLit{ValuePos: pos(16), Value: "3"},
			},
		})
		AssertParseExpr(t, `1 NOT BETWEEN 2 AND 3'`, &sql.BinaryExpr{
			X:     &sql.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: sql.NOTBETWEEN,
			Y: &sql.Range{
				X:   &sql.NumberLit{ValuePos: pos(14), Value: "2"},
				And: pos(16),
				Y:   &sql.NumberLit{ValuePos: pos(20), Value: "3"},
			},
		})
		AssertParseExprError(t, `1 BETWEEN`, `1:9: expected expression, found 'EOF'`)
		AssertParseExprError(t, `1 BETWEEN 2`, `1:11: expected range expression, found 'EOF'`)
		AssertParseExprError(t, `1 BETWEEN 2 + 3`, `1:15: expected range expression, found 'EOF'`)
		AssertParseExprError(t, `1 + `, `1:4: expected expression, found 'EOF'`)
	})
	t.Run("Call", func(t *testing.T) {
		AssertParseExpr(t, `sum()`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
		})
		AssertParseExpr(t, `sum(*)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Star:   pos(4),
			Rparen: pos(5),
		})
		AssertParseExpr(t, `sum(foo, 123)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Args: []sql.Expr{
				&sql.Ident{NamePos: pos(4), Name: "foo"},
				&sql.NumberLit{ValuePos: pos(9), Value: "123"},
			},
			Rparen: pos(12),
		})
		AssertParseExpr(t, `sum(distinct 'foo')`, &sql.Call{
			Name:     &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen:   pos(3),
			Distinct: pos(4),
			Args: []sql.Expr{
				&sql.StringLit{ValuePos: pos(13), Value: "foo"},
			},
			Rparen: pos(18),
		})
		AssertParseExpr(t, `sum() filter (where true)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Filter: &sql.FilterClause{
				Filter: pos(6),
				Lparen: pos(13),
				Where:  pos(14),
				X:      &sql.BoolLit{ValuePos: pos(20), Value: true},
				Rparen: pos(24),
			},
		})
		AssertParseExpr(t, `sum() over win1`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Name: &sql.Ident{NamePos: pos(11), Name: "win1"},
			},
		})
		AssertParseExpr(t, `sum() over (win1 partition by foo, bar order by baz ASC NULLS FIRST, biz)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen:      pos(11),
					Base:        &sql.Ident{NamePos: pos(12), Name: "win1"},
					Partition:   pos(17),
					PartitionBy: pos(27),
					Partitions: []sql.Expr{
						&sql.Ident{NamePos: pos(30), Name: "foo"},
						&sql.Ident{NamePos: pos(35), Name: "bar"},
					},
					Order:   pos(39),
					OrderBy: pos(45),
					OrderingTerms: []*sql.OrderingTerm{
						{
							X:          &sql.Ident{NamePos: pos(48), Name: "baz"},
							Asc:        pos(52),
							Nulls:      pos(56),
							NullsFirst: pos(62),
						},
						{
							X: &sql.Ident{NamePos: pos(69), Name: "biz"},
						},
					},
					Rparen: pos(72),
				},
			},
		})
		AssertParseExpr(t, `sum() over (order by baz DESC NULLS LAST)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen:  pos(11),
					Order:   pos(12),
					OrderBy: pos(18),
					OrderingTerms: []*sql.OrderingTerm{
						{
							X:         &sql.Ident{NamePos: pos(21), Name: "baz"},
							Desc:      pos(25),
							Nulls:     pos(30),
							NullsLast: pos(36),
						},
					},
					Rparen: pos(40),
				},
			},
		})
		AssertParseExpr(t, `sum() over (range foo preceding)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen: pos(11),
					Frame: &sql.FrameSpec{
						Range:      pos(12),
						X:          &sql.Ident{NamePos: pos(18), Name: "foo"},
						PrecedingX: pos(22),
					},
					Rparen: pos(31),
				},
			},
		})
		AssertParseExpr(t, `sum() over (rows between foo following and bar preceding)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen: pos(11),
					Frame: &sql.FrameSpec{
						Rows:       pos(12),
						Between:    pos(17),
						X:          &sql.Ident{NamePos: pos(25), Name: "foo"},
						FollowingX: pos(29),
						And:        pos(39),
						Y:          &sql.Ident{NamePos: pos(43), Name: "bar"},
						PrecedingY: pos(47),
					},
					Rparen: pos(56),
				},
			},
		})
		AssertParseExpr(t, `sum() over (rows between foo following and bar following)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen: pos(11),
					Frame: &sql.FrameSpec{
						Rows:       pos(12),
						Between:    pos(17),
						X:          &sql.Ident{NamePos: pos(25), Name: "foo"},
						FollowingX: pos(29),
						And:        pos(39),
						Y:          &sql.Ident{NamePos: pos(43), Name: "bar"},
						FollowingY: pos(47),
					},
					Rparen: pos(56),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups between unbounded preceding and unbounded following)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen: pos(11),
					Frame: &sql.FrameSpec{
						Groups:     pos(12),
						Between:    pos(19),
						UnboundedX: pos(27),
						PrecedingX: pos(37),
						And:        pos(47),
						UnboundedY: pos(51),
						FollowingY: pos(61),
					},
					Rparen: pos(70),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups between current row and current row)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen: pos(11),
					Frame: &sql.FrameSpec{
						Groups:      pos(12),
						Between:     pos(19),
						CurrentX:    pos(27),
						CurrentRowX: pos(35),
						And:         pos(39),
						CurrentY:    pos(43),
						CurrentRowY: pos(51),
					},
					Rparen: pos(54),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups current row exclude no others)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen: pos(11),
					Frame: &sql.FrameSpec{
						Groups:          pos(12),
						CurrentX:        pos(19),
						CurrentRowX:     pos(27),
						Exclude:         pos(31),
						ExcludeNo:       pos(39),
						ExcludeNoOthers: pos(42),
					},
					Rparen: pos(48),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups current row exclude current row)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen: pos(11),
					Frame: &sql.FrameSpec{
						Groups:            pos(12),
						CurrentX:          pos(19),
						CurrentRowX:       pos(27),
						Exclude:           pos(31),
						ExcludeCurrent:    pos(39),
						ExcludeCurrentRow: pos(47),
					},
					Rparen: pos(50),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups current row exclude group)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen: pos(11),
					Frame: &sql.FrameSpec{
						Groups:       pos(12),
						CurrentX:     pos(19),
						CurrentRowX:  pos(27),
						Exclude:      pos(31),
						ExcludeGroup: pos(39),
					},
					Rparen: pos(44),
				},
			},
		})
		AssertParseExpr(t, `sum() over (groups current row exclude ties)`, &sql.Call{
			Name:   &sql.Ident{NamePos: pos(0), Name: "sum"},
			Lparen: pos(3),
			Rparen: pos(4),
			Over: &sql.OverClause{
				Over: pos(6),
				Definition: &sql.WindowDefinition{
					Lparen: pos(11),
					Frame: &sql.FrameSpec{
						Groups:      pos(12),
						CurrentX:    pos(19),
						CurrentRowX: pos(27),
						Exclude:     pos(31),
						ExcludeTies: pos(39),
					},
					Rparen: pos(43),
				},
			},
		})

		AssertParseExprError(t, `sum(`, `1:4: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum(*`, `1:5: expected right paren, found 'EOF'`)
		AssertParseExprError(t, `sum(foo foo`, `1:9: expected comma or right paren, found foo`)
		AssertParseExprError(t, `sum() filter`, `1:12: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `sum() filter (`, `1:14: expected WHERE, found 'EOF'`)
		AssertParseExprError(t, `sum() filter (where`, `1:19: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() filter (where true`, `1:24: expected right paren, found 'EOF'`)
		AssertParseExprError(t, `sum() over`, `1:10: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `sum() over (partition`, `1:21: expected BY, found 'EOF'`)
		AssertParseExprError(t, `sum() over (partition by`, `1:24: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() over (partition by foo foo`, `1:30: expected right paren, found foo`)
		AssertParseExprError(t, `sum() over (order`, `1:17: expected BY, found 'EOF'`)
		AssertParseExprError(t, `sum() over (order by`, `1:20: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() over (order by foo foo`, `1:26: expected right paren, found foo`)
		AssertParseExprError(t, `sum() over (order by foo nulls foo`, `1:32: expected FIRST or LAST, found foo`)
		AssertParseExprError(t, `sum() over (range between`, `1:25: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between unbounded`, `1:35: expected PRECEDING, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between current`, `1:33: expected ROW, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo`, `1:29: expected PRECEDING or FOLLOWING, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo following`, `1:39: expected AND, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo following and`, `1:43: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo following and unbounded`, `1:53: expected FOLLOWING, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo following and current`, `1:51: expected ROW, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range between foo following and foo`, `1:47: expected PRECEDING or FOLLOWING, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range current row exclude`, `1:37: expected NO OTHERS, CURRENT ROW, GROUP, or TIES, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range current row exclude no`, `1:40: expected OTHERS, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range current row exclude current`, `1:45: expected ROW, found 'EOF'`)
		AssertParseExprError(t, `sum() over (range foo following`, `1:23: expected PRECEDING, found 'FOLLOWING'`)
	})

	t.Run("Cast", func(t *testing.T) {
		AssertParseExpr(t, `CAST (1 AS INTEGER)`, &sql.CastExpr{
			Cast:   pos(0),
			Lparen: pos(5),
			X:      &sql.NumberLit{ValuePos: pos(6), Value: "1"},
			As:     pos(8),
			Type:   &sql.Type{Name: &sql.Ident{NamePos: pos(11), Name: "INTEGER"}},
			Rparen: pos(18),
		})
		AssertParseExprError(t, `CAST`, `1:4: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `CAST (`, `1:6: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CAST (1`, `1:7: expected AS, found 'EOF'`)
		AssertParseExprError(t, `CAST (1 AS`, `1:10: expected type name, found 'EOF'`)
		AssertParseExprError(t, `CAST (1 AS INTEGER`, `1:18: expected right paren, found 'EOF'`)
	})

	t.Run("Case", func(t *testing.T) {
		AssertParseExpr(t, `CASE 1 WHEN 2 THEN 3 WHEN 4 THEN 5 ELSE 6 END`, &sql.CaseExpr{
			Case:    pos(0),
			Operand: &sql.NumberLit{ValuePos: pos(5), Value: "1"},
			Blocks: []*sql.CaseBlock{
				{
					When:      pos(7),
					Condition: &sql.NumberLit{ValuePos: pos(12), Value: "2"},
					Then:      pos(14),
					Body:      &sql.NumberLit{ValuePos: pos(19), Value: "3"},
				},
				{
					When:      pos(21),
					Condition: &sql.NumberLit{ValuePos: pos(26), Value: "4"},
					Then:      pos(28),
					Body:      &sql.NumberLit{ValuePos: pos(33), Value: "5"},
				},
			},
			Else:     pos(35),
			ElseExpr: &sql.NumberLit{ValuePos: pos(40), Value: "6"},
			End:      pos(42),
		})
		AssertParseExpr(t, `CASE WHEN 1 THEN 2 END`, &sql.CaseExpr{
			Case: pos(0),
			Blocks: []*sql.CaseBlock{
				{
					When:      pos(5),
					Condition: &sql.NumberLit{ValuePos: pos(10), Value: "1"},
					Then:      pos(12),
					Body:      &sql.NumberLit{ValuePos: pos(17), Value: "2"},
				},
			},
			End: pos(19),
		})
		AssertParseExprError(t, `CASE`, `1:4: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE 1`, `1:6: expected WHEN, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN`, `1:9: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1`, `1:11: expected THEN, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN`, `1:16: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2`, `1:18: expected WHEN, ELSE or END, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2 ELSE`, `1:23: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2 ELSE 3`, `1:25: expected END, found 'EOF'`)
	})

	t.Run("Raise", func(t *testing.T) {
		AssertParseExpr(t, `RAISE(IGNORE)`, &sql.Raise{
			Raise:  pos(0),
			Lparen: pos(5),
			Ignore: pos(6),
			Rparen: pos(12),
		})
		AssertParseExpr(t, `RAISE(ROLLBACK, 'bad error')`, &sql.Raise{
			Raise:    pos(0),
			Lparen:   pos(5),
			Rollback: pos(6),
			Comma:    pos(14),
			Error:    &sql.StringLit{ValuePos: pos(16), Value: "bad error"},
			Rparen:   pos(27),
		})
		AssertParseExpr(t, `RAISE(ABORT, 'error')`, &sql.Raise{
			Raise:  pos(0),
			Lparen: pos(5),
			Abort:  pos(6),
			Comma:  pos(11),
			Error:  &sql.StringLit{ValuePos: pos(13), Value: "error"},
			Rparen: pos(20),
		})
		AssertParseExpr(t, `RAISE(FAIL, 'error')`, &sql.Raise{
			Raise:  pos(0),
			Lparen: pos(5),
			Fail:   pos(6),
			Comma:  pos(10),
			Error:  &sql.StringLit{ValuePos: pos(12), Value: "error"},
			Rparen: pos(19),
		})
		AssertParseExprError(t, `RAISE`, `1:5: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `RAISE(`, `1:6: expected IGNORE, ROLLBACK, ABORT, or FAIL, found 'EOF'`)
		AssertParseExprError(t, `RAISE(IGNORE`, `1:12: expected right paren, found 'EOF'`)
		AssertParseExprError(t, `RAISE(ROLLBACK`, `1:14: expected comma, found 'EOF'`)
		AssertParseExprError(t, `RAISE(ROLLBACK,`, `1:15: expected error message, found 'EOF'`)
	})
}

func TestError_Error(t *testing.T) {
	err := &sql.Error{Msg: "test"}
	if got, want := err.Error(), `test`; got != want {
		t.Fatalf("Error()=%s, want %s", got, want)
	}
}

// ParseStatementOrFail parses a statement from s. Fail on error.
func ParseStatementOrFail(tb testing.TB, s string) sql.Statement {
	tb.Helper()
	stmt, err := sql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		tb.Fatal(err)
	}
	return stmt
}

// AssertParseStatement asserts the value of the first parse of s.
func AssertParseStatement(tb testing.TB, s string, want sql.Statement) {
	tb.Helper()
	stmt, err := sql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		tb.Fatal(err)
	} else if diff := deep.Equal(stmt, want); diff != nil {
		tb.Fatalf("mismatch:\n%s", strings.Join(diff, "\n"))
	}
}

// AssertParseStatementError asserts s parses to a given error string.
func AssertParseStatementError(tb testing.TB, s string, want string) {
	tb.Helper()
	_, err := sql.NewParser(strings.NewReader(s)).ParseStatement()
	if err == nil || err.Error() != want {
		tb.Fatalf("ParseStatement()=%q, want %q", err, want)
	}
}

// ParseExprOrFail parses a expression from s. Fail on error.
func ParseExprOrFail(tb testing.TB, s string) sql.Expr {
	tb.Helper()
	stmt, err := sql.NewParser(strings.NewReader(s)).ParseExpr()
	if err != nil {
		tb.Fatal(err)
	}
	return stmt
}

// AssertParseExpr asserts the value of the first parse of s.
func AssertParseExpr(tb testing.TB, s string, want sql.Expr) {
	tb.Helper()
	stmt, err := sql.NewParser(strings.NewReader(s)).ParseExpr()
	if err != nil {
		tb.Fatal(err)
	} else if diff := deep.Equal(stmt, want); diff != nil {
		tb.Fatalf("mismatch:\n%s", strings.Join(diff, "\n"))
	}
}

// AssertParseExprError asserts s parses to a given error string.
func AssertParseExprError(tb testing.TB, s string, want string) {
	tb.Helper()
	_, err := sql.NewParser(strings.NewReader(s)).ParseExpr()
	if err == nil || err.Error() != want {
		tb.Fatalf("ParseExpr()=%q, want %q", err, want)
	}
}

// pos is a helper function for generating positions based on offset for one-line parsing.
func pos(offset int) sql.Pos {
	return sql.Pos{Offset: offset, Line: 1, Column: offset + 1}
}

func deepEqual(a, b interface{}) string {
	return strings.Join(deep.Equal(a, b), "\n")
}
