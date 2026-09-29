package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mattn/go-sqlite3"
	command "github.com/rqlite/rqlite/v10/command/proto"
)

// Test_DBErrors_Wrapping verifies that classification finds a wrapped SQLite
// error, preserves its message and driver details, and does not wrap it twice.
func Test_DBErrors_Wrapping(t *testing.T) {
	original := sqlite3.Error{
		Code: sqlite3.ErrIoErr, ExtendedCode: sqlite3.ErrIoErrWrite, SystemErrno: syscall.ENOSPC,
	}
	wrapped := fmt.Errorf("statement failed: %w", original)
	got := classifyError(wrapped)
	mustBeFatalSQLiteError(t, got, sqlite3.ErrIoErr)
	if got.Error() != wrapped.Error() || errors.Unwrap(got) != wrapped {
		t.Fatalf("original error lost: %v", got)
	}
	var sqliteErr sqlite3.Error
	if !errors.As(got, &sqliteErr) || sqliteErr != original {
		t.Fatalf("driver details lost: %+v", sqliteErr)
	}
	if classifyError(got) != got {
		t.Fatal("fatal error wrapped twice")
	}
}

// Test_DBErrors_FullStopsBatch verifies that SQLITE_FULL returns a FatalError
// and stops Execute and Request before later statements run. It checks that
// earlier writes persist only outside a transaction, with and without WAL and
// RETURNING. max_page_count triggers the failure without exhausting the host disk.
func Test_DBErrors_FullStopsBatch(t *testing.T) {
	for _, wal := range []bool{false, true} {
		for _, transaction := range []bool{false, true} {
			for _, forceQuery := range []bool{false, true} {
				for _, request := range []bool{false, true} {
					name := fmt.Sprintf("wal=%t/tx=%t/returning=%t/request=%t", wal, transaction, forceQuery, request)
					t.Run(name, func(t *testing.T) {
						db := newErrorTestDB(t, wal)
						mustExecute(db, "CREATE TABLE data (value BLOB)")
						mustExecute(db, "CREATE TABLE marker (value INTEGER)")
						mustExecute(db, "INSERT INTO marker VALUES (0)")

						// Set the max page count to the current page count, which will prevent the database
						// from growing, and trigger the SQLITE_FULL error.
						var pages int
						if err := db.rwDB.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
							t.Fatal(err)
						}
						mustExecute(db, fmt.Sprintf("PRAGMA max_page_count=%d", pages))

						insert := "INSERT INTO data VALUES (zeroblob(1048576))"
						if forceQuery {
							insert += " RETURNING rowid"
						}
						req := &command.Request{Transaction: transaction, Statements: []*command.Statement{
							{Sql: "UPDATE marker SET value=1"},
							{Sql: insert, ForceQuery: forceQuery},
							{Sql: "UPDATE marker SET value=2"},
						}}
						run := db.Execute
						if request {
							run = db.Request
						}
						results, err := run(req, false)
						mustBeFatalSQLiteError(t, err, sqlite3.ErrFull)
						if len(results) != 2 || results[1].GetError() == "" || results[1].GetMutated() {
							t.Fatalf("unexpected results: %v", results)
						}
						want := 1
						if transaction {
							want = 0
						}
						var got int
						if err := db.roDB.QueryRow("SELECT value FROM marker").Scan(&got); err != nil || got != want {
							t.Fatalf("marker=%d, want %d; error=%v", got, want, err)
						}
					})
				}
			}
		}
	}
}

// Test_DBErrors_OrdinarySQL verifies that constraint violations remain statement
// errors: nontransactional batches continue, while transactional batches stop.
// It also checks that writes attempted through Query still report ErrQueryWrite.
func Test_DBErrors_OrdinarySQL(t *testing.T) {
	for _, request := range []bool{false, true} {
		for _, transaction := range []bool{false, true} {
			t.Run(fmt.Sprintf("request=%t/tx=%t", request, transaction), func(t *testing.T) {
				db := newErrorTestDB(t, true)
				mustExecute(db, "CREATE TABLE data (id INTEGER PRIMARY KEY)")
				mustExecute(db, "INSERT INTO data VALUES (1)")
				run := db.Execute
				if request {
					run = db.Request
				}
				results, err := run(&command.Request{Transaction: transaction, Statements: []*command.Statement{
					{Sql: "INSERT INTO data VALUES (1)"},
					{Sql: "INSERT INTO data VALUES (2)"},
				}}, false)
				want := 2
				if transaction {
					want = 1
				}
				if err != nil || len(results) != want || results[0].GetError() == "" {
					t.Fatalf("ordinary constraint behavior changed: %v, %v", results, err)
				}
			})
		}
	}
	db := newErrorTestDB(t, true)
	mustExecute(db, "CREATE TABLE data (id INTEGER)")
	rows, err := db.QueryStringStmt("INSERT INTO data VALUES (1)")
	if err != nil || len(rows) != 1 || rows[0].Error != ErrQueryWrite.Error() {
		t.Fatalf("read-only query behavior changed: %v, %v", rows, err)
	}
}

// Test_DBErrors_Prepare verifies that a SQLite allocation failure during
// preparation propagates as a FatalError from StmtReadOnly and stops Request
// before it processes subsequent statements.
func Test_DBErrors_Prepare(t *testing.T) {
	db := newErrorTestDB(t, false)
	mustExecute(db, "CREATE TABLE data (id INTEGER)")
	// SQLite compiles each SQL statement into a virtual-machine program during
	// preparation. SQLITE_LIMIT_VDBE_OP limits the number of instructions it can
	// allocate for that program, not the number executed at runtime. Exceeding
	// the limit returns SQLITE_NOMEM. Setting it to one makes even our simple
	// statements fail preparation without exhausting actual memory.
	for _, pool := range []*sql.DB{db.rwDB, db.roDB} {
		conn, err := pool.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		err = conn.Raw(func(raw any) error {
			raw.(*sqlite3.SQLiteConn).SetLimit(sqlite3.SQLITE_LIMIT_VDBE_OP, 1)
			return nil
		})
		conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := db.StmtReadOnly("SELECT * FROM data")
	mustBeFatalSQLiteError(t, err, sqlite3.ErrNomem)
	results, err := db.Request(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO data VALUES (1)"},
		{Sql: "SELECT 1"},
	}}, false)
	mustBeFatalSQLiteError(t, err, sqlite3.ErrNomem)
	if len(results) != 1 {
		t.Fatalf("request continued after prepare failure: %v", results)
	}
}

// Test_DBErrors_DriverFailures injects errors at connection acquisition,
// transaction begin and commit, statement execution, row iteration and closure,
// and rollback on write-capable paths, including Execute with ForceQuery. It
// verifies fatal propagation and batch termination, including
// promotion of a fatal rollback error after an ordinary statement error and
// preservation of an earlier fatal error when rollback also fails.
func Test_DBErrors_DriverFailures(t *testing.T) {
	ioErr := sqlite3.Error{Code: sqlite3.ErrIoErr, ExtendedCode: sqlite3.ErrIoErrFsync}
	constraint := sqlite3.Error{Code: sqlite3.ErrConstraint}

	for _, api := range []string{"execute", "execute_returning", "request"} {
		for _, stage := range []string{"connect", "begin", "commit", "statement", "next", "close", "rollback"} {
			// Request uses a concrete SQLite connection for statement preparation;
			// its statement paths are exercised by the real SQLite tests above.
			if api == "request" && stage != "connect" && stage != "begin" && stage != "commit" {
				continue
			}
			if api == "execute" && (stage == "next" || stage == "close") {
				continue
			}
			t.Run(api+"/"+stage, func(t *testing.T) {
				conn := &errorTestConn{}
				switch stage {
				case "connect":
					conn.connectErr = ioErr
				case "begin":
					conn.beginErr = ioErr
				case "commit":
					conn.commitErr = ioErr
				case "statement":
					conn.statementErr = ioErr
				case "next":
					conn.nextErr = ioErr
				case "close":
					conn.closeRowsErr = ioErr
				case "rollback":
					conn.rollbackErr = ioErr
					conn.statementErr = constraint
					if api == "execute_returning" {
						// A fatal query-path error must survive rollback failure.
						conn.statementErr = sqlite3.Error{Code: sqlite3.ErrFull}
					}
				}
				sqldb := sql.OpenDB(conn)
				defer sqldb.Close()
				db := &DB{rwDB: sqldb, roDB: sqldb}
				req := &command.Request{Transaction: stage == "begin" || stage == "commit" || stage == "rollback"}
				if api != "request" {
					req.Statements = []*command.Statement{
						{Sql: "first", ForceQuery: api == "execute_returning"},
						{Sql: "second", ForceQuery: api == "execute_returning"},
					}
				}
				var err error
				switch api {
				case "execute", "execute_returning":
					_, err = db.Execute(req, false)
				case "request":
					_, err = db.Request(req, false)
				}
				want := sqlite3.ErrIoErr
				if api == "execute_returning" && stage == "rollback" {
					want = sqlite3.ErrFull // Preserve the original fatal error.
				}
				mustBeFatalSQLiteError(t, err, want)
				if stage == "statement" || stage == "next" || stage == "close" || stage == "rollback" {
					if conn.statements != 1 {
						t.Fatalf("executed %d statements after failure", conn.statements)
					}
				}
			})
		}
	}
}

// Test_DBErrors_ExplicitRollback verifies that a fatal failure of the explicit
// ROLLBACK issued by RollbackOnError reaches the caller even when the original
// statement error is nonfatal, and that no later batch statement executes.
func Test_DBErrors_ExplicitRollback(t *testing.T) {
	conn := &errorTestConn{
		statementErr: sqlite3.Error{Code: sqlite3.ErrConstraint},
		rollbackErr:  sqlite3.Error{Code: sqlite3.ErrIoErr},
	}
	sqldb := sql.OpenDB(conn)
	defer sqldb.Close()
	db := &DB{rwDB: sqldb}
	_, err := db.Execute(&command.Request{RollbackOnError: true, Statements: []*command.Statement{
		{Sql: "first"}, {Sql: "second"},
	}}, false)
	mustBeFatalSQLiteError(t, err, sqlite3.ErrIoErr)
	if conn.statements != 1 {
		t.Fatalf("executed %d statements after failure", conn.statements)
	}
}

// Test_DBErrors_QueryErrors verifies that busy, locked, and I/O errors remain
// ordinary statement errors in read-only Query batches, including journal-mode
// changes, but become fatal errors that stop Execute batches.
func Test_DBErrors_QueryErrors(t *testing.T) {
	for _, code := range []sqlite3.ErrNo{sqlite3.ErrBusy, sqlite3.ErrLocked, sqlite3.ErrIoErr} {
		t.Run(fmt.Sprint(int(code)), func(t *testing.T) {
			conn := &errorTestConn{statementErr: sqlite3.Error{Code: code}}
			sqldb := sql.OpenDB(conn)
			defer sqldb.Close()

			db := &DB{rwDB: sqldb, roDB: sqldb}

			req := &command.Request{Statements: []*command.Statement{
				{Sql: "PRAGMA journal_mode=DELETE"}, {Sql: "SELECT 1"},
			}}
			rows, err := db.Query(req, false)
			if err != nil || len(rows) != 2 || rows[0].Error != conn.statementErr.Error() || rows[1].Error != conn.statementErr.Error() {
				t.Fatalf("read error behavior changed: %v, %v", rows, err)
			}
			if conn.statements != 2 {
				t.Fatalf("expected both queries to reach the driver, got %d", conn.statements)
			}

			results, err := db.Execute(req, false)
			mustBeFatalSQLiteError(t, err, code)
			if len(results) != 1 {
				t.Fatalf("execute continued after failure: %v", results)
			}
		})
	}
}

// Test_DBErrors_QueryDriverFailures verifies that connection and transaction
// errors retain their original type in Query, while row iteration and closure
// failures remain statement errors and allow later queries to run.
func Test_DBErrors_QueryDriverFailures(t *testing.T) {
	ioErr := sqlite3.Error{Code: sqlite3.ErrIoErr, ExtendedCode: sqlite3.ErrIoErrRead}
	for _, stage := range []string{"connect", "begin", "commit", "next", "close"} {
		t.Run(stage, func(t *testing.T) {
			conn := &errorTestConn{}
			switch stage {
			case "connect":
				conn.connectErr = ioErr
			case "begin":
				conn.beginErr = ioErr
			case "commit":
				conn.commitErr = ioErr
			case "next":
				conn.nextErr = ioErr
			case "close":
				conn.closeRowsErr = ioErr
			}
			sqldb := sql.OpenDB(conn)
			defer sqldb.Close()
			db := &DB{roDB: sqldb}
			rows, err := db.Query(&command.Request{
				Transaction: stage == "begin" || stage == "commit",
				Statements:  []*command.Statement{{Sql: "SELECT 1"}, {Sql: "SELECT 2"}},
			}, false)
			if stage == "next" || stage == "close" {
				if err != nil || len(rows) != 2 || rows[0].Error != ioErr.Error() || rows[1].Error != ioErr.Error() || conn.statements != 2 {
					t.Fatalf("expected ordinary row errors and batch continuation, got %v, %v", rows, err)
				}
			} else if err != ioErr {
				t.Fatalf("expected original driver error, got %T: %v", err, err)
			}
		})
	}
}

func newErrorTestDB(t *testing.T, wal bool) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "test.db"), false, wal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustBeFatalSQLiteError(t *testing.T, err error, code sqlite3.ErrNo) {
	t.Helper()
	var fatal *FatalError
	if !errors.As(err, &fatal) {
		t.Fatalf("expected *FatalError, got %T: %v", err, err)
	}
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code != code {
		t.Fatalf("expected SQLite code %d, got %v", code, err)
	}
}

// errorTestConn injects failures at database/sql boundaries that are difficult
// to trigger reliably with real SQLite (particularly commit and cleanup).
type errorTestConn struct {
	connectErr, beginErr, commitErr, rollbackErr error
	statementErr, nextErr, closeRowsErr          error
	statements                                   int
}

func (c *errorTestConn) Connect(context.Context) (driver.Conn, error) { return c, c.connectErr }
func (c *errorTestConn) Driver() driver.Driver                        { return c }
func (c *errorTestConn) Open(string) (driver.Conn, error)             { return c, c.connectErr }
func (c *errorTestConn) Close() error                                 { return nil }
func (c *errorTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *errorTestConn) Begin() (driver.Tx, error) { return c, c.beginErr }
func (c *errorTestConn) Commit() error             { return c.commitErr }
func (c *errorTestConn) Rollback() error           { return c.rollbackErr }
func (c *errorTestConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if query == "ROLLBACK" {
		return nil, c.rollbackErr
	}
	c.statements++
	return errorTestResult{}, c.statementErr
}
func (c *errorTestConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	c.statements++
	return &errorTestRows{conn: c}, c.statementErr
}

type errorTestRows struct{ conn *errorTestConn }

type errorTestResult struct{}

func (errorTestResult) LastInsertId() (int64, error) { return 1, nil }
func (errorTestResult) RowsAffected() (int64, error) { return 1, nil }

func (r *errorTestRows) Columns() []string { return []string{"value"} }
func (r *errorTestRows) Close() error      { return r.conn.closeRowsErr }
func (r *errorTestRows) Next([]driver.Value) error {
	if r.conn.nextErr != nil {
		return r.conn.nextErr
	}
	return io.EOF
}
