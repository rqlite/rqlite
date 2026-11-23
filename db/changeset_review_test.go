package db

import (
	"context"
	"errors"
	"testing"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

// Regression tests for two independent reviews of the prototype. The invariant:
// nothing is ever committed outside the Raft log, and a write the client is told
// succeeded is on every node.

func countRows(db *DB, q string) int64 {
	return mustQuery(db, q)[0].Values[0].Parameters[0].GetI()
}

func wantNotCapturable(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrNotCapturable) {
		t.Fatalf("expected ErrNotCapturable, got %v", err)
	}
}

// A statement that ends the capture transaction must not let later statements
// commit on the capturing node.
func Test_Review_OrRollbackDoesNotLeak(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)", "CREATE TABLE bar (v TEXT)", "CREATE TABLE log (v TEXT)")
	mustExecute(a, "INSERT INTO foo VALUES(1)")
	_, _, err := a.ExecuteCapture(context.Background(), csRequest(false,
		"INSERT OR ROLLBACK INTO foo VALUES(1)",
		"INSERT INTO bar VALUES('leaked')"), false)
	wantNotCapturable(t, err)
	if n := countRows(a, "SELECT count(*) FROM bar"); n != 0 {
		t.Fatalf("capture committed %d row(s)", n)
	}
	// Same, with both statements in one SQL string.
	_, _, err = a.ExecuteCapture(context.Background(), csRequest(false,
		"INSERT OR ROLLBACK INTO foo VALUES(1); INSERT INTO log VALUES('after')"), false)
	wantNotCapturable(t, err)
	if n := countRows(a, "SELECT count(*) FROM log"); n != 0 {
		t.Fatalf("capture committed %d row(s) from a tail statement", n)
	}
}

// RAISE(ROLLBACK) in a trigger ends the transaction too.
func Test_Review_RaiseRollbackDoesNotLeak(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)", "CREATE TABLE bar (v TEXT)",
		"CREATE TRIGGER tr BEFORE INSERT ON foo WHEN new.v = 'boom' BEGIN SELECT RAISE(ROLLBACK, 'boom'); END")
	_, _, err := a.ExecuteCapture(context.Background(), csRequest(false,
		"INSERT INTO foo(v) VALUES('boom')",
		"INSERT INTO bar VALUES('leaked')"), false)
	wantNotCapturable(t, err)
	if n := countRows(a, "SELECT count(*) FROM bar"); n != 0 {
		t.Fatalf("capture committed %d row(s)", n)
	}
}

// Transaction control smuggled behind another statement, a leading ';' or a
// comment must not commit anything.
func Test_Review_CommitSmuggle(t *testing.T) {
	for _, stmts := range [][]string{
		{"INSERT INTO bar VALUES('leaked'); COMMIT"},
		{"INSERT INTO bar VALUES('leaked'); COMMIT; INSERT INTO bar VALUES('b')"},
		{"INSERT INTO bar VALUES('leaked')", ";COMMIT"},
		{"INSERT INTO bar VALUES('leaked')", "/**/;END"},
		{"INSERT INTO bar VALUES('leaked'); ROLLBACK"},
		{"INSERT INTO bar VALUES('leaked'); SAVEPOINT x; RELEASE x"},
	} {
		a, _ := newPair(t, "CREATE TABLE bar (v TEXT)")
		_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, stmts...), false)
		wantNotCapturable(t, err)
		if n := countRows(a, "SELECT count(*) FROM bar"); n != 0 {
			t.Errorf("%q: capture committed %d row(s)", stmts, n)
		}
	}
}

// A PRAGMA smuggled into a capturable request must not stay on the writer.
func Test_Review_PragmaSmuggle(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE chk (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 0))")
	_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, "SELECT 1; PRAGMA ignore_check_constraints=1"), false)
	wantNotCapturable(t, err)
	res, cs, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO chk VALUES(1, -5)"), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 0 || res[0].GetE().GetError() == "" {
		t.Fatalf("the CHECK constraint was ignored: cs=%d res=%s", len(cs), asJSON(res))
	}
}

// Attach and detach, DDL behind another statement.
func Test_Review_DeniedStatementsFallBack(t *testing.T) {
	for _, s := range []string{
		"SELECT 1; ATTACH DATABASE ':memory:' AS m",
		"SELECT 1; CREATE TABLE x (a)",
		"SELECT 1; DROP TABLE bar",
		"SELECT 1; ALTER TABLE bar RENAME TO baz",
		"SELECT 1; CREATE TEMP TABLE t2 (a)",
	} {
		a, _ := newPair(t, "CREATE TABLE bar (id INTEGER PRIMARY KEY, v TEXT)")
		_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, s), false)
		wantNotCapturable(t, err)
		if n := countRows(a, "SELECT count(*) FROM sqlite_master"); n != 1 {
			t.Errorf("%q: schema changed (%d objects)", s, n)
		}
	}
}

// A row with NULL in a non-INTEGER primary key is not recorded by the session
// extension: the write must take the statement path, which stores it.
func Test_Review_NullPKNotCaptured(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (k TEXT PRIMARY KEY, v TEXT)")
	_, cs, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo(k, v) VALUES(NULL, 'x')"), false)
	if err == nil {
		t.Fatalf("a write the changeset does not record was captured (cs=%d bytes)", len(cs))
	}
	wantNotCapturable(t, err)
}

// Writes to a TEMP table are not recorded.
func Test_Review_TempTableNotCaptured(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	mustExecute(a, "CREATE TEMP TABLE tt (id INTEGER PRIMARY KEY, v TEXT)")
	_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO temp.tt VALUES(1, 'x')"), false)
	wantNotCapturable(t, err)
}

// Tables keyed by rowid have no stable key (VACUUM renumbers them per node).
func Test_Review_RowidTableNotCaptured(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (v TEXT)")
	_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo VALUES('a')"), false)
	wantNotCapturable(t, err)
	// A table reached only through a trigger counts too.
	b, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)", "CREATE TABLE log (v TEXT)",
		"CREATE TRIGGER tr AFTER INSERT ON foo BEGIN INSERT INTO log VALUES('x'); END")
	_, _, err = b.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo VALUES(1)"), false)
	wantNotCapturable(t, err)
	// And one reached through a foreign key action.
	c, _ := newPair(t, "CREATE TABLE p (id INTEGER PRIMARY KEY)",
		"CREATE TABLE ch (pid INTEGER REFERENCES p(id) ON DELETE CASCADE, v TEXT)")
	mustExecute(c, "PRAGMA foreign_keys=ON")
	mustExecute(c, "INSERT INTO p VALUES(1)")
	mustExecute(c, "INSERT INTO ch VALUES(1, 'x')")
	_, _, err = c.ExecuteCapture(context.Background(), csRequest(false, "DELETE FROM p"), false)
	wantNotCapturable(t, err)
}

// A nullable-key table with a rowid alias or NOT NULL key columns stays capturable.
func Test_Review_KeyedTablesStayCapturable(t *testing.T) {
	a, b := newPair(t,
		"CREATE TABLE t1 (id INTEGER PRIMARY KEY, v TEXT)",
		"CREATE TABLE t2 (k TEXT NOT NULL PRIMARY KEY, v TEXT)",
		"CREATE TABLE t3 (k TEXT, j INTEGER, v, PRIMARY KEY (k, j)) WITHOUT ROWID")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO t1(v) VALUES('a')", "INSERT INTO t2 VALUES('k','v')", "INSERT INTO t3 VALUES('k',1,'v')"))
	applyBoth(t, cs, a, b)
	if dump(t, a) != dump(t, b) {
		t.Fatal("databases differ")
	}
}

// Applying a changeset to a node that lacks the table, or has another shape,
// must be an error, never a silent skip.
func Test_Review_SchemaMismatchIsLoud(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1, 'x')"))
	b, _ := mustCreateOnDiskDatabaseWAL()
	defer b.Close()
	if err := b.ApplyChangeset(context.Background(), cs); err == nil {
		t.Fatal("changeset for a missing table applied with a nil error")
	}
	mustExecute(b, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	if err := b.ApplyChangeset(context.Background(), cs); err == nil {
		t.Fatal("changeset for a table with fewer columns applied with a nil error")
	}
	mustExecute(b, "DROP TABLE foo")
	mustExecute(b, "CREATE TABLE foo (id INTEGER, v TEXT)")
	if err := b.ApplyChangeset(context.Background(), cs); err == nil {
		t.Fatal("changeset whose key columns differ applied with a nil error")
	}
	// A view of the same name is not a table.
	mustExecute(b, "DROP TABLE foo")
	mustExecute(b, "CREATE VIEW foo AS SELECT 1 AS id, 'x' AS v")
	if err := b.ApplyChangeset(context.Background(), cs); err == nil {
		t.Fatal("changeset for a view applied with a nil error")
	}
}

// Tables with generated columns have the same shape on every node.
func Test_Review_GeneratedColumnsApply(t *testing.T) {
	a, b := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v INTEGER, g INTEGER GENERATED ALWAYS AS (v*2) STORED, h INTEGER AS (v+1) VIRTUAL)")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo(v) VALUES(3)"))
	applyBoth(t, cs, a, b)
	if dump(t, a) != dump(t, b) {
		t.Fatalf("databases differ:\n%s\nvs\n%s", dump(t, a), dump(t, b))
	}
}

// A malformed changeset is refused with an ordinary error: the database is not
// at fault, and the refusal is the same on every node.
func Test_Review_MalformedChangesetIsNotFatal(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	for _, cs := range [][]byte{
		[]byte("T\x02\x01\x00foo\x00\x12\x00\x01"),
		[]byte("garbage that is not a changeset"),
	} {
		err := a.ApplyChangeset(context.Background(), cs)
		if err == nil {
			t.Fatalf("%q applied", cs)
		}
		var fe *FatalError
		if errors.As(err, &fe) {
			t.Fatalf("%q: a malformed changeset is fatal: %v", cs, err)
		}
	}
}

// Every proper prefix of a changeset either is refused or ends on a change
// boundary, in which case it must apply exactly its own changes. Nothing else.
func Test_Review_TruncatedChangeset(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT, w BLOB)")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1, 'hello', x'0102')", "INSERT INTO foo VALUES(2, 'world', NULL)"))
	for i := 1; i < len(cs); i++ {
		b, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT, w BLOB)")
		if err := b.ApplyChangeset(context.Background(), cs[:i]); err == nil {
			if n := countRows(b, "SELECT count(*) FROM foo"); n > 1 { // 0 or 1: a cut on a change boundary is a shorter changeset (documented)
				t.Fatalf("prefix %d applied with %d rows", i, n)
			}
		}
	}
}

// After a refused apply the triggers of the connection are back on.
func Test_Review_TriggersAfterConflict(t *testing.T) {
	a, b := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)", "CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)",
		"CREATE TRIGGER tr AFTER INSERT ON foo BEGIN INSERT INTO log(v) VALUES(new.v); END")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1, 'x')"))
	mustExecute(b, "INSERT INTO foo VALUES(1, 'other')")
	if err := b.ApplyChangeset(context.Background(), cs); err == nil {
		t.Fatal("expected a conflict")
	}
	mustExecute(b, "INSERT INTO foo VALUES(2, 'y')")
	if got := countRows(b, "SELECT count(*) FROM log"); got != 2 {
		t.Fatalf("triggers off after a failed apply: log rows=%d", got)
	}
}

// AUTOINCREMENT bookkeeping agrees with the statement path.
func Test_Review_AutoincrementAgrees(t *testing.T) {
	schema := "CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"
	a, b := newPair(t, schema)
	ref, _ := newPair(t, schema)
	req := csRequest(false, "INSERT INTO foo(v) VALUES('a')", "DELETE FROM foo", "INSERT INTO foo(v) VALUES('b')")
	_, cs, err := a.ExecuteCapture(context.Background(), req, false)
	if errors.Is(err, ErrNotCapturable) {
		// The session coalesces the insert and the delete: the count check refuses.
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	applyBoth(t, cs, a, b)
	if _, err := ref.Execute(req, false); err != nil {
		t.Fatal(err)
	}
	if dump(t, a) != dump(t, ref) || dump(t, b) != dump(t, ref) {
		t.Fatalf("changeset path:\n%sstatement path:\n%s", dump(t, a), dump(t, ref))
	}
}

// INSERT OR REPLACE that removes another row through a UNIQUE index.
func Test_Review_ReplaceAgrees(t *testing.T) {
	a, b := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, u TEXT UNIQUE, v TEXT)")
	for _, d := range []*DB{a, b} {
		mustExecute(d, "INSERT INTO foo VALUES(1, 'k', 'old')")
	}
	_, cs, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT OR REPLACE INTO foo VALUES(2, 'k', 'new')"), false)
	if errors.Is(err, ErrNotCapturable) {
		// The REPLACE delete is recorded by the session and not counted by SQLite:
		// the count check refuses. Round3 covers that it agrees with the statements.
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	applyBoth(t, cs, a, b)
	if dump(t, a) != dump(t, b) || countRows(a, "SELECT count(*) FROM foo") != 1 {
		t.Fatalf("a:\n%sb:\n%s", dump(t, a), dump(t, b))
	}
}

// CDC and changeset capture share the one preupdate hook slot of SQLite. The
// combination is refused rather than crashing the process.
func Test_Review_CDCHookRefusesCapture(t *testing.T) {
	var hook PreUpdateHookCallback = func(ev *command.CDCEvent) error { return nil }
	cfg := NewConfig(ConnConfig{PreUpdateHook: hook}, true)
	a, err := OpenWithConfig(mustTempFile(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	mustExecute(a, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	if _, _, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo(v) VALUES('x')"), false); err == nil {
		t.Fatal("capture with a CDC hook installed was accepted")
	}
	if _, _, err := a.RequestCapture(context.Background(), csRequest(false, "INSERT INTO foo(v) VALUES('x')"), false); err == nil {
		t.Fatal("request capture with a CDC hook installed was accepted")
	}
}
