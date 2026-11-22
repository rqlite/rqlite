package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

func csRequest(tx bool, stmts ...string) *command.Request {
	r := &command.Request{Transaction: tx}
	for _, s := range stmts {
		r.Statements = append(r.Statements, &command.Statement{Sql: s})
	}
	return r
}

// mustCapture captures the request on db and fails the test if that does not
// yield a changeset.
func mustCapture(t *testing.T, db *DB, req *command.Request) ([]*command.ExecuteQueryResponse, []byte) {
	t.Helper()
	res, cs, err := db.ExecuteCapture(context.Background(), req, false)
	if err != nil {
		t.Fatalf("capture failed: %s", err)
	}
	return res, cs
}

// dump returns every row of every table, in a deterministic order, as text.
func dump(t *testing.T, db *DB) string {
	t.Helper()
	rows := mustQuery(db, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	var sb strings.Builder
	for _, v := range rows[0].Values {
		name := v.Parameters[0].GetS()
		// rowid is part of the observable state of every table, so include it.
		q := fmt.Sprintf(`SELECT rowid, * FROM "%s" ORDER BY rowid`, name)
		if r, err := db.QueryStringStmt(q); err == nil && r[0].Error == "" {
			fmt.Fprintf(&sb, "%s: %s\n", name, asJSON(r[0].Values))
		} else {
			// WITHOUT ROWID table.
			r := mustQuery(db, fmt.Sprintf(`SELECT * FROM "%s" ORDER BY 1`, name))
			fmt.Fprintf(&sb, "%s: %s\n", name, asJSON(r[0].Values))
		}
	}
	return sb.String()
}

func newPair(t *testing.T, schema ...string) (*DB, *DB) {
	t.Helper()
	a, _ := mustCreateOnDiskDatabaseWAL()
	b, _ := mustCreateOnDiskDatabaseWAL()
	t.Cleanup(func() { a.Close(); b.Close() })
	for _, s := range schema {
		mustExecute(a, s)
		mustExecute(b, s)
	}
	return a, b
}

func applyBoth(t *testing.T, cs []byte, dbs ...*DB) {
	t.Helper()
	for _, d := range dbs {
		if err := d.ApplyChangeset(context.Background(), cs); err != nil {
			t.Fatalf("apply failed: %s", err)
		}
	}
}

// Test_Changeset_CaptureDoesNotModify checks the capture leaves the database
// untouched.
func Test_Changeset_CaptureDoesNotModify(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	before := dump(t, a)
	res, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo(v) VALUES('x')"))
	if len(cs) == 0 {
		t.Fatal("expected a changeset")
	}
	if got := res[0].GetE().GetRowsAffected(); got != 1 {
		t.Fatalf("rows affected: got %d", got)
	}
	if after := dump(t, a); after != before {
		t.Fatalf("capture modified the database:\n%s\nvs\n%s", before, after)
	}
	if in, _ := func() (bool, error) {
		c, err := a.AcquireWriter(context.Background())
		if err != nil {
			return false, err
		}
		defer c.Close()
		return c.InTransaction()
	}(); in {
		t.Fatal("transaction left open")
	}
}

// Test_Changeset_NonDeterministic is the heart of the matter: statements that
// the SQL-rewriting path cannot make deterministic (#3055 and friends) leave
// identical databases once the changeset is applied.
func Test_Changeset_NonDeterministic(t *testing.T) {
	a, b := newPair(t,
		"CREATE TABLE foo (id INTEGER PRIMARY KEY, r INTEGER, b BLOB, t TEXT, d TEXT DEFAULT (datetime('now')))")
	stmts := []string{
		"INSERT INTO foo(r, b, t) VALUES(random(), randomblob(8), datetime('now'))",
		"INSERT INTO foo(r, b) VALUES(random(), randomblob(2*4))",
		"INSERT INTO foo(r, b) VALUES(random(), randomblob(8.0))",
		"INSERT INTO foo(r, b, t) SELECT random(), randomblob(16), strftime('%f','now') FROM foo",
		"UPDATE foo SET r = random(), b = randomblob(4) WHERE id % 2 = 0",
	}
	for _, s := range stmts {
		_, cs := mustCapture(t, a, csRequest(false, s))
		applyBoth(t, cs, a, b)
	}
	// With a bound parameter, the exact case of #3055.
	req := &command.Request{Statements: []*command.Statement{{
		Sql:        "INSERT INTO foo(b) VALUES(randomblob(?))",
		Parameters: []*command.Parameter{{Value: &command.Parameter_I{I: 8}}},
	}}}
	_, cs := mustCapture(t, a, req)
	applyBoth(t, cs, a, b)

	da, db := dump(t, a), dump(t, b)
	if da != db {
		t.Fatalf("databases differ:\n%s\nvs\n%s", da, db)
	}
	if !strings.Contains(da, "foo:") {
		t.Fatal("empty dump")
	}
}

// Test_Changeset_Results checks rows_affected and last_insert_id match those of
// the statement path.
func Test_Changeset_Results(t *testing.T) {
	schema := "CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"
	stmts := []string{
		"INSERT INTO foo(v) VALUES('a')",
		"INSERT INTO foo(v) VALUES('b'),('c'),('d')",
		"UPDATE foo SET v='z' WHERE id > 1",
		"DELETE FROM foo WHERE id = 1",
		"DELETE FROM foo WHERE id = 100",
	}
	cap, _ := newPair(t, schema)
	ref, _ := newPair(t, schema)
	for _, s := range stmts {
		cres, cs := mustCapture(t, cap, csRequest(false, s))
		applyBoth(t, cs, cap)
		rres, err := ref.Execute(csRequest(false, s), false)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(cres[0].GetE().String(), rres[0].GetE().String()) {
			t.Fatalf("%s: capture %v, statement path %v", s, cres[0].GetE(), rres[0].GetE())
		}
	}
	if dump(t, cap) != dump(t, ref) {
		t.Fatal("databases differ")
	}
}

// Test_Changeset_Probe records how the session extension treats the cases the
// design depends on. Each subtest requires that, after applying the changeset to
// a second database, both databases are identical.
func Test_Changeset_Replicates(t *testing.T) {
	tests := []struct {
		name   string
		schema []string
		stmts  []string
	}{
		{
			name:   "text primary key",
			schema: []string{"CREATE TABLE foo (a TEXT NOT NULL PRIMARY KEY, b INTEGER)"},
			stmts: []string{
				"INSERT INTO foo VALUES('x', random())",
				"INSERT INTO foo VALUES('y', 2)",
				"UPDATE foo SET b = random() WHERE a = 'y'",
				"DELETE FROM foo WHERE a = 'x'",
			},
		},
		{
			name:   "autoincrement sqlite_sequence",
			schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"},
			stmts: []string{
				"INSERT INTO foo(v) VALUES('a')",
				"INSERT INTO foo(v) VALUES('b')",
				"DELETE FROM foo",
				"INSERT INTO foo(v) VALUES('c')",
			},
		},
		{
			name:   "without rowid",
			schema: []string{"CREATE TABLE foo (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID"},
			stmts:  []string{"INSERT INTO foo VALUES('a', random())", "UPDATE foo SET v = random()"},
		},
		{
			name:   "composite key and blob",
			schema: []string{"CREATE TABLE foo (a INTEGER NOT NULL, b TEXT NOT NULL, c BLOB, d REAL, PRIMARY KEY(a, b))"},
			stmts: []string{
				"INSERT INTO foo VALUES(1, 'x', randomblob(10), 1.5e300)",
				"INSERT INTO foo VALUES(2, 'y', NULL, -0.0)",
				"UPDATE foo SET d = d / 3.0",
			},
		},
		{
			name: "trigger",
			schema: []string{
				"CREATE TABLE foo (id INTEGER PRIMARY KEY, v INTEGER)",
				"CREATE TABLE audit (id INTEGER PRIMARY KEY, fid INTEGER, r INTEGER, t TEXT DEFAULT (datetime('now')))",
				"CREATE TRIGGER trg AFTER INSERT ON foo BEGIN INSERT INTO audit(fid, r) VALUES(new.id, random()); END",
			},
			stmts: []string{"INSERT INTO foo(v) VALUES(1)", "INSERT INTO foo(v) VALUES(2)"},
		},
		{
			name: "foreign key cascade",
			schema: []string{
				"CREATE TABLE p (id INTEGER PRIMARY KEY)",
				"CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
			},
			stmts: []string{
				"INSERT INTO p VALUES(1),(2)",
				"INSERT INTO c VALUES(10,1),(11,1),(12,2)",
				"UPDATE p SET id = 5 WHERE id = 1",
				"DELETE FROM p WHERE id = 5",
			},
		},
		{
			name:   "default expression",
			schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY, t TEXT DEFAULT (datetime('now')), r INTEGER DEFAULT (random()))"},
			stmts:  []string{"INSERT INTO foo DEFAULT VALUES", "INSERT INTO foo(id) VALUES(7)"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := newPair(t, tt.schema...)
			for _, db := range []*DB{a, b} {
				mustExecute(db, "PRAGMA foreign_keys=ON")
			}
			for _, s := range tt.stmts {
				res, cs, err := a.ExecuteCapture(context.Background(), csRequest(false, s), false)
				if errors.Is(err, ErrNotCapturable) {
					// The session coalesces or does not count what the request did
					// (a key update cascading, a value updated to itself), so it is
					// refused and takes the statement path on every node.
					mustExecute(a, s)
					mustExecute(b, s)
					continue
				}
				if err != nil {
					t.Fatalf("capture failed: %s", err)
				}
				if res[0].GetE().GetError() != "" {
					t.Fatalf("%s: %s", s, res[0].GetE().GetError())
				}
				applyBoth(t, cs, a, b)
			}
			da, db := dump(t, a), dump(t, b)
			if da != db {
				t.Fatalf("databases differ:\nleader:\n%s\nfollower:\n%s", da, db)
			}
		})
	}
}

func Test_Changeset_NotCapturable(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	for _, s := range []string{
		"CREATE TABLE bar (id INTEGER)",
		"  -- comment\n DROP TABLE foo",
		"/* c */ ALTER TABLE foo ADD COLUMN x TEXT",
		"PRAGMA user_version=3",
		"VACUUM",
		"BEGIN",
		"COMMIT",
	} {
		_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, s), false)
		if !errors.Is(err, ErrNotCapturable) {
			t.Fatalf("%q: expected ErrNotCapturable, got %v", s, err)
		}
	}
	// A DDL statement hidden behind a capturable one in the same request.
	_, _, err := a.ExecuteCapture(context.Background(),
		csRequest(false, "INSERT INTO foo VALUES(1)", "CREATE TABLE bar (id INTEGER)"), false)
	if !errors.Is(err, ErrNotCapturable) {
		t.Fatalf("expected ErrNotCapturable, got %v", err)
	}
	rb := csRequest(false, "INSERT INTO foo VALUES(1)")
	rb.RollbackOnError = true
	if _, _, err := a.ExecuteCapture(context.Background(), rb, false); !errors.Is(err, ErrNotCapturable) {
		t.Fatalf("rollback_on_error: expected ErrNotCapturable, got %v", err)
	}
	fs := csRequest(false, "INSERT INTO foo VALUES(1)")
	fs.Statements[0].ForceStall = true
	if _, _, err := a.ExecuteCapture(context.Background(), fs, false); !errors.Is(err, ErrNotCapturable) {
		t.Fatalf("force_stall: expected ErrNotCapturable, got %v", err)
	}
	// Nothing was applied by any of the refused captures.
	if r := mustQuery(a, "SELECT count(*) FROM foo"); r[0].Values[0].Parameters[0].GetI() != 0 {
		t.Fatal("refused capture changed the database")
	}
	if r := mustQuery(a, "SELECT count(*) FROM sqlite_master WHERE name='bar'"); r[0].Values[0].Parameters[0].GetI() != 0 {
		t.Fatal("refused capture created a table")
	}
	// The connection is usable afterwards.
	mustExecute(a, "INSERT INTO foo VALUES(2)")
}

func Test_Changeset_Transaction(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	mustExecute(a, "INSERT INTO foo VALUES(1, 'a')")

	// Transaction: the second statement fails, so nothing is kept.
	res, cs, err := a.ExecuteCapture(context.Background(),
		csRequest(true, "INSERT INTO foo VALUES(2, 'b')", "INSERT INTO foo VALUES(1, 'dup')"), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 0 {
		t.Fatal("rolled-back transaction produced a changeset")
	}
	if len(res) != 2 || res[1].GetE().GetError() == "" {
		t.Fatalf("expected an error in the second result: %v", res)
	}

	// No transaction: the first statement is kept, as on the statement path.
	res, cs, err = a.ExecuteCapture(context.Background(),
		csRequest(false, "INSERT INTO foo VALUES(2, 'b')", "INSERT INTO foo VALUES(1, 'dup')"), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) == 0 || len(res) != 2 || res[1].GetE().GetError() == "" {
		t.Fatalf("unexpected: %d bytes, %v", len(cs), res)
	}
}

// Test_Changeset_ConflictAborts checks a node whose database differs from the
// leader's refuses the changeset and is left unchanged.
func Test_Changeset_ConflictAborts(t *testing.T) {
	a, b := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	mustExecute(a, "INSERT INTO foo VALUES(1, 'a')")
	mustExecute(b, "INSERT INTO foo VALUES(1, 'a')")
	_, cs := mustCapture(t, a, csRequest(false,
		"INSERT INTO foo VALUES(2, 'x')", "UPDATE foo SET v='new' WHERE id=1"))

	// b has diverged: the row the update targets holds another value.
	mustExecute(b, "UPDATE foo SET v='diverged' WHERE id=1")
	before := dump(t, b)
	err := b.ApplyChangeset(context.Background(), cs)
	if !errors.Is(err, ErrChangesetConflict) {
		t.Fatalf("expected ErrChangesetConflict, got %v", err)
	}
	if after := dump(t, b); after != before {
		t.Fatalf("aborted apply changed the database:\n%s\nvs\n%s", before, after)
	}

	// A duplicate primary key conflicts too.
	mustExecute(b, "UPDATE foo SET v='a' WHERE id=1")
	mustExecute(b, "INSERT INTO foo VALUES(2, 'already')")
	if err := b.ApplyChangeset(context.Background(), cs); !errors.Is(err, ErrChangesetConflict) {
		t.Fatalf("expected ErrChangesetConflict, got %v", err)
	}
}

func Test_Changeset_Request(t *testing.T) {
	a, b := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v INTEGER)")
	res, cs, err := a.RequestCapture(context.Background(),
		csRequest(false, "INSERT INTO foo(v) VALUES(random())", "SELECT count(*) FROM foo"), false)
	if err != nil {
		t.Fatal(err)
	}
	if got := res[1].GetQ().Values[0].Parameters[0].GetI(); got != 1 {
		t.Fatalf("read in request: got %d, want 1 (reads see the request's writes)", got)
	}
	applyBoth(t, cs, a, b)
	if dump(t, a) != dump(t, b) {
		t.Fatal("databases differ")
	}
}

func Test_Changeset_FirstKeyword(t *testing.T) {
	for in, want := range map[string]string{
		"insert into x": "INSERT", "  \n\tCreate": "CREATE", "-- a\nVACUUM": "VACUUM",
		"/* x */ /* y */ pragma a": "PRAGMA", "": "", "--": "", "/*": "", "WITH x": "WITH",
	} {
		if got := firstKeyword(in); got != want {
			t.Errorf("firstKeyword(%q) = %q, want %q", in, got, want)
		}
	}
}
