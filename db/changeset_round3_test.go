package db

import (
	"context"
	"errors"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"modernc.org/libc"
	"modernc.org/libc/sys/types"
)

// r3Case is one request that must leave every node as the statement path leaves
// the reference: either the capture reproduces it exactly, or it refuses and
// the statement path runs.
type r3Case struct {
	name   string
	fk     bool
	schema []string
	setup  []string // Run on every node, through the statement path, after the schema.
	tx     bool
	stmts  []string
}

func r3DB(t *testing.T, fk bool, schema, setup []string) *DB {
	t.Helper()
	var d *DB
	if fk {
		d, _ = mustCreateOnDiskDatabaseWALFK()
	} else {
		d, _ = mustCreateOnDiskDatabaseWAL()
	}
	t.Cleanup(func() { d.Close() })
	for _, s := range append(append([]string(nil), schema...), setup...) {
		mustExecute(d, s)
	}
	return d
}

// r3Check returns whether the request was replicated as a changeset.
func r3Check(t *testing.T, c r3Case) bool {
	t.Helper()
	ref := r3DB(t, c.fk, c.schema, c.setup)
	leader := r3DB(t, c.fk, c.schema, c.setup)
	follower := r3DB(t, c.fk, c.schema, c.setup)

	// A request whose commit fails (a deferred foreign key) fails as a whole on
	// the statement path; the capture must not acknowledge it.
	want, refErr := ref.Execute(csRequest(c.tx, c.stmts...), false)
	got, cs, err := leader.ExecuteCapture(context.Background(), csRequest(c.tx, c.stmts...), false)
	captured := false
	switch {
	case errors.Is(err, ErrNotCapturable):
		for _, d := range []*DB{leader, follower} {
			if _, err := d.Execute(csRequest(c.tx, c.stmts...), false); (err != nil) != (refErr != nil) {
				t.Fatalf("%s: fallback: %v, reference %v", c.name, err, refErr)
			}
		}
		got = want
	case err != nil:
		if refErr == nil {
			t.Fatalf("%s: capture: %v, the statement path succeeds", c.name, err)
		}
		return false
	default:
		if refErr != nil {
			t.Fatalf("%s: capture acknowledged a request the statement path fails: %v", c.name, refErr)
		}
		captured = true
		applyBoth(t, cs, leader, follower)
	}
	if len(got) != len(want) {
		t.Fatalf("%s: %d results, reference %d", c.name, len(got), len(want))
	}
	for j := range want {
		if (want[j].GetE().GetError() != "") != (got[j].GetE().GetError() != "") {
			t.Fatalf("%s: statement %d: error %q, reference %q", c.name, j, got[j].GetE().GetError(), want[j].GetE().GetError())
		}
		if captured && want[j].GetE().GetRowsAffected() != got[j].GetE().GetRowsAffected() {
			t.Fatalf("%s: statement %d: rows affected %d, reference %d", c.name, j,
				got[j].GetE().GetRowsAffected(), want[j].GetE().GetRowsAffected())
		}
	}
	rd := dump(t, ref)
	for name, d := range map[string]*DB{"leader": leader, "follower": follower} {
		if dd := dump(t, d); dd != rd {
			t.Fatalf("%s: %s differs from the statement path:\n%s\nvs\n%s", c.name, name, dd, rd)
		}
	}
	return captured
}

var r3FKParent = []string{
	"CREATE TABLE p (id INTEGER PRIMARY KEY)",
	"CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE)",
}

// Every SQLite corner the two re-reviews found, each held to the same standard:
// the nodes end as the statement path ends.
func Test_Changeset_Round3_AgreesWithStatements(t *testing.T) {
	for _, c := range []r3Case{
		{name: "integer pk desc with NULL key (N1)",
			schema: []string{"CREATE TABLE foo (k INTEGER PRIMARY KEY DESC, v TEXT)"},
			stmts:  []string{"INSERT INTO foo(k, v) VALUES(NULL, 'x')"}},
		{name: "temp table shadows a keyless main table (N2)",
			schema: []string{"CREATE TABLE foo (v TEXT)"},
			setup:  []string{"CREATE TEMP TABLE foo (id INTEGER PRIMARY KEY, v TEXT)"},
			stmts:  []string{"INSERT INTO main.foo VALUES('x')"}},
		{name: "temp table shadows a nullable-key main table (N2)",
			schema: []string{"CREATE TABLE nk (k TEXT PRIMARY KEY, v TEXT)"},
			setup:  []string{"CREATE TEMP TABLE nk (k TEXT NOT NULL PRIMARY KEY, v TEXT)"},
			stmts:  []string{"INSERT INTO main.nk VALUES(NULL, 'x')"}},
		{name: "temp shadow, trigger writes the keyless main table (N2)",
			schema: []string{
				"CREATE TABLE acct (id INTEGER PRIMARY KEY, bal INTEGER)",
				"CREATE TABLE audit (what TEXT)",
				"CREATE TRIGGER a AFTER INSERT ON acct BEGIN INSERT INTO audit VALUES('insert ' || new.id); END",
			},
			setup: []string{"CREATE TEMP TABLE audit (id INTEGER PRIMARY KEY, what TEXT)"},
			stmts: []string{"INSERT INTO acct VALUES(2, 20)"}},
		{name: "autoincrement, insert then delete (N3)",
			schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"},
			stmts:  []string{"INSERT INTO foo(v) VALUES('a')", "DELETE FROM foo"}},
		{name: "autoincrement, insert then delete, then reuse",
			schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"},
			setup:  []string{"INSERT INTO foo(v) VALUES('a')", "DELETE FROM foo"},
			stmts:  []string{"INSERT INTO foo(v) VALUES('b')"}},
		{name: "deferred foreign key violated, non-transactional (N2 correctness)",
			fk: true,
			schema: []string{
				"CREATE TABLE p (id INTEGER PRIMARY KEY)",
				"CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
				"CREATE TABLE other (id INTEGER PRIMARY KEY, v TEXT)",
			},
			stmts: []string{"INSERT INTO other VALUES(1, 'kept')", "INSERT INTO c VALUES(1, 99)"}},
		{name: "deferred foreign key violated, transactional",
			fk: true, tx: true,
			schema: []string{
				"CREATE TABLE p (id INTEGER PRIMARY KEY)",
				"CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
				"CREATE TABLE other (id INTEGER PRIMARY KEY, v TEXT)",
			},
			stmts: []string{"INSERT INTO other VALUES(1, 'x')", "INSERT INTO c VALUES(1, 99)"}},
		{name: "deferred foreign key violated then repaired by a later statement",
			fk: true,
			schema: []string{
				"CREATE TABLE p (id INTEGER PRIMARY KEY)",
				"CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
			},
			stmts: []string{"INSERT INTO c VALUES(1, 99)", "INSERT INTO p VALUES(99)"}},
		{name: "deferred foreign key violated then repaired inside a transaction",
			fk: true, tx: true,
			schema: []string{
				"CREATE TABLE p (id INTEGER PRIMARY KEY)",
				"CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
			},
			stmts: []string{"INSERT INTO c VALUES(1, 99)", "INSERT INTO p VALUES(99)"}},
		{name: "fk cascade into a child whose trigger writes a keyless table",
			fk: true,
			schema: append(append([]string(nil), r3FKParent...),
				"CREATE TABLE audit (v TEXT)",
				"CREATE TRIGGER c_del AFTER DELETE ON c BEGIN INSERT INTO audit VALUES('deleted ' || old.id); END",
				"INSERT INTO p VALUES(1)", "INSERT INTO c VALUES(10, 1)"),
			stmts: []string{"DELETE FROM p WHERE id = 1"}},
		{name: "fk cascade into a child whose trigger writes a nullable-key table",
			fk: true,
			schema: append(append([]string(nil), r3FKParent...),
				"CREATE TABLE nk (k TEXT PRIMARY KEY, v TEXT)",
				"CREATE TRIGGER c_del AFTER DELETE ON c BEGIN INSERT INTO nk VALUES(NULL, 'child deleted'); END",
				"INSERT INTO p VALUES(1)", "INSERT INTO c VALUES(10, 1)"),
			stmts: []string{"DELETE FROM p WHERE id = 1"}},
		{name: "replace through a unique index deletes another row",
			schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY, u TEXT UNIQUE, v TEXT)", "INSERT INTO foo VALUES(1, 'k', 'old')"},
			stmts:  []string{"REPLACE INTO foo VALUES(8, 'k', 'replaced')"}},
		{name: "replace through a unique index, offsetting a keyless write",
			schema: []string{
				"CREATE TABLE foo (id INTEGER PRIMARY KEY, u TEXT UNIQUE, v TEXT)",
				"CREATE TABLE nk (k TEXT PRIMARY KEY, v TEXT)",
				"INSERT INTO foo VALUES(1, 'k', 'old')",
			},
			stmts: []string{"REPLACE INTO foo VALUES(8, 'k', 'replaced')", "INSERT INTO nk VALUES(NULL, 'lost?')"}},
		{name: "update to the same value",
			schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)", "INSERT INTO foo VALUES(1, 'a')"},
			stmts:  []string{"UPDATE foo SET v = 'a'"}},
		{name: "select then reindex in one string (N7)",
			schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)", "CREATE INDEX foo_v ON foo(v)", "INSERT INTO foo VALUES(1, 'a')"},
			stmts:  []string{"SELECT 1; REINDEX"}},
		{name: "two updates of one row",
			schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)", "INSERT INTO foo VALUES(1, 'a')"},
			stmts:  []string{"UPDATE foo SET v = 'b'", "UPDATE foo SET v = 'c'"}},
	} {
		t.Run(c.name, func(t *testing.T) { r3Check(t, c) })
	}
}

// The ordinary cases must still be captured: the verification refuses what it
// cannot account for, not everything.
func Test_Changeset_Round3_OrdinaryRequestsStillCapture(t *testing.T) {
	for _, c := range []r3Case{
		{name: "insert", schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)"},
			stmts: []string{"INSERT INTO foo(v) VALUES('a'),('b')"}},
		{name: "trigger and update",
			schema: []string{
				"CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)",
				"CREATE TABLE audit (id INTEGER PRIMARY KEY, what TEXT)",
				"CREATE TRIGGER foo_ai AFTER INSERT ON foo BEGIN INSERT INTO audit(what) VALUES('ins ' || new.id); END",
				"INSERT INTO foo VALUES(1, 'a')",
			},
			stmts: []string{"INSERT INTO foo(v) VALUES('b')", "UPDATE foo SET v = 'z' WHERE id = 1"}},
		{name: "trigger updates the row just inserted (coalesced)",
			schema: []string{
				"CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)",
				"CREATE TRIGGER foo_ai AFTER INSERT ON foo BEGIN UPDATE foo SET v = v || '!' WHERE id = new.id; END",
			},
			stmts: []string{"INSERT INTO foo(v) VALUES('a')"}},
		{name: "insert then update of the same rows in one request (coalesced)",
			schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)"},
			stmts:  []string{"INSERT INTO foo(v) VALUES('a'),('b')", "UPDATE foo SET v = v || '+'"}},
		{name: "fk cascade", fk: true,
			schema: append(append([]string(nil), r3FKParent...), "INSERT INTO p VALUES(1)", "INSERT INTO c VALUES(10, 1)"),
			stmts:  []string{"DELETE FROM p WHERE id = 1"}},
		{name: "autoincrement insert",
			schema: []string{"CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)"},
			stmts:  []string{"INSERT INTO foo(v) VALUES('a')"}},
		{name: "without rowid",
			schema: []string{"CREATE TABLE k (a TEXT NOT NULL, b INTEGER NOT NULL, v REAL, PRIMARY KEY (a, b)) WITHOUT ROWID"},
			stmts:  []string{"INSERT INTO k VALUES('a', 1, 1.5), ('b', 2, 2.5)"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if !r3Check(t, c) {
				t.Fatal("an ordinary request took the statement path")
			}
		})
	}
}

// A TEMP table of the same name must not change what the apply side believes
// about the main table.
func Test_Changeset_Round3_TempShadowDoesNotRefuseApply(t *testing.T) {
	a, b := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1, 'x')"))
	mustExecute(b, "CREATE TEMP TABLE foo (only_one_column)")
	if err := b.ApplyChangeset(context.Background(), cs); err != nil {
		t.Fatalf("apply refused because of a temp table: %v", err)
	}
	if n := count(b, "SELECT count(*) FROM main.foo"); n != 1 {
		t.Fatalf("main.foo has %d rows, want 1", n)
	}
}

func count(db *DB, q string) int64 {
	r, err := db.QueryStringStmt(q)
	if err != nil || r[0].Error != "" {
		return -1
	}
	return r[0].Values[0].Parameters[0].GetI()
}

// The size limit is applied to the size the session reports, before the
// changeset buffer is built.
func Test_Changeset_Round3_SizeBoundBeforeMaterialising(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	a.SetChangesetMaxSize(64)
	built := 0
	orig := cSessionChangeset
	cSessionChangeset = func(tls *libc.TLS, p uintptr, n uintptr, pp uintptr) int32 {
		built++
		return orig(tls, p, n, pp)
	}
	defer func() { cSessionChangeset = orig }()
	_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo(v) VALUES(hex(zeroblob(500)))"), false)
	if !errors.Is(err, ErrChangesetTooLarge) {
		t.Fatalf("expected ErrChangesetTooLarge, got %v", err)
	}
	if built != 0 {
		t.Fatalf("the changeset was built %d times before being refused", built)
	}
}

// A request interrupted by its timeout is rolled back by SQLite. That is an
// error to the client, never a silent move to the statement path.
func Test_Changeset_Round3_TimeoutIsAnError(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	req := &command.Request{
		Statements: []*command.Statement{{
			Sql: "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 500000000) INSERT INTO foo(v) SELECT i FROM n",
		}},
	}
	before := dump(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, cs, err := a.ExecuteCapture(ctx, req, false)
	if errors.Is(err, ErrNotCapturable) {
		t.Fatalf("a timed-out capture was sent to the statement path: %v", err)
	}
	if err == nil {
		t.Fatalf("a timed-out capture reported success (changeset %d bytes)", len(cs))
	}
	if after := dump(t, a); after != before {
		t.Fatal("a timed-out capture changed the database")
	}
}

// Failures that belong to this node are fatal while a changeset is read, like on
// the statement path; they are not "malformed".
func Test_Changeset_Round3_NodeLocalFailuresAreFatal(t *testing.T) {
	a, b := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1, 'x')"))
	var fe *FatalError

	origM := cMalloc
	cMalloc = func(tls *libc.TLS, n types.Size_t) uintptr { return 0 }
	err := b.ApplyChangeset(context.Background(), cs)
	cMalloc = origM
	if !errors.As(err, &fe) {
		t.Fatalf("out of memory before the apply: %v, want a FatalError", err)
	}

	for _, rc := range []int32{7 /* NOMEM */, 10 /* IOERR */, 13 /* FULL */} {
		origS := cChangesetStart
		cChangesetStart = func(*libc.TLS, uintptr, int32, uintptr) int32 { return rc }
		err = b.ApplyChangeset(context.Background(), cs)
		cChangesetStart = origS
		if errors.Is(err, ErrChangesetMalformed) || !errors.As(err, &fe) {
			t.Fatalf("rc %d from the changeset iterator: %v, want a FatalError that is not malformed", rc, err)
		}
	}
	// A changeset the format check rejects stays a plain, non-fatal refusal.
	err = b.ApplyChangeset(context.Background(), []byte("T\x02\x01\x00foo\x00\x12\x00\x01"))
	if !errors.Is(err, ErrChangesetMalformed) || errors.As(err, &fe) {
		t.Fatalf("damaged bytes: %v, want ErrChangesetMalformed, not fatal", err)
	}
}
