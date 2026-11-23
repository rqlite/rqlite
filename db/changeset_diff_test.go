package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// A changeset above the limit is an error, never a fallback to SQL text.
func Test_Changeset_MaxSize(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	a.SetChangesetMaxSize(64)
	before := dump(t, a)
	_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo(v) VALUES(hex(zeroblob(500)))"), false)
	if !errors.Is(err, ErrChangesetTooLarge) {
		t.Fatalf("expected ErrChangesetTooLarge, got %v", err)
	}
	if errors.Is(err, ErrNotCapturable) {
		t.Fatal("an oversized changeset must not take the statement path")
	}
	if dump(t, a) != before {
		t.Fatal("an oversized capture changed the database")
	}
	a.SetChangesetMaxSize(0)
	if _, _, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo(v) VALUES('small')"), false); err != nil {
		t.Fatalf("default limit refused a small changeset: %v", err)
	}
}

var diffSchema = []string{
	"CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)",
	"CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE, v TEXT)",
	"CREATE TABLE k (a TEXT NOT NULL, b INTEGER NOT NULL, v REAL, PRIMARY KEY (a, b)) WITHOUT ROWID",
	"CREATE TABLE s (k TEXT NOT NULL PRIMARY KEY, n INTEGER DEFAULT 0)",
	"CREATE TABLE audit (id INTEGER PRIMARY KEY, what TEXT)",
	"CREATE INDEX p_v ON p(v)",
	"CREATE TRIGGER p_ai AFTER INSERT ON p BEGIN INSERT INTO audit(what) VALUES('ins ' || new.id); END",
	"CREATE TRIGGER p_au AFTER UPDATE ON p BEGIN INSERT INTO audit(what) VALUES('upd ' || new.id); END",
}

// Each request is replicated once as a changeset and once as SQL text. Every
// node of both must end up the same, and every statement must report the same
// error or none.
func Test_Changeset_DifferentialAgainstStatements(t *testing.T) {
	requests := []struct {
		tx    bool
		stmts []string
	}{
		{false, []string{"PRAGMA foreign_keys=ON"}},
		{false, []string{"INSERT INTO p(v) VALUES('a'),('b'),('c')"}},
		{false, []string{"INSERT INTO p(id, v) VALUES(2, 'dup')"}}, // constraint error
		{false, []string{"INSERT INTO p(v) VALUES('x')", "INSERT INTO p(id, v) VALUES(1, 'dup')", "INSERT INTO p(v) VALUES('y')"}},
		{true, []string{"INSERT INTO p(v) VALUES('t1')", "INSERT INTO p(id, v) VALUES(1, 'dup')", "INSERT INTO p(v) VALUES('t2')"}},
		{true, []string{"INSERT INTO p(v) VALUES('t3')", "UPDATE p SET v = v || '!' WHERE id < 3"}},
		{false, []string{"INSERT INTO c(pid, v) VALUES(1, 'x'), (2, 'y'), (3, 'z')"}},
		{false, []string{"DELETE FROM p WHERE id = 1"}}, // cascades
		{false, []string{"UPDATE p SET id = id + 100 WHERE id = 2"}},
		{false, []string{"INSERT OR REPLACE INTO p(id, v) VALUES(3, 'replaced')"}},
		{false, []string{"INSERT INTO k VALUES('a', 1, 1.5), ('a', 2, -0.0), ('b', 1, 1e300)"}},
		{false, []string{"UPDATE k SET v = v * 2", "UPDATE k SET b = b + 10 WHERE a = 'a'"}},
		{false, []string{"INSERT INTO s VALUES('x', 1) ON CONFLICT(k) DO UPDATE SET n = n + 1"}},
		{false, []string{"INSERT INTO s VALUES('x', 1) ON CONFLICT(k) DO UPDATE SET n = n + 1"}},
		{false, []string{"INSERT INTO p(v) VALUES('q'); INSERT INTO p(v) VALUES('r')"}}, // two statements in one string
		{false, []string{"DELETE FROM k WHERE a = 'zzz'"}},                              // no change
		{false, []string{"SELECT count(*) FROM p"}},
		{false, []string{"UPDATE p SET v = 'same' WHERE id = 3", "UPDATE p SET v = 'same' WHERE id = 3"}},
		{false, []string{"INSERT INTO p(id, v) VALUES(500, 'gone')", "DELETE FROM p WHERE id = 500"}}, // coalesces to nothing
		{false, []string{"DELETE FROM p"}},
		{false, []string{"INSERT INTO p(v) VALUES('end')"}},
	}

	ref, _ := newPair(t, diffSchema...)
	leader, follower := newPair(t, diffSchema...)
	ctx := context.Background()
	for i, rq := range requests {
		want, err := ref.Execute(csRequest(rq.tx, rq.stmts...), false)
		if err != nil {
			t.Fatalf("request %d: reference: %v", i, err)
		}
		got, cs, err := leader.ExecuteCapture(ctx, csRequest(rq.tx, rq.stmts...), false)
		switch {
		case errors.Is(err, ErrNotCapturable):
			// The statement path, on every node.
			for _, d := range []*DB{leader, follower} {
				if _, err := d.Execute(csRequest(rq.tx, rq.stmts...), false); err != nil {
					t.Fatalf("request %d: fallback: %v", i, err)
				}
			}
			got = want
		case err != nil:
			t.Fatalf("request %d: capture: %v", i, err)
		default:
			applyBoth(t, cs, leader, follower)
		}
		if len(got) != len(want) {
			t.Fatalf("request %d %q: %d results, reference %d", i, rq.stmts, len(got), len(want))
		}
		for j := range want {
			if (want[j].GetE().GetError() != "") != (got[j].GetE().GetError() != "") {
				t.Fatalf("request %d %q statement %d: error %q, reference %q", i, rq.stmts, j,
					got[j].GetE().GetError(), want[j].GetE().GetError())
			}
		}
		rd := dump(t, ref)
		for name, d := range map[string]*DB{"leader": leader, "follower": follower} {
			if dd := dump(t, d); dd != rd {
				t.Fatalf("request %d %q: %s differs from the reference:\n%s\nvs\n%s", i, rq.stmts, name, dd, rd)
			}
		}
	}
}

const fuzzDir = "testdata/fuzz/FuzzApplyChangeset"

var fuzzSchema = append([]string{
	"CREATE TABLE nokey (a TEXT, b INTEGER)",
}, diffSchema...)

func fuzzSeedChangesets(t testing.TB) [][]byte {
	a, _ := mustCreateOnDiskDatabaseWAL()
	defer a.Close()
	for _, s := range fuzzSchema {
		mustExecute(a, s)
	}
	var out [][]byte
	for _, stmts := range [][]string{
		{"INSERT INTO p(v) VALUES('a'),('b')"},
		{"INSERT INTO c(pid, v) VALUES(1, 'x')", "UPDATE p SET v = 'z' WHERE id = 1"},
		{"INSERT INTO k VALUES('a', 1, 1.5), ('b', 2, NULL)"},
		{"DELETE FROM k WHERE a = 'a'", "UPDATE k SET v = 9.5 WHERE a = 'b'"},
		{"INSERT INTO s VALUES('x', 1), ('y', 2)"},
		{"UPDATE s SET n = n + 1"},
		{"DELETE FROM p", "INSERT INTO p(id, v) VALUES(7, x'00ff')"},
	} {
		_, cs, err := a.ExecuteCapture(context.Background(), csRequest(false, stmts...), false)
		if err != nil {
			t.Fatalf("seed %q: %v", stmts, err)
		}
		out = append(out, cs)
		// Keep the capture database moving, so later seeds are not all inserts.
		for _, s := range stmts {
			mustExecute(a, s)
		}
	}
	return out
}

// Test_Changeset_WriteFuzzSeeds regenerates the committed seed corpus:
//
//	CHANGESET_WRITE_SEEDS=1 go test -run Test_Changeset_WriteFuzzSeeds ./db/
func Test_Changeset_WriteFuzzSeeds(t *testing.T) {
	if os.Getenv("CHANGESET_WRITE_SEEDS") == "" {
		t.Skip("set CHANGESET_WRITE_SEEDS=1 to rewrite the seed corpus")
	}
	if err := os.MkdirAll(fuzzDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i, cs := range fuzzSeedChangesets(t) {
		body := fmt.Sprintf("go test fuzz v1\n[]byte(%q)\n", cs)
		if err := os.WriteFile(filepath.Join(fuzzDir, fmt.Sprintf("seed-%02d", i)), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		// A damaged copy of each: one byte flipped, and cut in two.
		bad := append([]byte(nil), cs...)
		bad[len(bad)/2] ^= 0xff
		if err := os.WriteFile(filepath.Join(fuzzDir, fmt.Sprintf("flip-%02d", i)), []byte(fmt.Sprintf("go test fuzz v1\n[]byte(%q)\n", bad)), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(fuzzDir, fmt.Sprintf("cut-%02d", i)), []byte(fmt.Sprintf("go test fuzz v1\n[]byte(%q)\n", cs[:len(cs)-1])), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// FuzzApplyChangeset feeds arbitrary bytes to the apply path, as a damaged or
// hostile Raft entry would. It must never crash, never report a database fault
// for bad input, and never leave a changed database behind an error.
func FuzzApplyChangeset(f *testing.F) {
	for _, cs := range fuzzSeedChangesets(f) {
		f.Add(cs)
	}
	f.Add([]byte{})
	f.Add([]byte("T\x02\x01\x00p\x00\x12\x00\x01"))

	db, _ := mustCreateOnDiskDatabaseWAL()
	f.Cleanup(func() { db.Close() })
	for _, s := range fuzzSchema {
		mustExecute(db, s)
	}
	f.Fuzz(func(t *testing.T, cs []byte) {
		before := dump(t, db)
		err := db.ApplyChangeset(context.Background(), cs)
		var fe *FatalError
		if errors.As(err, &fe) {
			t.Fatalf("bad input is a database fault: %v", err)
		}
		if err != nil && dump(t, db) != before {
			t.Fatalf("a refused changeset changed the database: %v", err)
		}
	})
}
