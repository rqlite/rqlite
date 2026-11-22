package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// mustNewCluster returns n stores, the first of which is the Leader, joined into
// one cluster. changesets sets Store.ChangesetReplication on every node.
func mustNewCluster(t *testing.T, n int, changesets bool) []*Store {
	t.Helper()
	var stores []*Store
	for i := 0; i < n; i++ {
		s, ln := mustNewStore(t)
		s.ChangesetReplication = changesets
		t.Cleanup(func() { ln.Close() })
		if err := s.Open(); err != nil {
			t.Fatalf("failed to open store: %s", err)
		}
		t.Cleanup(func() { s.Close(true) })
		if i == 0 {
			if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
				t.Fatalf("failed to bootstrap: %s", err)
			}
		} else if err := stores[0].Join(joinRequest(s.ID(), s.Addr(), true)); err != nil {
			t.Fatalf("failed to join: %s", err)
		}
		if _, err := s.WaitForLeader(10 * time.Second); err != nil {
			t.Fatalf("no leader: %s", err)
		}
		stores = append(stores, s)
	}
	return stores
}

// dumpStore returns every row of every table of the store's database, with
// rowids, as text.
func dumpStore(t *testing.T, s *Store) string {
	t.Helper()
	q := func(sql string) []*proto.QueryRows {
		qr := queryRequestFromString(sql, false, false, false)
		qr.Level = proto.ConsistencyLevel_NONE
		r, _, _, err := s.Query(context.Background(), qr)
		if err != nil {
			t.Fatalf("query %q: %s", sql, err)
		}
		return r
	}
	var sb strings.Builder
	for _, v := range q("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")[0].Values {
		name := v.Parameters[0].GetS()
		fmt.Fprintf(&sb, "%s: %s\n", name, asJSON(q(fmt.Sprintf(`SELECT rowid, * FROM "%s" ORDER BY rowid`, name))))
	}
	return sb.String()
}

func waitConverged(t *testing.T, stores []*Store) {
	t.Helper()
	ci, err := stores[0].CommitIndex()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stores {
		if err := s.WaitForCommitIndex(ci, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	testPoll(t, func() bool {
		for _, s := range stores {
			if s.fsmIdx.Load() < stores[0].fsmIdx.Load() {
				return false
			}
		}
		return true
	}, 50*time.Millisecond, 5*time.Second)
}

// nonDeterministicWrites are writes whose effect differs on every evaluation.
// Run straight at the Store, no SQL rewriting happens (that lives in the HTTP
// layer), which is how #3055 shows: randomblob(?) is not a literal the
// rewriter can replace.
func nonDeterministicWrites(t *testing.T, s *Store) {
	t.Helper()
	mustExec := func(er *proto.ExecuteRequest) {
		t.Helper()
		r, _, err := s.Execute(context.Background(), er)
		if err != nil {
			t.Fatalf("execute: %s", err)
		}
		for _, x := range r {
			if x.GetE().GetError() != "" {
				t.Fatalf("execute: %s", x.GetE().GetError())
			}
		}
	}
	mustExec(executeRequestFromString(
		`CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, r INTEGER, b BLOB, t TEXT, d TEXT DEFAULT (datetime('now')))`, false, false))
	mustExec(executeRequestFromString(`INSERT INTO foo(r, b, t) VALUES(random(), randomblob(8), datetime('now'))`, false, false))
	mustExec(executeRequestFromString(`INSERT INTO foo(b) VALUES(randomblob(2*4))`, false, false))
	mustExec(executeRequestFromString(`INSERT INTO foo(b) VALUES(randomblob(8.0))`, false, false))
	mustExec(&proto.ExecuteRequest{Request: &proto.Request{Statements: []*proto.Statement{{
		Sql:        `INSERT INTO foo(b) VALUES(randomblob(?))`,
		Parameters: []*proto.Parameter{{Value: &proto.Parameter_I{I: 16}}},
	}}}})
	mustExec(executeRequestFromStrings([]string{
		`INSERT INTO foo(r, t) SELECT random(), strftime('%f','now') FROM foo`,
		`UPDATE foo SET r = random() WHERE id % 2 = 0`,
	}, false, true))
	mustExec(executeRequestFromString(`INSERT INTO foo DEFAULT VALUES`, false, false))
}

// Test_Changeset_MultiNodeIdentical writes non-deterministic SQL through a
// three-node cluster. With changeset replication every node holds identical
// data; on the statement path they do not, which is bug #3055.
func Test_Changeset_MultiNodeIdentical(t *testing.T) {
	for _, tt := range []struct {
		name       string
		changesets bool
	}{
		{"statement path diverges", false},
		{"changeset path converges", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stores := mustNewCluster(t, 3, tt.changesets)
			nonDeterministicWrites(t, stores[0])
			waitConverged(t, stores)

			d0, d1, d2 := dumpStore(t, stores[0]), dumpStore(t, stores[1]), dumpStore(t, stores[2])
			identical := d0 == d1 && d0 == d2
			if tt.changesets != identical {
				t.Fatalf("changesets=%v but identical=%v\nnode0:\n%s\nnode1:\n%s\nnode2:\n%s",
					tt.changesets, identical, d0, d1, d2)
			}
			if got := stores[0].numChangesets.Load(); tt.changesets && got == 0 || !tt.changesets && got != 0 {
				t.Fatalf("unexpected number of changesets applied: %d", got)
			}
			// CREATE TABLE is DDL, so it took the statement path: 6 writes
			// remain for the changeset path.
			if tt.changesets {
				if got := stores[1].numChangesets.Load(); got != 6 {
					t.Fatalf("follower applied %d changesets, expected 6", got)
				}
			}
		})
	}
}

// Test_Changeset_ResultsMatchStatementPath checks rows affected and last insert
// ID are the same as on the statement path.
func Test_Changeset_ResultsMatchStatementPath(t *testing.T) {
	run := func(changesets bool) string {
		stores := mustNewCluster(t, 1, changesets)
		s := stores[0]
		var sb strings.Builder
		for _, sql := range []string{
			`CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`,
			`INSERT INTO foo(v) VALUES('a')`,
			`INSERT INTO foo(v) VALUES('b'),('c')`,
			`UPDATE foo SET v='z' WHERE id > 1`,
			`DELETE FROM foo WHERE id = 1`,
			`DELETE FROM foo WHERE id = 99`,
			`INSERT INTO foo(id, v) VALUES(2, 'dup')`,
		} {
			r, _, err := s.Execute(context.Background(), executeRequestFromString(sql, false, false))
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&sb, "%s\n", asJSON(r))
		}
		return sb.String()
	}
	if want, got := run(false), run(true); want != got {
		t.Fatalf("results differ\nstatement path:\n%s\nchangeset path:\n%s", want, got)
	}
}

// Test_Changeset_DDLFallsBack checks DDL, which the session extension does not
// record, still replicates, as SQL.
func Test_Changeset_DDLFallsBack(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	s := stores[0]
	for _, sql := range []string{
		`CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)`,
		`CREATE TRIGGER trg AFTER INSERT ON foo BEGIN UPDATE foo SET v = v || '!' WHERE id = new.id; END`,
		`ALTER TABLE foo ADD COLUMN w INTEGER`,
	} {
		if _, _, err := s.Execute(context.Background(), executeRequestFromString(sql, false, false)); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.numChangesets.Load(); got != 0 {
		t.Fatalf("DDL produced %d changesets", got)
	}
	// A trigger makes the write's effect depend on more than the statement.
	if _, _, err := s.Execute(context.Background(), executeRequestFromString(`INSERT INTO foo(v, w) VALUES('x', random())`, false, false)); err != nil {
		t.Fatal(err)
	}
	waitConverged(t, stores)
	if a, b := dumpStore(t, stores[0]), dumpStore(t, stores[1]); a != b {
		t.Fatalf("nodes differ\n%s\n%s", a, b)
	}
	if got := s.numChangesets.Load(); got != 1 {
		t.Fatalf("expected the insert to be one changeset, got %d", got)
	}
}

// Test_Changeset_Request covers the unified endpoint, with a read in the request.
func Test_Changeset_Request(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	s := stores[0]
	if _, _, err := s.Execute(context.Background(), executeRequestFromString(
		`CREATE TABLE foo (id INTEGER PRIMARY KEY, v INTEGER)`, false, false)); err != nil {
		t.Fatal(err)
	}
	eqr := executeQueryRequestFromStrings([]string{
		`INSERT INTO foo(v) VALUES(random())`,
		`SELECT count(*) FROM foo`,
	}, proto.ConsistencyLevel_WEAK, false, false, false)
	r, nRW, _, err := s.Request(context.Background(), eqr)
	if err != nil {
		t.Fatal(err)
	}
	if nRW != 1 || len(r) != 2 || r[1].GetQ().Values[0].Parameters[0].GetI() != 1 {
		t.Fatalf("unexpected response: nRW=%d %s", nRW, asJSON(r))
	}
	waitConverged(t, stores)
	if a, b := dumpStore(t, stores[0]), dumpStore(t, stores[1]); a != b {
		t.Fatalf("nodes differ\n%s\n%s", a, b)
	}
	if got := s.numChangesets.Load(); got != 1 {
		t.Fatalf("expected 1 changeset, got %d", got)
	}
}

// Test_Changeset_RestartReplaysLog checks a node restarted from its Raft log
// reaches the same data, and that statement-path entries logged before the
// feature was switched on still apply.
func Test_Changeset_LogCompatibility(t *testing.T) {
	// Write some entries as SQL text, as an older version would have.
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO foo(v) VALUES('old')`,
	} {
		if _, _, err := s.Execute(context.Background(), executeRequestFromString(sql, false, false)); err != nil {
			t.Fatal(err)
		}
	}
	// A follower with the feature on joins, replaying the SQL-text entries.
	f, fln := mustNewStore(t)
	defer fln.Close()
	f.ChangesetReplication = true
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close(true)
	s.ChangesetReplication = true
	if err := s.Join(joinRequest(f.ID(), f.Addr(), true)); err != nil {
		t.Fatal(err)
	}
	defer s.Close(true)
	if _, _, err := s.Execute(context.Background(), executeRequestFromString(`INSERT INTO foo(v) VALUES('new')`, false, false)); err != nil {
		t.Fatal(err)
	}
	waitConverged(t, []*Store{s, f})
	if a, b := dumpStore(t, s), dumpStore(t, f); a != b || !strings.Contains(a, "old") || !strings.Contains(a, "new") {
		t.Fatalf("nodes differ or lack data\n%s\n%s", a, b)
	}
}
