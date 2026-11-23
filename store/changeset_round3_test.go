package store

import (
	"context"
	"errors"
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
)

func r3Count(t *testing.T, s *Store, sqlText string) int64 {
	t.Helper()
	q := queryRequestFromString(sqlText, false, false, false)
	q.Level = proto.ConsistencyLevel_NONE
	r, _, _, err := s.Query(context.Background(), q)
	if err != nil || r[0].Error != "" {
		t.Fatalf("query %q: %v %v", sqlText, err, r)
	}
	return r[0].Values[0].Parameters[0].GetI()
}

// Every acknowledged write is on every node: INTEGER PRIMARY KEY DESC stores a
// NULL key the session does not record, so the capture is refused and the
// request replicates as SQL.
func Test_Round3_ClusterIntegerPKDescIsNotLost(t *testing.T) {
	stores := mustNewCluster(t, 3, true)
	execOn(t, stores[0], "CREATE TABLE t (id INTEGER PRIMARY KEY DESC, v TEXT)")
	execOn(t, stores[0], "INSERT INTO t VALUES(NULL, 'x')")
	wantConverged(t, stores)
	for i, s := range stores {
		if n := r3Count(t, s, "SELECT count(*) FROM t"); n != 1 {
			t.Fatalf("node %d holds %d rows", i, n)
		}
	}
}

// A TEMP table shadowing a main one must not hide a trigger's write.
func Test_Round3_ClusterTempShadowDoesNotLoseTriggerWrites(t *testing.T) {
	stores := mustNewCluster(t, 3, true)
	execOn(t, stores[0], "CREATE TABLE acct (id INTEGER PRIMARY KEY, bal INTEGER)")
	execOn(t, stores[0], "CREATE TABLE audit (what TEXT)")
	execOn(t, stores[0], "CREATE TRIGGER a AFTER INSERT ON acct BEGIN INSERT INTO audit VALUES('insert ' || new.id); END")
	execOn(t, stores[0], "INSERT INTO acct VALUES(1, 10)")
	execOn(t, stores[0], "CREATE TEMP TABLE audit (id INTEGER PRIMARY KEY, what TEXT)")
	execOn(t, stores[0], "INSERT INTO acct VALUES(2, 20)")
	execOn(t, stores[0], "INSERT INTO main.audit VALUES('direct')")
	wantConverged(t, stores)
	for i, s := range stores {
		if n := r3Count(t, s, "SELECT count(*) FROM main.audit"); n != 3 {
			t.Fatalf("node %d holds %d audit rows, want 3", i, n)
		}
	}
}

// INSERT OR ROLLBACK and a COMMIT inside one request converge on three nodes.
func Test_Round3_ClusterDivergenceReproduction(t *testing.T) {
	stores := mustNewCluster(t, 3, true)
	execOn(t, stores[0], "CREATE TABLE foo (id INTEGER PRIMARY KEY)", "CREATE TABLE bar (v TEXT)")
	execOn(t, stores[0], "INSERT INTO foo VALUES(1)")
	execOn(t, stores[0], "INSERT OR ROLLBACK INTO foo VALUES(1)", "INSERT INTO bar VALUES('once')")
	execOn(t, stores[0], "INSERT INTO foo VALUES(2); COMMIT; INSERT INTO foo VALUES(3)")
	wantConverged(t, stores)
}

// A deferred foreign key violation is what COMMIT would refuse: it is never
// replicated as a changeset that ignores it, and the nodes agree.
func Test_Round3_ClusterDeferredForeignKey(t *testing.T) {
	stores := mustNewCluster(t, 3, true)
	execOn(t, stores[0], "PRAGMA foreign_keys=ON")
	execOn(t, stores[0], "CREATE TABLE p (id INTEGER PRIMARY KEY)",
		"CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)")
	_, _, _ = stores[0].Execute(context.Background(), executeRequestFromStrings([]string{"INSERT INTO c VALUES(1, 99)"}, false, false))
	wantConverged(t, stores)
}

// A damaged changeset entry is not fatal: the node stays up and unchanged.
func Test_Round3_MalformedChangesetNotFatal(t *testing.T) {
	stores := mustNewCluster(t, 1, true)
	execOn(t, stores[0], "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	af := stores[0].raft.Apply(mustChangesetCommand(t, []byte("T\x02\x01\x00foo\x00\x12\x00\x01")), stores[0].ApplyTimeout)
	if af.Error() != nil {
		t.Fatal(af.Error())
	}
	if r := af.Response().(*fsmGenericResponse); r.error == nil {
		t.Fatal("a damaged changeset was applied")
	}
	execOn(t, stores[0], "INSERT INTO foo VALUES(1, 'still alive')")
	if n := r3Count(t, stores[0], "SELECT count(*) FROM foo"); n != 1 {
		t.Fatalf("%d rows", n)
	}
}

// A request that changed nothing is answered from the local database, so the
// quorum must confirm the node still leads before the answer is returned.
func Test_Round3_NoChangeRequestVerifiesLeader(t *testing.T) {
	s := mustNewCluster(t, 1, true)[0]
	execOn(t, s, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	real := verifyLeader
	defer func() { verifyLeader = real }()
	calls := 0
	verifyLeader = func(*Store) error { calls++; return ErrNotLeader }
	eqr := executeQueryRequestFromStrings([]string{`DELETE FROM foo WHERE id = 99`, `SELECT 1`}, proto.ConsistencyLevel_STRONG, false, false, false)
	if _, _, _, err := s.Request(context.Background(), eqr); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
	if _, _, err := s.Execute(context.Background(), executeRequestFromStrings([]string{`DELETE FROM foo WHERE id = 99`}, false, false)); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("execute: expected ErrNotLeader, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("verifyLeader called %d times", calls)
	}
}

// A follower stops at the barrier: nothing is captured, and no answer is
// returned even if the quorum check would pass.
func Test_Changeset_Round3_BarrierFailureReturnsBeforeCapture(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	mustWrite(t, stores[0], `CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)`)
	waitConverged(t, stores)
	real := verifyLeader
	defer func() { verifyLeader = real }()
	verifyLeader = func(*Store) error { return nil }
	read := executeRequestFromString(`SELECT 1`, false, false).Request
	res, _, ok, err := stores[1].changesetApply(context.Background(), read, false, false)
	if !errors.Is(err, ErrNotLeader) || !ok || res != nil {
		t.Fatalf("expected ErrNotLeader before any capture, got res=%v ok=%v err=%v", res, ok, err)
	}
}

// An entry that the apply refuses (damaged bytes) is an error to the caller,
// and the entry was logged.
func Test_Changeset_Round3_ApplyErrorIsReturned(t *testing.T) {
	s := mustNewCluster(t, 1, true)[0]
	execOn(t, s, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	realCmd := commandMarshal
	defer func() { commandMarshal = realCmd }()
	commandMarshal = func(*proto.Command) ([]byte, error) {
		return mustChangesetCommand(t, []byte("T\x02\x01\x00foo\x00\x12\x00\x01")), nil
	}
	_, _, ok, logged, err := s.changesetApplyLogged(context.Background(),
		executeRequestFromString(`INSERT INTO foo(v) VALUES('x')`, false, false).Request, false, false)
	if err == nil || !ok || !logged {
		t.Fatalf("expected an apply error from a logged entry, got ok=%v logged=%v err=%v", ok, logged, err)
	}
}

// A Leader whose term changed between the capture and the proposal replicates
// nothing.
func Test_Round3_DeposedBetweenCaptureAndProposal(t *testing.T) {
	s := mustNewCluster(t, 1, true)[0]
	execOn(t, s, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	real := currentTerm
	defer func() { currentTerm = real }()
	n := 0
	currentTerm = func(st *Store) uint64 {
		n++
		if n >= 2 { // the first call is the term before the capture, the second the re-check
			return real(st) + 1
		}
		return real(st)
	}
	idx := s.raft.AppliedIndex()
	_, _, err := s.Execute(context.Background(), executeRequestFromStrings([]string{`INSERT INTO foo(v) VALUES('x')`}, false, false))
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
	currentTerm = real
	if s.raft.AppliedIndex() != idx {
		t.Fatal("an entry was proposed by a deposed Leader")
	}
	if n := r3Count(t, s, "SELECT count(*) FROM foo"); n != 0 {
		t.Fatalf("%d rows", n)
	}
}
