//go:build changeset_knownbugs

// Known-bug tests for the changeset replication prototype (round 3 reviews,
// see docs/changeset-reviews/). Each test asserts the CORRECT behaviour and
// FAILS today. They are behind a build tag so the default suite stays green:
//
//	CGO_ENABLED=0 go test -tags changeset_knownbugs ./db/ ./store/
package db

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// kbMustReplicate runs stmts through the statement path on a reference node
// (the ground truth), and through the capture path on a. The capture path must
// either refuse (ErrNotCapturable: the statement path is then used) or produce
// a changeset which, applied to an identical node, yields the reference state
// without error.
func kbMustReplicate(t *testing.T, fk bool, schema []string, stmts ...string) {
	t.Helper()
	var a, b *DB
	if fk {
		a, _ = mustCreateOnDiskDatabaseWALFK()
		b, _ = mustCreateOnDiskDatabaseWALFK()
		t.Cleanup(func() { a.Close(); b.Close() })
		for _, s := range schema {
			mustExecute(a, s)
			mustExecute(b, s)
		}
	} else {
		a, b = newPair(t, schema...)
	}
	var ref *DB
	if fk {
		ref, _ = mustCreateOnDiskDatabaseWALFK()
	} else {
		ref, _ = mustCreateOnDiskDatabaseWAL()
	}
	defer ref.Close()
	for _, s := range schema {
		mustExecute(ref, s)
	}
	req := csRequest(false, stmts...)
	if _, err := ref.Execute(req, false); err != nil {
		t.Fatalf("statement path failed: %v", err)
	}
	_, cs, err := a.ExecuteCapture(context.Background(), req, false)
	if errors.Is(err, ErrNotCapturable) {
		return // correct: falls back to the statement path
	}
	if err != nil {
		t.Fatalf("capture failed with a non-fallback error: %v", err)
	}
	if err := b.ApplyChangeset(context.Background(), cs); err != nil {
		t.Fatalf("changeset captured but cannot be applied to an identical node: %v", err)
	}
	if got, want := dump(t, b), dump(t, ref); got != want {
		t.Fatalf("changeset applied to a follower diverges from the statement path (acknowledged write lost):\nfollower:\n%s\nstatement path:\n%s", got, want)
	}
}

// --- Completeness: counting cannot prove a changeset is complete ---------

func Test_KnownBug_NullInIntegerPKDescLostWhenOffsetByReplaceDelete(t *testing.T) {
	kbMustReplicate(t, false, []string{
		"CREATE TABLE foo (k INTEGER PRIMARY KEY DESC, v TEXT)",
		"CREATE TABLE bar (id INTEGER PRIMARY KEY, u TEXT UNIQUE, v TEXT)",
		"INSERT INTO bar VALUES(1, 'k', 'old')",
	}, "INSERT INTO foo(k, v) VALUES(NULL, 'lost')", "REPLACE INTO bar VALUES(2, 'k', 'new')")
}

func Test_KnownBug_NullInIntegerPKDescLostWhenOffsetByWithoutRowidWrite(t *testing.T) {
	kbMustReplicate(t, false, []string{
		"CREATE TABLE foo (k INTEGER PRIMARY KEY DESC, v TEXT)",
		"CREATE TABLE w (id TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID",
	}, "INSERT INTO foo(k, v) VALUES(NULL, 'lost')", "INSERT INTO w VALUES('a', 'kept')")
}

func Test_KnownBug_NullInIntegerPKDescLostWhenOffsetByWithoutRowidTrigger(t *testing.T) {
	kbMustReplicate(t, false, []string{
		"CREATE TABLE foo (k INTEGER PRIMARY KEY DESC, v TEXT)",
		"CREATE TABLE w (id TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID",
		"CREATE TRIGGER foo_ai AFTER INSERT ON foo BEGIN INSERT INTO w VALUES(new.v, 'audit'); END",
	}, "INSERT INTO foo(k, v) VALUES(NULL, 'lost')")
}

func Test_KnownBug_NullInIntegerPKDescLostWhenOffsetByPKUpdate(t *testing.T) {
	kbMustReplicate(t, false, []string{
		"CREATE TABLE foo (k INTEGER PRIMARY KEY DESC, v TEXT)",
		"CREATE TABLE bar (id INTEGER PRIMARY KEY, v TEXT)",
		"INSERT INTO bar VALUES(1, 'x')",
	}, "INSERT INTO foo(k, v) VALUES(NULL, 'lost')", "UPDATE bar SET id = 2 WHERE id = 1")
}

func Test_KnownBug_AutoincrementSequenceDivergesWhenOffsetByWithoutRowidWrite(t *testing.T) {
	kbMustReplicate(t, false, []string{
		"CREATE TABLE foo (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
		"CREATE TABLE w (id TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID",
	}, "INSERT INTO foo(v) VALUES('a')", "DELETE FROM foo", "INSERT INTO w VALUES('a', 'kept')")
}

// --- A captured changeset that is fatal on an identical node -------------

func Test_KnownBug_PrimaryKeySwapDivergesOnApply(t *testing.T) {
	kbMustReplicate(t, false, []string{
		"CREATE TABLE p (k TEXT PRIMARY KEY NOT NULL, u TEXT UNIQUE NOT NULL)",
		"INSERT INTO p VALUES('x', '1')", "INSERT INTO p VALUES('y', '2')",
	}, "UPDATE p SET k = 'tmp' WHERE k = 'x'", "UPDATE p SET k = 'x' WHERE k = 'y'", "UPDATE p SET k = 'y' WHERE k = 'tmp'")
}

// --- A peer-supplied conflicting changeset must not be a fatal error -----

func Test_KnownBug_ConflictingPeerChangesetIsFatalClass(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	_, cs, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO t VALUES(1,'x')"), false)
	if err != nil || len(cs) == 0 {
		t.Fatalf("capture: err=%v cs=%d", err, len(cs))
	}
	// Every node of a healthy cluster already holds the row: one such entry
	// conflicts on all of them at once.
	b, _ := newPair(t, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExecute(b, "INSERT INTO t VALUES(1,'y')")
	e := b.ApplyChangeset(context.Background(), cs)
	if errors.Is(e, ErrChangesetConflict) {
		t.Fatalf("a conflicting changeset is classed ErrChangesetConflict, which the FSM turns into a fatal error on every node: %v", e)
	}
}

// --- Error classification by message text ---------------------------------

func Test_KnownBug_RaiseRollbackWithTimeoutTextMisdetectedAsInterrupt(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)",
		"CREATE TRIGGER boom BEFORE INSERT ON foo BEGIN SELECT RAISE(ROLLBACK, 'request timeout: please retry'); END")
	_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo VALUES(1)"), false)
	if err != nil && !errors.Is(err, ErrNotCapturable) && strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("a deterministic RAISE(ROLLBACK) whose text contains 'timeout' is treated as an interrupt: %v", err)
	}
}
