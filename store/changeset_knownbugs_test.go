//go:build changeset_knownbugs

// Known-bug tests for the changeset replication prototype (round 3 reviews,
// see docs/changeset-reviews/). Each asserts the CORRECT behaviour and FAILS
// today. Run: CGO_ENABLED=0 go test -tags changeset_knownbugs ./db/ ./store/
package store

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// A client makes the nodes diverge through the statement path (a DDL-led
// request is not capturable, and randomblob(?) is never rewritten), then sends
// a captured DELETE of that row. Every node whose blob differs from the
// Leader's refuses the changeset, and the refusal is fatal. The scenario runs
// in a child process because the fatal error exits the process.
func Test_KnownBug_ClientKillsFollowersWithDDLLedRandomblobThenDelete(t *testing.T) {
	if os.Getenv("KB_CHILD") == "1" {
		stores := mustNewCluster(t, 2, true)
		ex := func(er *proto.ExecuteRequest) {
			r, _, err := stores[0].Execute(context.Background(), er)
			t.Logf("execute: %v err=%v", r, err)
		}
		ex(executeRequestFromString(`CREATE TABLE foo (id INTEGER PRIMARY KEY, b BLOB)`, false, false))
		ex(&proto.ExecuteRequest{Request: &proto.Request{Statements: []*proto.Statement{
			{Sql: `CREATE TABLE IF NOT EXISTS z (id INTEGER PRIMARY KEY)`},
			{Sql: `INSERT INTO foo(id, b) VALUES(1, randomblob(?))`, Parameters: []*proto.Parameter{{Value: &proto.Parameter_I{I: 8}}}},
		}}})
		waitConverged(t, stores)
		ex(executeRequestFromString(`DELETE FROM foo WHERE id = 1`, false, false))
		waitConverged(t, stores)
		t.Logf("CHILD SURVIVED")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^Test_KnownBug_ClientKillsFollowersWithDDLLedRandomblobThenDelete$", "-test.v")
	cmd.Env = append(os.Environ(), "KB_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "CHILD SURVIVED") {
		t.Fatalf("a follower process died on a client's DELETE (exit: %v); tail of output:\n%s", err, tail(string(out), 1500))
	}
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

func kbNoLostRow(t *testing.T, stores []*Store) {
	t.Helper()
	waitConverged(t, stores)
	for i, s := range stores {
		d := dumpStore(t, s)
		found := false
		for _, l := range strings.Split(d, "\n") {
			if strings.HasPrefix(l, "foo:") && strings.Contains(l, `"lost"`) {
				found = true
			}
		}
		if !found {
			t.Fatalf("acknowledged INSERT into foo is missing on node %d:\n%s", i, d)
		}
	}
}

// One INSERT, acknowledged, lost on every node: the NULL-keyed row is not
// recorded and the trigger's WITHOUT ROWID row makes the counts agree.
func Test_KnownBug_AcknowledgedInsertLostNullInIntegerPKDescWithWithoutRowidTrigger(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	mustWrite(t, stores[0], `CREATE TABLE foo (k INTEGER PRIMARY KEY DESC, v TEXT)`)
	mustWrite(t, stores[0], `CREATE TABLE w (id TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID`)
	mustWrite(t, stores[0], `CREATE TRIGGER foo_ai AFTER INSERT ON foo BEGIN INSERT INTO w VALUES(new.v, 'audit'); END`)
	if _, _, err := stores[0].Execute(context.Background(),
		executeRequestFromString(`INSERT INTO foo(k, v) VALUES(NULL, 'lost')`, false, false)); err != nil {
		t.Fatal(err)
	}
	kbNoLostRow(t, stores)
}

func Test_KnownBug_AcknowledgedInsertLostNullInIntegerPKDescWithReplaceDelete(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	mustWrite(t, stores[0], `CREATE TABLE foo (k INTEGER PRIMARY KEY DESC, v TEXT)`)
	mustWrite(t, stores[0], `CREATE TABLE bar (id INTEGER PRIMARY KEY, u TEXT UNIQUE, v TEXT)`)
	mustWrite(t, stores[0], `INSERT INTO bar VALUES(1, 'k', 'old')`)
	if _, _, err := stores[0].Execute(context.Background(),
		executeRequestFromString(`INSERT INTO foo(k, v) VALUES(NULL, 'lost'); REPLACE INTO bar VALUES(2, 'k', 'new')`, false, false)); err != nil {
		t.Fatal(err)
	}
	kbNoLostRow(t, stores)
}
