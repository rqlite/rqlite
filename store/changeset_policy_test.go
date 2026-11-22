package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
	sql "github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/random"
)

// A request that wrote nothing never went through the log, so it must not
// count as a STRONG read: that would let later strong reads skip the log.
func Test_Changeset_NoChangeIsNotAStrongRead(t *testing.T) {
	s := mustNewCluster(t, 1, true)[0]
	execOn(t, s, `CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)`)
	before := s.strongReadTerm.Load()
	eqr := executeQueryRequestFromStrings([]string{`DELETE FROM foo WHERE id = 99`, `SELECT 1`}, proto.ConsistencyLevel_STRONG, false, false, false)
	if _, _, _, err := s.Request(context.Background(), eqr); err != nil {
		t.Fatal(err)
	}
	if got := s.strongReadTerm.Load(); got != before {
		t.Fatalf("strongReadTerm moved from %d to %d without a log entry", before, got)
	}
	// A request that does change something goes through the log and does count.
	eqr = executeQueryRequestFromStrings([]string{`INSERT INTO foo(v) VALUES('x')`, `SELECT 1`}, proto.ConsistencyLevel_STRONG, false, false, false)
	if _, _, _, err := s.Request(context.Background(), eqr); err != nil {
		t.Fatal(err)
	}
	if s.strongReadTerm.Load() == before {
		t.Fatal("a logged request did not record the strong read")
	}
}

// A refused changeset is counted, and the node stays up and unchanged.
func Test_Changeset_RefusalIsCounted(t *testing.T) {
	s := mustNewCluster(t, 1, true)[0]
	mustWrite(t, s, `CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)`)
	mustWrite(t, s, `INSERT INTO foo VALUES(1, 'a')`)
	// An entry no node can apply: a damaged changeset.
	data := mustChangesetCommand(t, []byte("garbage that is not a changeset"))
	af := s.raft.Apply(data, s.ApplyTimeout)
	if af.Error() != nil {
		t.Fatal(af.Error())
	}
	r := af.Response().(*fsmGenericResponse)
	if r.error == nil {
		t.Fatal("a damaged changeset was applied")
	}
	if got := s.numChangesetDivergences.Load(); got != 1 {
		t.Fatalf("expected one divergence, got %d", got)
	}
	// The node is alive and keeps replicating.
	mustWrite(t, s, `INSERT INTO foo VALUES(2, 'b')`)
}

// Changeset replication needs the connection's pre-update hook, as CDC does.
func Test_Changeset_RefusesCDC(t *testing.T) {
	ly := mustMockLayer("localhost:0")
	defer ly.Close()
	s := New(&Config{
		DBConf: NewDBConfig(),
		Dir:    t.TempDir(),
		ID:     random.String(),
		CDC:    &CDCConfig{},
	}, ly)
	s.ChangesetReplication = true
	err := s.Open()
	if err == nil || !strings.Contains(err.Error(), "CDC") {
		t.Fatalf("expected a refusal naming CDC, got %v", err)
	}
}

// A changeset above the limit fails the request, and is not replicated as SQL.
func Test_Changeset_TooLargeFailsTheRequest(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	execOn(t, stores[0], `CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)`)
	stores[0].ChangesetMaxSize = 64
	_, _, err := stores[0].Execute(context.Background(), executeRequestFromStrings([]string{`INSERT INTO foo(v) VALUES(hex(zeroblob(1000)))`}, false, false))
	if !errors.Is(err, sql.ErrChangesetTooLarge) {
		t.Fatalf("expected a too-large error, got %v", err)
	}
	wantConverged(t, stores)
	stores[0].ChangesetMaxSize = 0
	execOn(t, stores[0], `INSERT INTO foo(v) VALUES('small')`)
	wantConverged(t, stores)
}

// Load swaps the database, so it waits for a write in flight.
func Test_Changeset_LoadWaitsForWrites(t *testing.T) {
	s := mustNewCluster(t, 1, true)[0]
	s.changesetMu.Lock()
	done := make(chan error, 1)
	go func() { done <- s.Load(context.Background(), &proto.LoadRequest{}) }()
	select {
	case err := <-done:
		t.Fatalf("Load did not wait for the write lock: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	s.changesetMu.Unlock()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Load never ran")
	}
}

// A follower asked to lead reports the store's own error.
func Test_Changeset_NotLeaderIsMapped(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	_, _, _, _, err := stores[1].changesetApplyLogged(context.Background(),
		executeRequestFromString(`INSERT INTO foo VALUES(1)`, false, false).Request, false, false)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}
