package store

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/random"
)

// Test_StoreCDC_Config tests that CDC is enabled by, and only by, the Store's
// configuration.
func Test_StoreCDC_Config(t *testing.T) {
	check := func(s *Store, want bool) {
		t.Helper()
		if err := s.Open(); err != nil {
			t.Fatalf("failed to open store: %v", err)
		}
		defer s.Close(true)
		stats, err := s.Stats()
		if err != nil {
			t.Fatalf("failed to get stats: %v", err)
		}
		if enabled := stats["cdc"].(map[string]any)["enabled"]; enabled != want {
			t.Fatalf("expected CDC enabled to be %v, got %v", want, enabled)
		}
	}

	s, ln := mustNewStore(t)
	defer ln.Close()
	check(s, false)

	s, ln = mustNewStoreCDC(t)
	defer ln.Close()
	check(s, true)
}

func Test_StoreCDC_RolledBackInsert(t *testing.T) {
	s, ln := mustNewStoreCDC(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap store: %v", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to wait for leader: %v", err)
	}
	if _, _, err := s.Execute(context.Background(), executeRequestFromString(
		"CREATE TABLE foo (id INTEGER PRIMARY KEY)", false, false)); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	results, _, err := s.Execute(context.Background(), &proto.ExecuteRequest{
		Request: &proto.Request{Statements: []*proto.Statement{
			{Sql: "INSERT INTO foo VALUES(1), (1)"},
			{Sql: "INSERT INTO foo VALUES(2)"},
		}},
	})
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}
	if len(results) != 2 || results[0].GetError() == "" || results[1].GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	select {
	case group := <-s.CDCEventsC():
		if len(group.Events) != 1 || group.Events[0].NewRowId != 2 {
			t.Fatalf("unexpected committed group: %s", asJSON(group))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for committed insert")
	}
}

// Test_StoreCDC_Events_Single tests that CDC events are actually sent when database changes occur.
func Test_StoreCDC_Events_Single(t *testing.T) {
	s, ln := mustNewStoreCDC(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	er = executeRequestFromString(`INSERT INTO foo(id, name) VALUES(101, "fiona")`, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	timeout := time.After(5 * time.Second)
	select {
	case events := <-s.CDCEventsC():
		if events == nil {
			t.Fatalf("received nil CDC events")
		}
		if len(events.Events) != 1 {
			t.Fatalf("expected 1 CDC event, got %d", len(events.Events))
		}
		ev := events.Events[0]

		if ev.Table != "foo" {
			t.Fatalf("expected table name to be 'foo', got %s", ev.Table)
		}
		if !slices.Equal(ev.ColumnNames, []string{"id", "name"}) {
			t.Fatalf("expected column names to be [id name], got %v", ev.ColumnNames)
		}
		if ev.Op != proto.CDCEvent_INSERT {
			t.Fatalf("expected CDC event operation to be INSERT, got %s", ev.Op)
		}
		if ev.NewRowId != 101 {
			t.Fatalf("expected new row ID to be 101, got %d", ev.NewRowId)
		}
		if ev.NewRow.Values[0].GetI() != 101 {
			t.Fatalf("expected new row ID value to be 1, got %d", ev.NewRow.Values[0].GetI())
		}
		if ev.NewRow.Values[1].GetS() != "fiona" {
			t.Fatalf("expected new row name value to be 'fiona', got %s", ev.NewRow.Values[1].GetS())
		}
	case <-timeout:
		t.Fatalf("timeout waiting for CDC INSERT event for table 'foo'")
	}
}

// Test_StoreCDC_Events_Twice checks that the reset-lifecycle of the CDCCollator is handled properly
// across two distinct changes to the database.
func Test_StoreCDC_Events_Twice(t *testing.T) {
	s, ln := mustNewStoreCDC(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	timeout := time.After(5 * time.Second)

	// Write first event.
	er = executeRequestFromString(`INSERT INTO foo(id, name) VALUES(101, "alice")`, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}
	select {
	case events := <-s.CDCEventsC():
		if events == nil {
			t.Fatalf("received nil CDC events")
		}
		if len(events.Events) != 1 {
			t.Fatalf("expected 1 CDC event, got %d", len(events.Events))
		}
		ev := events.Events[0]

		if ev.Table != "foo" {
			t.Fatalf("expected table name to be 'foo', got %s", ev.Table)
		}
		if !slices.Equal(ev.ColumnNames, []string{"id", "name"}) {
			t.Fatalf("expected column names to be [id name], got %v", ev.ColumnNames)
		}
		if ev.Op != proto.CDCEvent_INSERT {
			t.Fatalf("expected CDC event operation to be INSERT, got %s", ev.Op)
		}
		if ev.NewRowId != 101 {
			t.Fatalf("expected new row ID to be 101, got %d", ev.NewRowId)
		}
		if ev.NewRow.Values[0].GetI() != 101 {
			t.Fatalf("expected new row ID value to be 1, got %d", ev.NewRow.Values[0].GetI())
		}
		if ev.NewRow.Values[1].GetS() != "alice" {
			t.Fatalf("expected new row name value to be 'alice', got %s", ev.NewRow.Values[1].GetS())
		}
	case <-timeout:
		t.Fatalf("timeout waiting for CDC INSERT event for table 'foo'")
	}

	// Write a second row.
	er = executeRequestFromString(`INSERT INTO foo(id, name) VALUES(102, "bob")`, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}
	select {
	case events := <-s.CDCEventsC():
		if events == nil {
			t.Fatalf("received nil CDC events")
		}
		if len(events.Events) != 1 {
			t.Fatalf("expected 1 CDC event, got %d", len(events.Events))
		}
		ev := events.Events[0]

		if ev.Table != "foo" {
			t.Fatalf("expected table name to be 'foo', got %s", ev.Table)
		}
		if !slices.Equal(ev.ColumnNames, []string{"id", "name"}) {
			t.Fatalf("expected column names to be [id name], got %v", ev.ColumnNames)
		}
		if ev.Op != proto.CDCEvent_INSERT {
			t.Fatalf("expected CDC event operation to be INSERT, got %s", ev.Op)
		}
		if ev.NewRowId != 102 {
			t.Fatalf("expected new row ID to be 101, got %d", ev.NewRowId)
		}
		if ev.NewRow.Values[0].GetI() != 102 {
			t.Fatalf("expected new row ID value to be 1, got %d", ev.NewRow.Values[0].GetI())
		}
		if ev.NewRow.Values[1].GetS() != "bob" {
			t.Fatalf("expected new row name value to be 'declan', got %s", ev.NewRow.Values[1].GetS())
		}
	case <-timeout:
		t.Fatalf("timeout waiting for CDC INSERT event for table 'foo'")
	}
}

// Test_StoreCDC_Events_MultiStatementIndex ensures that a bulk request with
// multiple statements emits the correct group of CDC events.
//
// The Store resets the CDC streamer with the Raft index once per log entry,
// then processes every statement in that entry. A non-transactional request
// with two statements autocommits twice, so SQLite fires the commit hook
// twice during that single apply. Every CDC group the Store emits for the
// request must be tagged with the request's Raft index, because the CDC
// service uses that index to deduplicate and to advance its high watermark.
func Test_StoreCDC_Events_MultiStatementIndex(t *testing.T) {
	s, ln := mustNewStoreCDC(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY)`, false, false)
	if _, _, err := s.Execute(context.Background(), er); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	// One bulk request, no transaction, so each committed seperately.
	er = executeRequestFromStrings([]string{
		`INSERT INTO foo(id) VALUES(1)`,
		`INSERT INTO foo(id) VALUES(2)`,
	}, false, false)
	results, raftIndex, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute inserts: %s", err.Error())
	}
	for _, r := range results {
		if r.GetError() != "" {
			t.Fatalf("unexpected statement error: %s", r.GetError())
		}
	}

	// Collect every group emitted for this request, until both inserted
	// rows have been seen. The test does not care whether they arrive in
	// one group or two, only that each group carries the request's index.
	var groups []*proto.CDCIndexedEventGroup
	numEvents := 0
	timeout := time.After(5 * time.Second)
	for numEvents < 2 {
		select {
		case g := <-s.CDCEventsC():
			if g == nil {
				t.Fatalf("received nil CDC event group")
			}
			groups = append(groups, g)
			numEvents += len(g.Events)
		case <-timeout:
			t.Fatalf("timeout waiting for CDC events, got %d of 2 events in %d groups",
				numEvents, len(groups))
		}
	}

	for i, g := range groups {
		if g.Index != raftIndex {
			t.Fatalf("group %d of %d: expected Raft index %d, got %d (rows %v)",
				i+1, len(groups), raftIndex, g.Index, rowIDs(g))
		}
	}
}

// rowIDs returns the new row IDs in a CDC group, for test diagnostics.
func rowIDs(g *proto.CDCIndexedEventGroup) []int64 {
	ids := make([]int64, 0, len(g.Events))
	for _, ev := range g.Events {
		ids = append(ids, ev.NewRowId)
	}
	return ids
}

// mustNewStoreCDC returns a new Store with CDC enabled.
func mustNewStoreCDC(t *testing.T) (*Store, net.Listener) {
	ly := mustMockLayer("localhost:0")
	s := New(&Config{
		DBConf: NewDBConfig(),
		Dir:    t.TempDir(),
		ID:     random.String(),
		CDC:    &CDCConfig{},
	}, ly)
	if s == nil {
		panic("failed to create new store")
	}
	return s, ly
}
