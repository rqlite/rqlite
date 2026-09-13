package db

import (
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

func Test_CDCStreamer_New(t *testing.T) {
	ch := make(chan *command.CDCIndexedEventGroup, 10)
	streamer, err := NewCDCStreamer(ch, &mockColumnNamesProvider{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if streamer == nil {
		t.Fatalf("expected CDCStreamer to be created, got nil")
	}
	if len(streamer.pending.Events) != 0 {
		t.Fatalf("expected no pending events after commit, got %d", len(streamer.pending.Events))
	}
	if err := streamer.Close(); err != nil {
		t.Fatalf("expected no error on close, got %v", err)
	}
}

func Test_CDCStreamer_CommitOne(t *testing.T) {
	ch := make(chan *command.CDCIndexedEventGroup, 10)
	np := &mockColumnNamesProvider{
		columns: map[string][]string{
			"test_table": {"id", "name", "value"},
		},
	}
	streamer, err := NewCDCStreamer(ch, np)
	if err != nil {
		t.Fatalf("error creating CDCStreamer: %v", err)
	}

	streamer.Reset(5678)
	change := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_INSERT,
		OldRowId: 100,
		NewRowId: 200,
	}
	if err := streamer.PreupdateHook(change); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	streamer.CommitHook()
	if len(streamer.pending.Events) != 0 {
		t.Fatalf("expected no pending events after commit, got %d", len(streamer.pending.Events))
	}

	select {
	case ev := <-ch:
		if ev.Index != 5678 {
			t.Fatalf("expected index value to be 5678, got %d", ev.Index)
		}
		if len(ev.Events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(ev.Events))
		}
		if !slices.Equal(ev.Events[0].ColumnNames, []string{"id", "name", "value"}) {
			t.Fatalf("expected column names to be [id name value], got %v", ev.Events[0].ColumnNames)
		}
		if !reflect.DeepEqual(change, ev.Events[0]) {
			t.Fatalf("received event does not match sent event: expected %v, got %v", change, ev.Events[0])
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for event on channel")
	}

	if err := streamer.Close(); err != nil {
		t.Fatalf("expected no error on close, got %v", err)
	}
}

func Test_CDCStreamer_CommitTwo(t *testing.T) {
	ch := make(chan *command.CDCIndexedEventGroup, 10)
	np := &mockColumnNamesProvider{
		columns: map[string][]string{
			"test_table": {"id", "name", "value"},
		},
	}
	streamer, err := NewCDCStreamer(ch, np)
	if err != nil {
		t.Fatalf("error creating CDCStreamer: %v", err)
	}

	streamer.Reset(9012)
	change1 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_UPDATE,
		OldRowId: 300,
		NewRowId: 400,
	}
	change2 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_DELETE,
		OldRowId: 500,
		NewRowId: 0,
	}

	if err := streamer.PreupdateHook(change1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := streamer.PreupdateHook(change2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	streamer.CommitHook()
	if len(streamer.pending.Events) != 0 {
		t.Fatalf("expected no pending events after commit, got %d", len(streamer.pending.Events))
	}

	select {
	case ev := <-ch:
		if ev.Index != 9012 {
			t.Fatalf("expected index value to be 9012, got %d", ev.GetIndex())
		}
		if len(ev.Events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(ev.Events))
		}
		if !reflect.DeepEqual(change1, ev.Events[0]) {
			t.Fatalf("received first event does not match sent event: expected %v, got %v", change1, ev.Events[0])
		}
		if !slices.Equal(ev.Events[0].ColumnNames, []string{"id", "name", "value"}) {
			t.Fatalf("expected column names to be [id name value], got %v", ev.Events[0].ColumnNames)
		}
		if !reflect.DeepEqual(change2, ev.Events[1]) {
			t.Fatalf("received second event does not match sent event: expected %v, got %v", change2, ev.Events[1])
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for event on channel")
	}

	if err := streamer.Close(); err != nil {
		t.Fatalf("expected no error on close, got %v", err)
	}
}

// Test_NewCDCStreamer_ResetThenPreupdate tests the behavior of the CDCStreamer
// when  predupdate is called followed by a reset. It ensures that the reset
// clears out any pending events.
func Test_CDCStreamer_ResetThenPreupdate(t *testing.T) {
	ch := make(chan *command.CDCIndexedEventGroup, 10)
	np := &mockColumnNamesProvider{
		columns: map[string][]string{
			"test_table": {"id", "name", "value"},
		},
	}
	streamer, err := NewCDCStreamer(ch, np)
	if err != nil {
		t.Fatalf("error creating CDCStreamer: %v", err)
	}

	streamer.Reset(1234)
	change1 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_INSERT,
		OldRowId: 100,
		NewRowId: 200,
	}
	if err := streamer.PreupdateHook(change1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if streamer.Len() != 1 {
		t.Fatalf("expected 1 pending event, got %d", streamer.Len())
	}

	streamer.Reset(5678)
	if streamer.Len() != 0 {
		t.Fatalf("expected no pending events after reset, got %d", streamer.Len())
	}
	change2 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_UPDATE,
		OldRowId: 300,
		NewRowId: 400,
	}
	if err := streamer.PreupdateHook(change2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	streamer.CommitHook()
	if len(streamer.pending.Events) != 0 {
		t.Fatalf("expected no pending events after commit, got %d", len(streamer.pending.Events))
	}

	select {
	case ev := <-ch:
		if ev.Index != 5678 {
			t.Fatalf("expected K value to be 5678, got %d", ev.Index)
		}
		if len(ev.Events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(ev.Events))
		}
		if !slices.Equal(ev.Events[0].ColumnNames, []string{"id", "name", "value"}) {
			t.Fatalf("expected column names to be [id name value], got %v", ev.Events[0].ColumnNames)
		}
		if !reflect.DeepEqual(change2, ev.Events[0]) {
			t.Fatalf("received event does not match sent event: expected %v, got %v", change2, ev.Events[0])
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for event on channel")
	}

	if err := streamer.Close(); err != nil {
		t.Fatalf("expected no error on close, got %v", err)
	}
}

func Test_CDCStreamer_ExecuteRollback(t *testing.T) {
	db, streamer, ch := mustCreateCDCStreamerDatabase(t)
	streamer.Reset(42)
	results, err := db.Execute(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1), (1)"},
		{Sql: "INSERT INTO foo VALUES(2)"},
	}}, false)
	if err != nil {
		t.Fatalf("error executing request: %v", err)
	}
	if len(results) != 2 || results[0].GetError() == "" || results[1].GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if len(ch) != 1 {
		t.Fatalf("expected one committed group, got %d", len(ch))
	}
	group := <-ch
	if group.Index != 42 || len(group.Events) != 1 || group.Events[0].NewRowId != 2 {
		t.Fatalf("unexpected committed group: %s", asJSON(group))
	}
	if got := asJSON(mustQuery(db, "SELECT id FROM foo")); got != `[{"columns":["id"],"types":["integer"],"values":[[2]]}]` {
		t.Fatalf("unexpected rows: %s", got)
	}
}

func Test_CDCStreamer_RequestRollback(t *testing.T) {
	db, streamer, ch := mustCreateCDCStreamerDatabase(t)
	streamer.Reset(42)
	results, err := db.Request(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1), (1)"},
		{Sql: "INSERT INTO foo VALUES(2)"},
	}}, false)
	if err != nil {
		t.Fatalf("error processing request: %v", err)
	}
	if len(results) != 2 || results[0].GetError() == "" || results[1].GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if len(ch) != 1 {
		t.Fatalf("expected one committed group, got %d", len(ch))
	}
	group := <-ch
	if group.Index != 42 || len(group.Events) != 1 || group.Events[0].NewRowId != 2 {
		t.Fatalf("unexpected committed group: %s", asJSON(group))
	}
}

func Test_CDCStreamer_TransactionRollback(t *testing.T) {
	db, streamer, ch := mustCreateCDCStreamerDatabase(t)
	streamer.Reset(42)
	results, err := db.Execute(&command.Request{Transaction: true, Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO foo VALUES(1)"},
	}}, false)
	if err != nil {
		t.Fatalf("error executing transaction: %v", err)
	}
	if len(results) != 2 || results[0].GetError() != "" || results[1].GetError() == "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if streamer.Len() != 0 || len(ch) != 0 {
		t.Fatalf("rolled-back transaction retained %d pending events and emitted %d groups", streamer.Len(), len(ch))
	}
	mustExecute(db, "INSERT INTO foo VALUES(2)")
	if len(ch) != 1 {
		t.Fatalf("expected one committed group, got %d", len(ch))
	}
	group := <-ch
	if group.Index != 42 || len(group.Events) != 1 || group.Events[0].NewRowId != 2 {
		t.Fatalf("unexpected committed group: %s", asJSON(group))
	}
}

func Test_CDCStreamer_ConflictFailRetainsChanges(t *testing.T) {
	db, streamer, ch := mustCreateCDCStreamerDatabase(t)
	streamer.Reset(42)
	results, err := db.ExecuteStringStmt("INSERT OR FAIL INTO foo VALUES(1), (1)")
	if err != nil {
		t.Fatalf("error executing request: %v", err)
	}
	if len(results) != 1 || results[0].GetError() == "" {
		t.Fatalf("expected constraint error, got %s", asJSON(results))
	}
	// FAIL preserves the first insert even though the statement returns an error.
	if len(ch) != 1 {
		t.Fatalf("expected one committed group, got %d", len(ch))
	}
	group := <-ch
	if group.Index != 42 || len(group.Events) != 1 || group.Events[0].NewRowId != 1 {
		t.Fatalf("unexpected committed group: %s", asJSON(group))
	}
	if got := asJSON(mustQuery(db, "SELECT id FROM foo")); got != `[{"columns":["id"],"types":["integer"],"values":[[1]]}]` {
		t.Fatalf("unexpected rows: %s", got)
	}
}

func mustCreateCDCStreamerDatabase(t *testing.T) (*DB, *CDCStreamer, chan *command.CDCIndexedEventGroup) {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "cdc.db"), false, true)
	if err != nil {
		t.Fatalf("error opening database: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	ch := make(chan *command.CDCIndexedEventGroup, 10)
	streamer, err := NewCDCStreamer(ch, db)
	if err != nil {
		t.Fatalf("error creating CDC streamer: %v", err)
	}
	if err := db.RegisterPreUpdateHook(streamer.PreupdateHook, nil, false); err != nil {
		t.Fatalf("error registering preupdate hook: %v", err)
	}
	if err := db.RegisterCommitHook(streamer.CommitHook); err != nil {
		t.Fatalf("error registering commit hook: %v", err)
	}
	if err := db.RegisterRollbackHook(streamer.RollbackHook); err != nil {
		t.Fatalf("error registering rollback hook: %v", err)
	}
	return db, streamer, ch
}

type mockColumnNamesProvider struct {
	columns map[string][]string
}

func (m *mockColumnNamesProvider) ColumnNames(table string) ([]string, error) {
	if cols, ok := m.columns[table]; ok {
		return cols, nil
	}
	return []string{}, nil
}
