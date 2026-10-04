package db

import (
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

// Test_CDCCollator_New verifies that a collator requires a column-name
// provider and starts detached with nothing pending.
func Test_CDCCollator_New(t *testing.T) {
	if _, err := NewCDCCollator(nil); err == nil {
		t.Fatal("expected error for nil ColumnsNameProvider")
	}
	c, err := NewCDCCollator(&mockColumnNamesProvider{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if c == nil {
		t.Fatal("expected collator to be created, got nil")
	}
	if c.Len() != 0 {
		t.Fatalf("expected no pending events, got %d", c.Len())
	}
}

// Test_CDCCollator_CommitOne verifies that a single event committed in one
// transaction lands in the caller's slice with its column names resolved.
func Test_CDCCollator_CommitOne(t *testing.T) {
	np := &mockColumnNamesProvider{
		columns: map[string][]string{"test_table": {"id", "name", "value"}},
	}
	c := mustNewCDCCollator(t, np)

	var events []*command.CDCEvent
	c.Reset(&events)
	change := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_INSERT,
		OldRowId: 100,
		NewRowId: 200,
	}
	if err := c.PreupdateHook(change); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if c.Len() != 1 {
		t.Fatalf("expected 1 pending event, got %d", c.Len())
	}
	if len(events) != 0 {
		t.Fatal("event reached the caller before commit")
	}

	if !c.CommitHook() {
		t.Fatal("commit hook must always allow the transaction to proceed")
	}
	if c.Len() != 0 {
		t.Fatalf("expected no pending events after commit, got %d", c.Len())
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if !slices.Equal(events[0].ColumnNames, []string{"id", "name", "value"}) {
		t.Fatalf("expected column names [id name value], got %v", events[0].ColumnNames)
	}
	if !reflect.DeepEqual(change, events[0]) {
		t.Fatalf("collected event does not match: expected %v, got %v", change, events[0])
	}
}

// Test_CDCCollator_CommitTwo verifies that two events from one transaction
// are appended in the order they occurred.
func Test_CDCCollator_CommitTwo(t *testing.T) {
	np := &mockColumnNamesProvider{
		columns: map[string][]string{"test_table": {"id", "name", "value"}},
	}
	c := mustNewCDCCollator(t, np)

	var events []*command.CDCEvent
	c.Reset(&events)
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
	if err := c.PreupdateHook(change1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := c.PreupdateHook(change2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if c.Len() != 2 {
		t.Fatalf("expected 2 pending events, got %d", c.Len())
	}

	c.CommitHook()
	if c.Len() != 0 {
		t.Fatalf("expected no pending events after commit, got %d", c.Len())
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if !reflect.DeepEqual(change1, events[0]) {
		t.Fatalf("first event does not match: expected %v, got %v", change1, events[0])
	}
	if !slices.Equal(events[0].ColumnNames, []string{"id", "name", "value"}) {
		t.Fatalf("expected column names [id name value], got %v", events[0].ColumnNames)
	}
	if !reflect.DeepEqual(change2, events[1]) {
		t.Fatalf("second event does not match: expected %v, got %v", change2, events[1])
	}
}

// Test_CDCCollator_MultipleCommits verifies that several transactions committed
// after one Reset all accumulate in the same caller-owned slice.
func Test_CDCCollator_MultipleCommits(t *testing.T) {
	np := &mockColumnNamesProvider{
		columns: map[string][]string{"foo": {"id"}},
	}
	c := mustNewCDCCollator(t, np)

	var events []*command.CDCEvent
	c.Reset(&events)
	for i := int64(1); i <= 3; i++ {
		if err := c.PreupdateHook(&command.CDCEvent{Table: "foo", Op: command.CDCEvent_INSERT, NewRowId: i}); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		c.CommitHook()
	}

	if got := newRowIDs(events); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Fatalf("expected rows [1 2 3] in one slice, got %v", got)
	}
	for _, ev := range events {
		if !slices.Equal(ev.ColumnNames, []string{"id"}) {
			t.Fatalf("expected column names [id], got %v", ev.ColumnNames)
		}
	}
}

// Test_CDCCollator_CommitNoEvents verifies that a commit with nothing pending,
// as happens for schema changes, leaves the caller's slice untouched.
func Test_CDCCollator_CommitNoEvents(t *testing.T) {
	c := mustNewCDCCollator(t, &mockColumnNamesProvider{})

	var events []*command.CDCEvent
	c.Reset(&events)
	if !c.CommitHook() {
		t.Fatal("commit hook must always allow the transaction to proceed")
	}
	if events != nil {
		t.Fatalf("expected slice to remain nil, got %d events", len(events))
	}
}

// Test_CDCCollator_ResetThenPreupdate verifies that Reset discards events from
// an unfinished transaction, redirects later commits to the new slice, and
// never modifies the previous slice.
func Test_CDCCollator_ResetThenPreupdate(t *testing.T) {
	np := &mockColumnNamesProvider{
		columns: map[string][]string{"test_table": {"id", "name", "value"}},
	}
	c := mustNewCDCCollator(t, np)

	var events1 []*command.CDCEvent
	c.Reset(&events1)
	change1 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_INSERT,
		OldRowId: 100,
		NewRowId: 200,
	}
	if err := c.PreupdateHook(change1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if c.Len() != 1 {
		t.Fatalf("expected 1 pending event, got %d", c.Len())
	}

	var events2 []*command.CDCEvent
	c.Reset(&events2)
	if c.Len() != 0 {
		t.Fatalf("expected no pending events after reset, got %d", c.Len())
	}
	change2 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_UPDATE,
		OldRowId: 300,
		NewRowId: 400,
	}
	if err := c.PreupdateHook(change2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	c.CommitHook()
	if c.Len() != 0 {
		t.Fatalf("expected no pending events after commit, got %d", c.Len())
	}

	if len(events1) != 0 {
		t.Fatalf("previous slice was modified after reset: %s", asJSON(events1))
	}
	if len(events2) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events2))
	}
	if !slices.Equal(events2[0].ColumnNames, []string{"id", "name", "value"}) {
		t.Fatalf("expected column names [id name value], got %v", events2[0].ColumnNames)
	}
	if !reflect.DeepEqual(change2, events2[0]) {
		t.Fatalf("event does not match: expected %v, got %v", change2, events2[0])
	}
}

// Test_CDCCollator_ResetAppendsOnly verifies that Reset never clears the slice
// the caller passes in, so events already present are preserved.
func Test_CDCCollator_ResetAppendsOnly(t *testing.T) {
	c := mustNewCDCCollator(t, &mockColumnNamesProvider{})

	events := []*command.CDCEvent{{Table: "foo", NewRowId: 99}}
	c.Reset(&events)
	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 100})
	c.CommitHook()

	if got := newRowIDs(events); !slices.Equal(got, []int64{99, 100}) {
		t.Fatalf("expected rows [99 100], got %v", got)
	}
}

// Test_CDCCollator_RollbackDiscardsPendingOnly verifies that a rollback drops
// only the events of the transaction in progress, leaving earlier commits in
// place and allowing later commits to continue appending.
func Test_CDCCollator_RollbackDiscardsPendingOnly(t *testing.T) {
	c := mustNewCDCCollator(t, &mockColumnNamesProvider{})

	var events []*command.CDCEvent
	c.Reset(&events)

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 1})
	c.CommitHook()

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 2})
	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 3})
	if c.Len() != 2 {
		t.Fatalf("expected 2 pending events, got %d", c.Len())
	}
	c.RollbackHook()
	if c.Len() != 0 {
		t.Fatalf("expected no pending events after rollback, got %d", c.Len())
	}

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 4})
	c.CommitHook()

	if got := newRowIDs(events); !slices.Equal(got, []int64{1, 4}) {
		t.Fatalf("expected rows [1 4], got %v", got)
	}
}

// Test_CDCCollator_Detached verifies that a collator with no destination,
// whether never reset or explicitly reset with nil, discards committed events
// without failing the transaction and without touching any previous slice.
func Test_CDCCollator_Detached(t *testing.T) {
	c := mustNewCDCCollator(t, &mockColumnNamesProvider{})

	// Never reset.
	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 1})
	if !c.CommitHook() {
		t.Fatal("commit hook must always allow the transaction to proceed")
	}
	if c.Len() != 0 {
		t.Fatalf("expected pending events to be discarded, got %d", c.Len())
	}

	// Attached, then detached after the caller has taken the events.
	var events []*command.CDCEvent
	c.Reset(&events)
	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 2})
	c.CommitHook()
	c.Reset(nil)

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 3})
	c.CommitHook()
	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 4})
	c.RollbackHook()

	if got := newRowIDs(events); !slices.Equal(got, []int64{2}) {
		t.Fatalf("detached collator modified a handed-on slice: rows %v", got)
	}
	if c.Len() != 0 {
		t.Fatalf("expected no pending events, got %d", c.Len())
	}
}

// Test_CDCCollator_ColumnNamesError verifies that a failure to resolve column
// names is recorded on the event, and that the event is still delivered.
func Test_CDCCollator_ColumnNamesError(t *testing.T) {
	c := mustNewCDCCollator(t, &errorColumnNamesProvider{err: errors.New("no such table")})

	var events []*command.CDCEvent
	c.Reset(&events)
	c.PreupdateHook(&command.CDCEvent{Table: "missing", NewRowId: 1})
	c.CommitHook()

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	ev := events[0]
	if ev.ColumnNames != nil {
		t.Fatalf("expected no column names, got %v", ev.ColumnNames)
	}
	if !strings.Contains(ev.Error, "missing") || !strings.Contains(ev.Error, "no such table") {
		t.Fatalf("unexpected event error: %q", ev.Error)
	}
}

// Test_CDCCollator_ColumnNamesPerCommit verifies that column names are
// resolved at each commit, so a schema change between two transactions
// collected into the same slice is reflected in the later events.
func Test_CDCCollator_ColumnNamesPerCommit(t *testing.T) {
	np := &mockColumnNamesProvider{
		columns: map[string][]string{"foo": {"id"}},
	}
	c := mustNewCDCCollator(t, np)

	var events []*command.CDCEvent
	c.Reset(&events)

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 1})
	c.CommitHook()

	// Simulate ALTER TABLE foo ADD COLUMN name between the two transactions.
	np.columns["foo"] = []string{"id", "name"}

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 2})
	c.CommitHook()

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if !slices.Equal(events[0].ColumnNames, []string{"id"}) {
		t.Fatalf("expected first event columns [id], got %v", events[0].ColumnNames)
	}
	if !slices.Equal(events[1].ColumnNames, []string{"id", "name"}) {
		t.Fatalf("expected second event columns [id name], got %v", events[1].ColumnNames)
	}
}

// Test_CDCCollator_MultiStatementRequest verifies, through a real database
// with the hooks registered, that two autocommit statements in one request
// are collected together into the caller's slice. This is the production path
// on which the original bug appeared.
func Test_CDCCollator_MultiStatementRequest(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events []*command.CDCEvent
	c.Reset(&events)
	results, err := db.Execute(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO foo VALUES(2)"},
	}}, false)
	if err != nil {
		t.Fatalf("error executing request: %v", err)
	}
	for _, r := range results {
		if r.GetError() != "" {
			t.Fatalf("unexpected statement error: %s", r.GetError())
		}
	}

	if got := newRowIDs(events); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("expected rows [1 2] in one slice, got %v", got)
	}
	for _, ev := range events {
		if ev.Op != command.CDCEvent_INSERT {
			t.Fatalf("expected INSERT, got %s", ev.Op)
		}
		if !slices.Equal(ev.ColumnNames, []string{"id"}) {
			t.Fatalf("expected column names [id], got %v", ev.ColumnNames)
		}
	}
	if c.Len() != 0 {
		t.Fatalf("expected no pending events, got %d", c.Len())
	}
}

// Test_CDCCollator_Transaction verifies that a request executed as a single
// transaction commits once and delivers all of its rows together.
func Test_CDCCollator_Transaction(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events []*command.CDCEvent
	c.Reset(&events)
	results, err := db.Execute(&command.Request{Transaction: true, Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO foo VALUES(2)"},
	}}, false)
	if err != nil {
		t.Fatalf("error executing transaction: %v", err)
	}
	for _, r := range results {
		if r.GetError() != "" {
			t.Fatalf("unexpected statement error: %s", r.GetError())
		}
	}
	if got := newRowIDs(events); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("expected rows [1 2], got %v", got)
	}
}

// Test_CDCCollator_SuccessiveRequests verifies that each Reset starts
// collecting into a fresh slice and that no earlier slice is modified by
// later requests.
func Test_CDCCollator_SuccessiveRequests(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events1 []*command.CDCEvent
	c.Reset(&events1)
	mustExecute(db, "INSERT INTO foo VALUES(1)")

	var events2 []*command.CDCEvent
	c.Reset(&events2)
	mustExecute(db, "INSERT INTO foo VALUES(2)")

	if !slices.Equal(newRowIDs(events1), []int64{1}) {
		t.Fatalf("unexpected first slice: %s", asJSON(events1))
	}
	if !slices.Equal(newRowIDs(events2), []int64{2}) {
		t.Fatalf("unexpected second slice: %s", asJSON(events2))
	}
}

// Test_CDCCollator_SchemaChangeNoEvents verifies that a schema change commits
// without producing any events.
func Test_CDCCollator_SchemaChangeNoEvents(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events []*command.CDCEvent
	c.Reset(&events)
	mustExecute(db, "CREATE TABLE bar (id INTEGER PRIMARY KEY)")

	if len(events) != 0 {
		t.Fatalf("expected no events for schema change, got %d", len(events))
	}
}

// Test_CDCCollator_DetachedDatabase verifies, through a real database, that a
// detached collator records nothing while writes continue, and that a later
// Reset resumes collection.
func Test_CDCCollator_DetachedDatabase(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events1 []*command.CDCEvent
	c.Reset(&events1)
	mustExecute(db, "INSERT INTO foo VALUES(1)")
	c.Reset(nil)

	// Writes while detached, including one that rolls back.
	mustExecute(db, "INSERT INTO foo VALUES(2)")
	if _, err := db.ExecuteStringStmt("INSERT INTO foo VALUES(3), (3)"); err != nil {
		t.Fatalf("error executing statement: %v", err)
	}

	var events2 []*command.CDCEvent
	c.Reset(&events2)
	mustExecute(db, "INSERT INTO foo VALUES(4)")

	if !slices.Equal(newRowIDs(events1), []int64{1}) {
		t.Fatalf("detached collator modified a handed-on slice: %s", asJSON(events1))
	}
	if !slices.Equal(newRowIDs(events2), []int64{4}) {
		t.Fatalf("unexpected events after reattaching: %s", asJSON(events2))
	}
}

// Test_CDCCollator_ExecuteRollback verifies that an autocommit statement which
// fails and rolls back contributes no events, while a following statement in
// the same request is still collected.
func Test_CDCCollator_ExecuteRollback(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events []*command.CDCEvent
	c.Reset(&events)
	results, err := db.Execute(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1), (1)"}, // Fails due to UNIQUE constraint on PK.
		{Sql: "INSERT INTO foo VALUES(2)"},      // Succeeds.
	}}, false)
	if err != nil {
		t.Fatalf("error executing request: %v", err)
	}
	if len(results) != 2 || !strings.Contains(results[0].GetError(), "UNIQUE") || results[1].GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}

	if !slices.Equal(newRowIDs(events), []int64{2}) {
		t.Fatalf("unexpected committed events: %s", asJSON(events))
	}
	if got := asJSON(mustQuery(db, "SELECT id FROM foo")); got != `[{"columns":["id"],"types":["integer"],"values":[[2]]}]` {
		t.Fatalf("unexpected rows: %s", got)
	}
}

// Test_CDCCollator_RequestRollback verifies the same rollback behavior through
// the unified Request path.
func Test_CDCCollator_RequestRollback(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events []*command.CDCEvent
	c.Reset(&events)
	results, err := db.Request(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1), (1)"},
		{Sql: "INSERT INTO foo VALUES(2)"},
	}}, false)
	if err != nil {
		t.Fatalf("error processing request: %v", err)
	}
	if len(results) != 2 || !strings.Contains(results[0].GetError(), "UNIQUE") || results[1].GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if !slices.Equal(newRowIDs(events), []int64{2}) {
		t.Fatalf("unexpected committed events: %s", asJSON(events))
	}
}

// Test_CDCCollator_RequestPartialSuccess verifies that when a statement in the
// middle of a request fails, the rows committed before and after it are both
// collected.
func Test_CDCCollator_RequestPartialSuccess(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events []*command.CDCEvent
	c.Reset(&events)
	results, err := db.Request(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO missing VALUES(2)"},
		{Sql: "INSERT INTO foo VALUES(3)"},
	}}, false)
	if err != nil {
		t.Fatalf("error processing request: %v", err)
	}
	if len(results) != 3 || results[0].GetError() != "" || results[1].GetError() == "" || results[2].GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if !slices.Equal(newRowIDs(events), []int64{1, 3}) {
		t.Fatalf("unexpected events for partially successful request: %s", asJSON(events))
	}
}

// Test_CDCCollator_CommitsAroundRollback verifies that a rolled-back
// autocommit statement between two successful ones leaves exactly the two
// committed rows collected, matching the database contents.
func Test_CDCCollator_CommitsAroundRollback(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events []*command.CDCEvent
	c.Reset(&events)
	results, err := db.Execute(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO foo VALUES(2), (2)"},
		{Sql: "INSERT INTO foo VALUES(3)"},
	}}, false)
	if err != nil {
		t.Fatalf("error executing request: %v", err)
	}
	if len(results) != 3 || results[0].GetError() != "" || !strings.Contains(results[1].GetError(), "UNIQUE") || results[2].GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if !slices.Equal(newRowIDs(events), []int64{1, 3}) {
		t.Fatalf("expected commits before and after rollback, got %s", asJSON(events))
	}
	if got := asJSON(mustQuery(db, "SELECT id FROM foo ORDER BY id")); got != `[{"columns":["id"],"types":["integer"],"values":[[1],[3]]}]` {
		t.Fatalf("unexpected committed rows: %s", got)
	}
}

// Test_CDCCollator_TransactionRollback verifies that when an explicit
// transaction is rolled back, none of its rows are collected and nothing is
// left pending, while a subsequent commit is collected normally.
func Test_CDCCollator_TransactionRollback(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events []*command.CDCEvent
	c.Reset(&events)
	results, err := db.Execute(&command.Request{Transaction: true, Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO foo VALUES(1)"},
	}}, false)
	if err != nil {
		t.Fatalf("error executing transaction: %v", err)
	}
	if len(results) != 2 || results[0].GetError() != "" || !strings.Contains(results[1].GetError(), "UNIQUE") {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if c.Len() != 0 || len(events) != 0 {
		t.Fatalf("rolled-back transaction left %d pending events and %d collected", c.Len(), len(events))
	}

	mustExecute(db, "INSERT INTO foo VALUES(2)")
	if !slices.Equal(newRowIDs(events), []int64{2}) {
		t.Fatalf("unexpected committed events: %s", asJSON(events))
	}
}

// Test_CDCCollator_ConflictFailRetainsChanges verifies that a statement using
// FAIL conflict resolution, which returns an error yet commits the rows
// changed before the conflict, has those rows collected.
func Test_CDCCollator_ConflictFailRetainsChanges(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	var events []*command.CDCEvent
	c.Reset(&events)
	results, err := db.ExecuteStringStmt("INSERT OR FAIL INTO foo VALUES(1), (1)")
	if err != nil {
		t.Fatalf("error executing statement: %v", err)
	}
	if len(results) != 1 || results[0].GetError() == "" {
		t.Fatalf("expected constraint error, got %s", asJSON(results))
	}
	if !slices.Equal(newRowIDs(events), []int64{1}) {
		t.Fatalf("unexpected committed events: %s", asJSON(events))
	}
	if got := asJSON(mustQuery(db, "SELECT id FROM foo")); got != `[{"columns":["id"],"types":["integer"],"values":[[1]]}]` {
		t.Fatalf("unexpected rows: %s", got)
	}
}

// mustNewCDCCollator returns a collator or fails the test.
func mustNewCDCCollator(t *testing.T, np ColumnsNameProvider) *CDCCollator {
	t.Helper()
	c, err := NewCDCCollator(np)
	if err != nil {
		t.Fatalf("error creating CDCCollator: %v", err)
	}
	return c
}

// mustCreateCDCCollatorDatabase opens a WAL-mode database containing table
// foo, and registers a collator's hooks on it.
func mustCreateCDCCollatorDatabase(t *testing.T) (*DB, *CDCCollator) {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "cdc.db"), false, true)
	if err != nil {
		t.Fatalf("error opening database: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")

	c := mustNewCDCCollator(t, db)
	if err := db.RegisterPreUpdateHook(c.PreupdateHook, nil, false); err != nil {
		t.Fatalf("error registering preupdate hook: %v", err)
	}
	if err := db.RegisterCommitHook(c.CommitHook); err != nil {
		t.Fatalf("error registering commit hook: %v", err)
	}
	if err := db.RegisterRollbackHook(c.RollbackHook); err != nil {
		t.Fatalf("error registering rollback hook: %v", err)
	}
	return db, c
}

// newRowIDs returns the new row IDs of the given events, in order.
func newRowIDs(events []*command.CDCEvent) []int64 {
	ids := make([]int64, 0, len(events))
	for _, ev := range events {
		ids = append(ids, ev.NewRowId)
	}
	return ids
}

// errorColumnNamesProvider always fails to resolve column names.
type errorColumnNamesProvider struct {
	err error
}

func (p *errorColumnNamesProvider) ColumnNames(table string) ([]string, error) {
	return nil, p.err
}
