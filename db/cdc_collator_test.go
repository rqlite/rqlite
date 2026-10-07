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
// provider and starts with no events collected.
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
	if c.Events() != nil {
		t.Fatalf("expected no events, got %d", len(c.Events()))
	}
}

// Test_CDCCollator_CommitOne verifies that a single event committed in one
// transaction is returned by Events with its column names resolved.
func Test_CDCCollator_CommitOne(t *testing.T) {
	np := &mockColumnNamesProvider{
		columns: map[string][]string{"test_table": {"id", "name", "value"}},
	}
	c := mustNewCDCCollator(t, np)

	c.Reset()
	change := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_INSERT,
		OldRowId: 100,
		NewRowId: 200,
	}
	if err := c.PreupdateHook(change); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(c.Events()) != 0 {
		t.Fatal("event reached the caller before commit")
	}

	if !c.CommitHook() {
		t.Fatal("commit hook must always allow the transaction to proceed")
	}
	if len(c.Events()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(c.Events()))
	}
	if !slices.Equal(c.Events()[0].ColumnNames, []string{"id", "name", "value"}) {
		t.Fatalf("expected column names [id name value], got %v", c.Events()[0].ColumnNames)
	}
	if !reflect.DeepEqual(change, c.Events()[0]) {
		t.Fatalf("collected event does not match: expected %v, got %v", change, c.Events()[0])
	}
}

// Test_CDCCollator_CommitTwo verifies that two events from one transaction
// are appended in the order they occurred.
func Test_CDCCollator_CommitTwo(t *testing.T) {
	np := &mockColumnNamesProvider{
		columns: map[string][]string{"test_table": {"id", "name", "value"}},
	}
	c := mustNewCDCCollator(t, np)

	c.Reset()
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
	if len(c.Events()) != 0 {
		t.Fatal("c.Events() reached the caller before commit")
	}

	c.CommitHook()
	if len(c.Events()) != 2 {
		t.Fatalf("expected 2 c.Events(), got %d", len(c.Events()))
	}
	if !reflect.DeepEqual(change1, c.Events()[0]) {
		t.Fatalf("first event does not match: expected %v, got %v", change1, c.Events()[0])
	}
	if !slices.Equal(c.Events()[0].ColumnNames, []string{"id", "name", "value"}) {
		t.Fatalf("expected column names [id name value], got %v", c.Events()[0].ColumnNames)
	}
	if !reflect.DeepEqual(change2, c.Events()[1]) {
		t.Fatalf("second event does not match: expected %v, got %v", change2, c.Events()[1])
	}
}

// Test_CDCCollator_MultipleCommits verifies that several transactions committed
// after one Reset all accumulate and are returned together by Events.
func Test_CDCCollator_MultipleCommits(t *testing.T) {
	np := &mockColumnNamesProvider{
		columns: map[string][]string{"foo": {"id"}},
	}
	c := mustNewCDCCollator(t, np)

	c.Reset()
	for i := int64(1); i <= 3; i++ {
		if err := c.PreupdateHook(&command.CDCEvent{Table: "foo", Op: command.CDCEvent_INSERT, NewRowId: i}); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		c.CommitHook()
	}

	if got := newRowIDs(c.Events()); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Fatalf("expected rows [1 2 3] in one slice, got %v", got)
	}
	for _, ev := range c.Events() {
		if !slices.Equal(ev.ColumnNames, []string{"id"}) {
			t.Fatalf("expected column names [id], got %v", ev.ColumnNames)
		}
	}
}

// Test_CDCCollator_CommitNoEvents verifies that a commit with nothing pending,
// as happens for schema changes, collects nothing.
func Test_CDCCollator_CommitNoEvents(t *testing.T) {
	c := mustNewCDCCollator(t, &mockColumnNamesProvider{})

	c.Reset()
	if !c.CommitHook() {
		t.Fatal("commit hook must always allow the transaction to proceed")
	}
	if c.Events() != nil {
		t.Fatalf("expected nil c.Events(), got %d c.Events()", len(c.Events()))
	}
}

// Test_CDCCollator_ResetThenPreupdate verifies that Reset discards events from
// an unfinished transaction, so that only changes made after the Reset are
// collected by a later commit.
func Test_CDCCollator_ResetThenPreupdate(t *testing.T) {
	np := &mockColumnNamesProvider{
		columns: map[string][]string{"test_table": {"id", "name", "value"}},
	}
	c := mustNewCDCCollator(t, np)

	c.Reset()
	change1 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_INSERT,
		OldRowId: 100,
		NewRowId: 200,
	}
	if err := c.PreupdateHook(change1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	c.Reset()
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

	events := c.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if !slices.Equal(events[0].ColumnNames, []string{"id", "name", "value"}) {
		t.Fatalf("expected column names [id name value], got %v", events[0].ColumnNames)
	}
	if !reflect.DeepEqual(change2, events[0]) {
		t.Fatalf("event does not match: expected %v, got %v", change2, events[0])
	}
}

// Test_CDCCollator_ResetDiscardsCollected verifies that Reset discards events
// already collected, and that a slice previously returned by Events is not
// modified by the Reset or by later commits.
func Test_CDCCollator_ResetDiscardsCollected(t *testing.T) {
	c := mustNewCDCCollator(t, &mockColumnNamesProvider{})

	c.Reset()
	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 1})
	c.CommitHook()
	events1 := c.Events()
	if got := newRowIDs(events1); !slices.Equal(got, []int64{1}) {
		t.Fatalf("expected rows [1], got %v", got)
	}

	c.Reset()
	if c.Events() != nil {
		t.Fatalf("expected nil events after reset, got %d events", len(c.Events()))
	}
	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 2})
	c.CommitHook()
	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 3})
	c.CommitHook()

	if got := newRowIDs(events1); !slices.Equal(got, []int64{1}) {
		t.Fatalf("previously returned slice was modified: rows %v", got)
	}
	if got := newRowIDs(c.Events()); !slices.Equal(got, []int64{2, 3}) {
		t.Fatalf("expected rows [2 3], got %v", got)
	}
}

// Test_CDCCollator_RollbackDiscardsPendingOnly verifies that a rollback drops
// only the events of the transaction in progress, leaving earlier commits in
// place and allowing later commits to continue appending.
func Test_CDCCollator_RollbackDiscardsPendingOnly(t *testing.T) {
	c := mustNewCDCCollator(t, &mockColumnNamesProvider{})

	c.Reset()

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 1})
	c.CommitHook()

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 2})
	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 3})
	c.RollbackHook()

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 4})
	c.CommitHook()

	if got := newRowIDs(c.Events()); !slices.Equal(got, []int64{1, 4}) {
		t.Fatalf("expected rows [1 4], got %v", got)
	}
}

// Test_CDCCollator_NoReset verifies that a collator which has never been
// reset collects committed events and discards rolled-back ones.
func Test_CDCCollator_NoReset(t *testing.T) {
	c := mustNewCDCCollator(t, &mockColumnNamesProvider{})

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 1})
	if !c.CommitHook() {
		t.Fatal("commit hook must always allow the transaction to proceed")
	}
	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 2})
	c.RollbackHook()

	if got := newRowIDs(c.Events()); !slices.Equal(got, []int64{1}) {
		t.Fatalf("expected rows [1], got %v", got)
	}
}

// Test_CDCCollator_ColumnNamesError verifies that a failure to resolve column
// names is recorded on the event, and that the event is still delivered.
func Test_CDCCollator_ColumnNamesError(t *testing.T) {
	c := mustNewCDCCollator(t, &errorColumnNamesProvider{err: errors.New("no such table")})

	c.Reset()
	c.PreupdateHook(&command.CDCEvent{Table: "missing", NewRowId: 1})
	c.CommitHook()

	if len(c.Events()) != 1 {
		t.Fatalf("expected 1 event, got %d", len(c.Events()))
	}
	ev := c.Events()[0]
	if ev.ColumnNames != nil {
		t.Fatalf("expected no column names, got %v", ev.ColumnNames)
	}
	if !strings.Contains(ev.Error, "missing") || !strings.Contains(ev.Error, "no such table") {
		t.Fatalf("unexpected event error: %q", ev.Error)
	}
}

// Test_CDCCollator_ColumnNamesPerCommit verifies that column names are
// resolved at each commit, so a schema change between two transactions
// collected together is reflected in the later events.
func Test_CDCCollator_ColumnNamesPerCommit(t *testing.T) {
	np := &mockColumnNamesProvider{
		columns: map[string][]string{"foo": {"id"}},
	}
	c := mustNewCDCCollator(t, np)

	c.Reset()

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 1})
	c.CommitHook()

	// Simulate ALTER TABLE foo ADD COLUMN name between the two transactions.
	np.columns["foo"] = []string{"id", "name"}

	c.PreupdateHook(&command.CDCEvent{Table: "foo", NewRowId: 2})
	c.CommitHook()

	if len(c.Events()) != 2 {
		t.Fatalf("expected 2 c.Events(), got %d", len(c.Events()))
	}
	if !slices.Equal(c.Events()[0].ColumnNames, []string{"id"}) {
		t.Fatalf("expected first event columns [id], got %v", c.Events()[0].ColumnNames)
	}
	if !slices.Equal(c.Events()[1].ColumnNames, []string{"id", "name"}) {
		t.Fatalf("expected second event columns [id name], got %v", c.Events()[1].ColumnNames)
	}
}

// Test_CDCCollator_MultiStatementRequest verifies, through a real database
// with the hooks registered, that two autocommit statements in one request
// are collected together.
func Test_CDCCollator_MultiStatementRequest(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	c.Reset()
	results, err := db.Execute(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO foo VALUES(2)"},
	}}, false)
	if err != nil {
		t.Fatalf("error executing request: %v", err)
	}
	for _, r := range results {
		if r.GetE().GetError() != "" {
			t.Fatalf("unexpected statement error: %s", r.GetE().GetError())
		}
	}

	if got := newRowIDs(c.Events()); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("expected rows [1 2] in one slice, got %v", got)
	}
	for _, ev := range c.Events() {
		if ev.Op != command.CDCEvent_INSERT {
			t.Fatalf("expected INSERT, got %s", ev.Op)
		}
		if !slices.Equal(ev.ColumnNames, []string{"id"}) {
			t.Fatalf("expected column names [id], got %v", ev.ColumnNames)
		}
	}
}

// Test_CDCCollator_Transaction verifies that a request executed as a single
// transaction commits once and delivers all of its rows together.
func Test_CDCCollator_Transaction(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	c.Reset()
	results, err := db.Execute(&command.Request{Transaction: true, Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO foo VALUES(2)"},
	}}, false)
	if err != nil {
		t.Fatalf("error executing transaction: %v", err)
	}
	for _, r := range results {
		if r.GetE().GetError() != "" {
			t.Fatalf("unexpected statement error: %s", r.GetE().GetError())
		}
	}
	if got := newRowIDs(c.Events()); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("expected rows [1 2], got %v", got)
	}
}

// Test_CDCCollator_SuccessiveRequests verifies that each Reset starts a
// fresh collection and that events returned for an earlier request are not
// modified by later requests.
func Test_CDCCollator_SuccessiveRequests(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	c.Reset()
	mustExecute(db, "INSERT INTO foo VALUES(1)")
	events1 := c.Events()

	c.Reset()
	mustExecute(db, "INSERT INTO foo VALUES(2)")
	events2 := c.Events()

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

	c.Reset()
	mustExecute(db, "CREATE TABLE bar (id INTEGER PRIMARY KEY)")

	if len(c.Events()) != 0 {
		t.Fatalf("expected no c.Events() for schema change, got %d", len(c.Events()))
	}
}

// Test_CDCCollator_WritesBetweenRequests verifies, through a real database,
// that writes made after the caller has retrieved its events do not modify
// the slice it holds, and are discarded by the next Reset.
func Test_CDCCollator_WritesBetweenRequests(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	c.Reset()
	mustExecute(db, "INSERT INTO foo VALUES(1)")
	events1 := c.Events()

	// Writes before the next Reset, including one that rolls back.
	mustExecute(db, "INSERT INTO foo VALUES(2)")
	if _, err := db.ExecuteStringStmt("INSERT INTO foo VALUES(3), (3)"); err != nil {
		t.Fatalf("error executing statement: %v", err)
	}

	c.Reset()
	mustExecute(db, "INSERT INTO foo VALUES(4)")
	events2 := c.Events()

	if !slices.Equal(newRowIDs(events1), []int64{1}) {
		t.Fatalf("later writes modified a handed-on slice: %s", asJSON(events1))
	}
	if !slices.Equal(newRowIDs(events2), []int64{4}) {
		t.Fatalf("unexpected events after reset: %s", asJSON(events2))
	}
}

// Test_CDCCollator_ExecuteRollback verifies that an autocommit statement which
// fails and rolls back contributes no events, while a following statement in
// the same request is still collected.
func Test_CDCCollator_ExecuteRollback(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	c.Reset()
	results, err := db.Execute(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1), (1)"}, // Fails due to UNIQUE constraint on PK.
		{Sql: "INSERT INTO foo VALUES(2)"},      // Succeeds.
	}}, false)
	if err != nil {
		t.Fatalf("error executing request: %v", err)
	}
	if len(results) != 2 || !strings.Contains(results[0].GetE().GetError(), "UNIQUE") || results[1].GetE().GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}

	if !slices.Equal(newRowIDs(c.Events()), []int64{2}) {
		t.Fatalf("unexpected committed c.Events(): %s", asJSON(c.Events()))
	}
	if got := asJSON(mustQuery(db, "SELECT id FROM foo")); got != `[{"columns":["id"],"types":["integer"],"values":[[2]]}]` {
		t.Fatalf("unexpected rows: %s", got)
	}
}

// Test_CDCCollator_RequestRollback verifies the same rollback behavior through
// the unified Request path.
func Test_CDCCollator_RequestRollback(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	c.Reset()
	results, err := db.Request(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1), (1)"},
		{Sql: "INSERT INTO foo VALUES(2)"},
	}}, false)
	if err != nil {
		t.Fatalf("error processing request: %v", err)
	}
	if len(results) != 2 || !strings.Contains(results[0].GetE().GetError(), "UNIQUE") || results[1].GetE().GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if !slices.Equal(newRowIDs(c.Events()), []int64{2}) {
		t.Fatalf("unexpected committed c.Events(): %s", asJSON(c.Events()))
	}
}

// Test_CDCCollator_RequestPartialSuccess verifies that when a statement in the
// middle of a request fails, the rows committed before and after it are both
// collected.
func Test_CDCCollator_RequestPartialSuccess(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	c.Reset()
	results, err := db.Request(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO missing VALUES(2)"},
		{Sql: "INSERT INTO foo VALUES(3)"},
	}}, false)
	if err != nil {
		t.Fatalf("error processing request: %v", err)
	}
	if len(results) != 3 || results[0].GetE().GetError() != "" || results[1].GetE().GetError() == "" || results[2].GetE().GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if !slices.Equal(newRowIDs(c.Events()), []int64{1, 3}) {
		t.Fatalf("unexpected c.Events() for partially successful request: %s", asJSON(c.Events()))
	}
}

// Test_CDCCollator_CommitsAroundRollback verifies that a rolled-back
// autocommit statement between two successful ones leaves exactly the two
// committed rows collected, matching the database contents.
func Test_CDCCollator_CommitsAroundRollback(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	c.Reset()
	results, err := db.Execute(&command.Request{Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO foo VALUES(2), (2)"},
		{Sql: "INSERT INTO foo VALUES(3)"},
	}}, false)
	if err != nil {
		t.Fatalf("error executing request: %v", err)
	}
	if len(results) != 3 || results[0].GetE().GetError() != "" || !strings.Contains(results[1].GetE().GetError(), "UNIQUE") || results[2].GetE().GetError() != "" {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if !slices.Equal(newRowIDs(c.Events()), []int64{1, 3}) {
		t.Fatalf("expected commits before and after rollback, got %s", asJSON(c.Events()))
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

	c.Reset()
	results, err := db.Execute(&command.Request{Transaction: true, Statements: []*command.Statement{
		{Sql: "INSERT INTO foo VALUES(1)"},
		{Sql: "INSERT INTO foo VALUES(1)"},
	}}, false)
	if err != nil {
		t.Fatalf("error executing transaction: %v", err)
	}
	if len(results) != 2 || results[0].GetE().GetError() != "" || !strings.Contains(results[1].GetE().GetError(), "UNIQUE") {
		t.Fatalf("unexpected results: %s", asJSON(results))
	}
	if c.Events() != nil {
		t.Fatalf("rolled-back transaction left %d c.Events() collected", len(c.Events()))
	}

	mustExecute(db, "INSERT INTO foo VALUES(2)")
	if !slices.Equal(newRowIDs(c.Events()), []int64{2}) {
		t.Fatalf("unexpected committed c.Events(): %s", asJSON(c.Events()))
	}
}

// Test_CDCCollator_ConflictFailRetainsChanges verifies that a statement using
// FAIL conflict resolution, which returns an error yet commits the rows
// changed before the conflict, has those rows collected.
func Test_CDCCollator_ConflictFailRetainsChanges(t *testing.T) {
	db, c := mustCreateCDCCollatorDatabase(t)

	c.Reset()
	results, err := db.ExecuteStringStmt("INSERT OR FAIL INTO foo VALUES(1), (1)")
	if err != nil {
		t.Fatalf("error executing statement: %v", err)
	}
	if len(results) != 1 || results[0].GetE().GetError() == "" {
		t.Fatalf("expected constraint error, got %s", asJSON(results))
	}
	if !slices.Equal(newRowIDs(c.Events()), []int64{1}) {
		t.Fatalf("unexpected committed c.Events(): %s", asJSON(c.Events()))
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

	// The collator resolves column names through whichever database is
	// currently open, since the database is reopened to install the hooks.
	p := &lazyColumnNamesProvider{}
	c := mustNewCDCCollator(t, p)
	db = mustReopenWithHooks(t, db, &DriverConfig{
		PreUpdateHook: c.PreupdateHook,
		CommitHook:    c.CommitHook,
		RollbackHook:  c.RollbackHook,
	})
	p.db = db
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

type mockColumnNamesProvider struct {
	columns map[string][]string
}

func (m *mockColumnNamesProvider) ColumnNames(table string) ([]string, error) {
	if cols, ok := m.columns[table]; ok {
		return cols, nil
	}
	return []string{}, nil
}

// lazyColumnNamesProvider resolves column names through a database which may
// be set after the provider is created.
type lazyColumnNamesProvider struct {
	db *DB
}

func (p *lazyColumnNamesProvider) ColumnNames(table string) ([]string, error) {
	return p.db.ColumnNames(table)
}
