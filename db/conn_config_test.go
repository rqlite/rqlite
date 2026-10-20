package db

import (
	"context"
	"database/sql"
	"expvar"
	"regexp"
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db/querylog"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// Test_Open_WALRetainedOnClose tests that with the default connection
// policy the WAL is left in place when the database is closed.
func Test_Open_WALRetainedOnClose(t *testing.T) {
	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("Open failed: %s", err.Error())
	}
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	q, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	if !fsutil.FileExists(db.WALPath()) {
		t.Fatalf("WAL file not created")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %s", err.Error())
	}
	if !fsutil.FileExists(db.WALPath()) {
		t.Fatalf("WAL file removed on close")
	}

	// Now, delete the WAL file, and re-open the database. The SELECT should
	// fail with "no table", proving the WAL was not checkpointed.
	if err := fsutil.Remove(db.WALPath()); err != nil {
		t.Fatalf("Failed to remove WAL file: %s", err.Error())
	}
	db, err = Open(path, false, true)
	if err != nil {
		t.Fatalf("Open failed: %s", err.Error())
	}

	q, err = db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query empty table: %s", err.Error())
	}
	if exp, got := `[{"error":"no such table: foo","error_v2":{"message":"no such table: foo","sqlite":{"code":1,"extended_code":1}}}]`, asJSON(q); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %s", err.Error())
	}
}

// Test_Open_CheckpointOnClose tests that a connection policy which enables
// checkpoint on close removes the WAL when the database is closed.
func Test_Open_CheckpointOnClose(t *testing.T) {
	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithConfig(path, NewConfig(ConnConfig{ChkOnClose: CnkOnCloseModeEnabled}, true))
	if err != nil {
		t.Fatalf("OpenWithConfig failed: %s", err.Error())
	}
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	if !fsutil.FileExists(db.WALPath()) {
		t.Fatalf("WAL file not created")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %s", err.Error())
	}
	if fsutil.FileExists(db.WALPath()) {
		t.Fatalf("WAL file not removed on close")
	}
}

// Test_ConnConfig_AutoCheckpointDisabled tests that every connection opened
// with every policy has automatic checkpointing disabled. rqlite must have
// full control over checkpointing, and database/sql is free to open a new
// connection at any time.
func Test_ConnConfig_AutoCheckpointDisabled(t *testing.T) {
	for name, cfg := range map[string]ConnConfig{
		"default":    {},
		"checkpoint": {ChkOnClose: CnkOnCloseModeEnabled},
		"foreignkey": {ForeignKeys: true},
	} {
		path := mustTempPath()
		defer fsutil.RemoveAll(path)
		for _, readOnly := range []bool{ModeReadWrite, ModeReadOnly} {
			db := sql.OpenDB(cfg.connector(MakeDSN(path, readOnly, false, true)))
			defer db.Close()

			// Hold each connection open so the pool must create a new one each time.
			for i := 0; i < 3; i++ {
				conn, err := db.Conn(context.Background())
				if err != nil {
					t.Fatalf("policy %s: failed to get connection: %s", name, err)
				}
				defer conn.Close()
				var n int
				if err := conn.QueryRowContext(context.Background(), "PRAGMA wal_autocheckpoint").Scan(&n); err != nil {
					t.Fatalf("policy %s: failed to read autocheckpoint setting: %s", name, err)
				}
				if n != 0 {
					t.Errorf("policy %s, read-only %v, connection %d: autocheckpoint is %d, want 0", name, readOnly, i, n)
				}
			}
		}
	}
}

// Verifies that a ConnConfig with a QueryLogger does not interfere with
// normal database operations. Log output capture is tested in db/querylog.
func Test_ConnConfig_QueryLogOnly(t *testing.T) {
	ql := querylog.New(querylog.DefaultConfig())

	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithConfig(path, NewConfig(ConnConfig{QueryLogger: ql}, true))
	if err != nil {
		t.Fatalf("OpenWithConfig failed: %s", err)
	}
	defer db.Close()

	mustExecute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	mustExecute(db, "INSERT INTO t VALUES (1, 'hello')")

	rows, err := db.QueryStringStmt("SELECT val FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %s", err)
	}
	if len(rows) != 1 || len(rows[0].Values) != 1 {
		t.Fatalf("expected 1 row from SELECT, got unexpected result")
	}
}

// Verifies that a ConnConfig with nil
// QueryLogger opens and operates normally without tracing.
func Test_ConnConfig_NoQueryLog(t *testing.T) {
	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithConfig(path, NewConfig(ConnConfig{QueryLogger: nil}, true))
	if err != nil {
		t.Fatalf("OpenWithConfig failed: %s", err)
	}
	defer db.Close()

	mustExecute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
}

// Test_ConnConfig_NoHooks verifies that a policy with no hooks installs
// none: writing to a database opened with it fires nothing.
func Test_ConnConfig_NoHooks(t *testing.T) {
	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("Open failed: %s", err)
	}
	defer db.Close()

	before := hookStats()
	mustExecute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	mustExecute(db, "INSERT INTO t VALUES (1)")
	mustExecute(db, "BEGIN; INSERT INTO t VALUES (2); ROLLBACK")
	if after := hookStats(); after != before {
		t.Fatalf("hooks fired with no hooks configured: before %v, after %v", before, after)
	}
}

// Test_ConnConfig_Hooks verifies that hooks set in the policy are installed,
// and are called with converted event data.
func Test_ConnConfig_Hooks(t *testing.T) {
	var preupdates, updates, commits, rollbacks int
	var lastPreupdate *proto.CDCEvent
	var lastUpdate *proto.UpdateHookEvent
	cfg := NewConfig(ConnConfig{
		PreUpdateHook: func(ev *proto.CDCEvent) error {
			preupdates++
			lastPreupdate = ev
			return nil
		},
		UpdateHook: func(ev *proto.UpdateHookEvent) error {
			updates++
			lastUpdate = ev
			return nil
		},
		CommitHook: func() bool {
			commits++
			return true
		},
		RollbackHook: func() {
			rollbacks++
		},
	}, true)

	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithConfig(path, cfg)
	if err != nil {
		t.Fatalf("OpenWithConfig failed: %s", err)
	}
	defer db.Close()

	// Opening a WAL-mode database runs a transaction which is rolled back, to
	// force creation of the WAL files, so the rollback hook has already fired.
	if rollbacks != 1 {
		t.Fatalf("expected 1 rollback on open, got %d", rollbacks)
	}
	rollbacks = 0

	mustExecute(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

	// The schema change commits, but changes no rows.
	if preupdates != 0 || updates != 0 || commits != 1 || rollbacks != 0 {
		t.Fatalf("after CREATE TABLE: preupdates=%d, updates=%d, commits=%d, rollbacks=%d",
			preupdates, updates, commits, rollbacks)
	}

	mustExecute(db, "INSERT INTO t VALUES (1, 'fiona')")
	if preupdates != 1 || updates != 1 || commits != 2 || rollbacks != 0 {
		t.Fatalf("after INSERT: preupdates=%d, updates=%d, commits=%d, rollbacks=%d",
			preupdates, updates, commits, rollbacks)
	}
	if lastPreupdate.Table != "t" || lastPreupdate.Op != proto.CDCEvent_INSERT || lastPreupdate.NewRowId != 1 ||
		lastPreupdate.NewRow == nil || lastPreupdate.NewRow.Values[1].GetS() != "fiona" {
		t.Fatalf("unexpected preupdate event: %v", lastPreupdate)
	}
	if lastUpdate.Table != "t" || lastUpdate.Op != proto.UpdateHookEvent_INSERT || lastUpdate.RowId != 1 {
		t.Fatalf("unexpected update event: %v", lastUpdate)
	}

	mustExecute(db, "BEGIN; INSERT INTO t VALUES (2, 'declan'); ROLLBACK")
	if preupdates != 2 || updates != 2 || commits != 2 || rollbacks != 1 {
		t.Fatalf("after rolled-back INSERT: preupdates=%d, updates=%d, commits=%d, rollbacks=%d",
			preupdates, updates, commits, rollbacks)
	}
}

// Test_ConnConfig_PreUpdateHookOptions verifies the table filter and
// row-IDs-only options of the preupdate hook.
func Test_ConnConfig_PreUpdateHookOptions(t *testing.T) {
	var events []*proto.CDCEvent
	cfg := NewConfig(ConnConfig{
		PreUpdateHook: func(ev *proto.CDCEvent) error {
			events = append(events, ev)
			return nil
		},
		PreUpdateTableRe:    regexp.MustCompile("^foo$"),
		PreUpdateRowIDsOnly: true,
	}, true)

	path := mustTempPath()
	defer fsutil.RemoveAll(path)
	db, err := OpenWithConfig(path, cfg)
	if err != nil {
		t.Fatalf("OpenWithConfig failed: %s", err)
	}
	defer db.Close()
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)")
	mustExecute(db, "CREATE TABLE bar (id INTEGER PRIMARY KEY, name TEXT)")

	mustExecute(db, "INSERT INTO foo VALUES (1, 'fiona')")
	mustExecute(db, "INSERT INTO bar VALUES (1, 'fiona')")
	if len(events) != 1 {
		t.Fatalf("expected 1 event for table matching filter, got %d", len(events))
	}
	if events[0].Table != "foo" || events[0].NewRowId != 1 {
		t.Fatalf("unexpected event: %v", events[0])
	}
	if events[0].NewRow != nil {
		t.Fatalf("expected no row data with row IDs only, got %v", events[0].NewRow)
	}
}

// hookStats returns the current values of the hook invocation stats.
func hookStats() [3]int64 {
	return [3]int64{
		stats.Get(numPreupdates).(*expvar.Int).Value(),
		stats.Get(numUpdateHooks).(*expvar.Int).Value(),
		stats.Get(numCommitHooks).(*expvar.Int).Value(),
	}
}

// Test_ExtensionNames verifies that extension paths are reported by base
// name, in sorted order.
func Test_ExtensionNames(t *testing.T) {
	names := extensionNames([]string{"/tmp/ext2.so", "/tmp/ext1.so"})
	if len(names) != 2 || names[0] != "ext1.so" || names[1] != "ext2.so" {
		t.Fatalf("unexpected extension names: %v", names)
	}
}
