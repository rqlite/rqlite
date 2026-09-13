package db

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"testing"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

func Test_DB_ConnectionReplacement_Checkpointing(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(path)

	discardWriterConnection(t, db)
	if n, err := db.GetCheckpointing(); err != nil || n != 0 {
		t.Fatalf("checkpointing after replacement: got %d, error %v, want 0", n, err)
	}

	if err := db.EnableCheckpointing(); err != nil {
		t.Fatalf("failed to enable checkpointing: %s", err)
	}
	discardWriterConnection(t, db)
	if n, err := db.GetCheckpointing(); err != nil || n != 1000 {
		t.Fatalf("checkpointing after enabling and replacement: got %d, error %v, want 1000", n, err)
	}

	if err := db.DisableCheckpointing(); err != nil {
		t.Fatalf("failed to disable checkpointing: %s", err)
	}
	discardWriterConnection(t, db)
	if n, err := db.GetCheckpointing(); err != nil || n != 0 {
		t.Fatalf("checkpointing after disabling and replacement: got %d, error %v, want 0", n, err)
	}
}

func Test_DB_ConnectionReplacement_Hooks(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(path)
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")

	var preupdates, updates, commits int
	if err := db.RegisterPreUpdateHook(func(ev *command.CDCEvent) error {
		preupdates++
		if ev.Table != "foo" || ev.NewRowId != 1 || ev.NewRow.Values[0].GetI() != 1 {
			t.Errorf("unexpected preupdate event: %v", ev)
		}
		return nil
	}, nil, false); err != nil {
		t.Fatalf("failed to register preupdate hook: %s", err)
	}
	if err := db.RegisterUpdateHook(func(ev *command.UpdateHookEvent) error {
		updates++
		return nil
	}); err != nil {
		t.Fatalf("failed to register update hook: %s", err)
	}
	if err := db.RegisterCommitHook(func() bool {
		commits++
		return true
	}); err != nil {
		t.Fatalf("failed to register commit hook: %s", err)
	}

	discardWriterConnection(t, db)
	mustExecute(db, "INSERT INTO foo VALUES (1)")
	if preupdates != 1 || updates != 1 || commits != 1 {
		t.Fatalf("hooks after replacement: preupdates=%d, updates=%d, commits=%d; want 1 each", preupdates, updates, commits)
	}

	var replacementPreupdates int
	if err := db.RegisterPreUpdateHook(func(ev *command.CDCEvent) error {
		replacementPreupdates++
		return nil
	}, nil, true); err != nil {
		t.Fatalf("failed to replace preupdate hook: %s", err)
	}
	if err := db.RegisterUpdateHook(nil); err != nil {
		t.Fatalf("failed to remove update hook: %s", err)
	}
	if err := db.RegisterCommitHook(nil); err != nil {
		t.Fatalf("failed to remove commit hook: %s", err)
	}
	discardWriterConnection(t, db)
	mustExecute(db, "INSERT INTO foo VALUES (2)")
	if preupdates != 1 || replacementPreupdates != 1 || updates != 1 || commits != 1 {
		t.Fatalf("stale hooks after replacement: preupdates=%d, replacement=%d, updates=%d, commits=%d", preupdates, replacementPreupdates, updates, commits)
	}
}

func Test_DB_ConnectionReplacement_RollbackHook(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer db.Close()
	defer os.Remove(path)
	mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")

	var rollbacks int
	if err := db.RegisterRollbackHook(func() {
		rollbacks++
	}); err != nil {
		t.Fatalf("failed to register rollback hook: %s", err)
	}
	discardWriterConnection(t, db)
	mustExecute(db, "BEGIN; INSERT INTO foo VALUES (1); ROLLBACK")
	if rollbacks != 1 {
		t.Fatalf("rollback hook after replacement: got %d calls, want 1", rollbacks)
	}

	if err := db.RegisterRollbackHook(nil); err != nil {
		t.Fatalf("failed to remove rollback hook: %s", err)
	}
	discardWriterConnection(t, db)
	mustExecute(db, "BEGIN; INSERT INTO foo VALUES (1); ROLLBACK")
	if rollbacks != 1 {
		t.Fatalf("removed rollback hook called after replacement: got %d calls, want 1", rollbacks)
	}
}

// A cancelled transaction can make database/sql discard its connection, but
// cancellation races with explicit rollback. Return ErrBadConn directly to
// force the same replacement deterministically; all other operations use DB's
// exported methods.
func discardWriterConnection(t *testing.T, db *DB) {
	t.Helper()
	conn, err := db.rwDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to get writer connection: %s", err)
	}
	defer conn.Close()
	if err := conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("failed to discard writer connection: %v", err)
	}
}
