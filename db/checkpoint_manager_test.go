package db

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

func Test_CheckpointManager_Create(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()

	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}
	if cm == nil {
		t.Fatal("expected non-nil checkpoint manager")
	}
	if err := cm.Close(); err != nil {
		t.Fatalf("expected Close() to return nil, got %s", err.Error())
	}
}

func Test_CheckpointManager_Checkpoint_OK(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()

	_, err = db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("alice")`)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}

	var buf bytes.Buffer
	meta, n, err := cm.Checkpoint(&buf, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
	}
	if meta == nil {
		t.Fatal("expected non-nil CheckpointMeta")
	}
	if !meta.Success() {
		t.Fatalf("expected checkpoint to succeed (WAL truncated), got meta=%s", meta)
	}
	if n == 0 {
		t.Fatal("expected compacted WAL bytes written to be > 0")
	}
	if buf.Len() == 0 {
		t.Fatal("expected data to be written to writer")
	}

	if !IsValidSQLiteWALData(buf.Bytes()) {
		t.Fatal("expected valid SQLite WAL data to be written to writer")
	}

	// Confirm that the WAL file is zero bytes long after truncate checkpoint.
	sz, err := fsutil.FileSize(db.WALPath())
	if err != nil {
		t.Fatalf("failed to get WAL file size: %s", err.Error())
	}
	if sz != 0 {
		t.Fatalf("expected WAL file size to be 0 after checkpoint truncate, got %d", sz)
	}

	// Check that the database has the correct data after checkpoint.
	rows, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"alice"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected query result: got %s, want %s", got, exp)
	}

	// Check back-to-back checkpointing.
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("bob")`)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}
	meta, n, err = cm.Checkpoint(&buf, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
	}
	if meta == nil {
		t.Fatal("expected non-nil CheckpointMeta")
	}
	if !meta.Success() {
		t.Fatalf("expected checkpoint to succeed (WAL truncated), got meta=%s", meta)
	}
	if n == 0 {
		t.Fatal("expected compacted WAL bytes written to be > 0")
	}
	if buf.Len() == 0 {
		t.Fatal("expected data to be written to writer")
	}
	rows, err = db.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected query result: got %s, want %s", got, exp)
	}

	// Ensure idempotency.
	meta, n, err = cm.Checkpoint(&buf, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
	}
	if meta == nil {
		t.Fatal("expected non-nil CheckpointMeta")
	}
	if !meta.Success() {
		t.Fatalf("expected idempotent checkpoint to succeed, got meta=%s", meta)
	}
}

func Test_CheckpointManager_Checkpoint_NoWriter_OK(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()

	_, err = db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("alice")`)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}

	meta, n, err := cm.Checkpoint(nil, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
	}
	if meta == nil {
		t.Fatal("expected non-nil CheckpointMeta for nil-writer checkpoint")
	}
	if !meta.Success() {
		t.Fatalf("expected nil-writer checkpoint to succeed (WAL truncated), got meta=%s", meta)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes written for nil-writer checkpoint, got %d", n)
	}

	// Confirm that the WAL file is zero bytes long after truncate checkpoint.
	sz, err := fsutil.FileSize(db.WALPath())
	if err != nil {
		t.Fatalf("failed to get WAL file size: %s", err.Error())
	}
	if sz != 0 {
		t.Fatalf("expected WAL file size to be 0 after checkpoint truncate, got %d", sz)
	}

	// Check that the database has the correct data after checkpoint.
	rows, err := db.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"alice"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected query result: got %s, want %s", got, exp)
	}
}

// Test_CheckpointManager_Checkpoint_Blocked_Read tests that if a checkpoint is
// blocked by a long-running read of the WAL file which holds WAL frames other
// than the last page, the checkpoint fails with ErrDatabaseCheckpointBusy
func Test_CheckpointManager_Checkpoint_Blocked_Read(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()

	_, err = db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Insert a row or the long-running read I'm a about to kick off would just complete
	// and the checkpoint would complete. After all a SELECT * on an empty table would not
	// read any WAL frames, so the checkpoint would not be blocked.
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("alice")`)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	// Issue a long-running read that should block the checkpoint.
	qr := &command.Request{
		Statements: []*command.Statement{
			{
				Sql:        "SELECT * FROM foo",
				ForceStall: true,
			},
		},
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	go func() {
		db.QueryWithContext(ctx, qr, false)
	}()
	// Wait for read to kick in.
	time.Sleep(2 * time.Second)

	// Now, add more pages to the WAL.
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("alice")`)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}
	defer cm.Close()

	var buf bytes.Buffer
	meta, n, err := cm.Checkpoint(&buf, 100*time.Millisecond)
	if err != ErrDatabaseCheckpointBusy {
		t.Fatalf("expected checkpoint to fail with ErrDatabaseCheckpointBusy, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes written on busy error, got %d", n)
	}
	if meta == nil {
		t.Fatalf("expected nil CheckpointMeta on busy error, got %s", meta)
	}

	// Confirm that the returned error is retryable.
	var re RetryableError
	if errors.As(err, &re) && !re.Retryable() {
		t.Fatalf("expected error to be retryable, got %s", err.Error())
	}
}

// Test_CheckpointManager_Checkpoint_Blocked_Read_Twice tests that two
// consecutive pnCkpt < pnLog checkpoint failures leave the manager in a state
// that allows the next attempt -- once the blocking reader is released -- to
// fully capture all WAL frames.
//
// Guards against regressions in the busy branch that would advance
// nextFrameIdx or salt and cause subsequent compacted WAL snapshots to silently
// omit frames.
func Test_CheckpointManager_Checkpoint_Blocked_Read_Twice(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()

	if _, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	// One row so the reader doesn't complete immediately.
	if _, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("alice")`); err != nil {
		t.Fatalf("failed to insert: %s", err.Error())
	}

	// Long-running read that holds a read mark mid-WAL.
	qr := &command.Request{
		Statements: []*command.Statement{
			{
				Sql:        "SELECT * FROM foo",
				ForceStall: true,
			},
		},
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	go func() {
		db.QueryWithContext(ctx, qr, false)
	}()
	time.Sleep(2 * time.Second)

	// Add WAL frames after the reader's mark so checkpoints will see
	// pnCkpt < pnLog (some pages can't be moved past the read mark).
	if _, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("bob")`); err != nil {
		t.Fatalf("failed to insert: %s", err.Error())
	}

	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}
	defer cm.Close()

	doBusyCheckpoint := func(label string) []byte {
		var buf bytes.Buffer
		meta, n, err := cm.Checkpoint(&buf, 100*time.Millisecond)
		if err != ErrDatabaseCheckpointBusy {
			t.Fatalf("%s: expected ErrDatabaseCheckpointBusy, got %v", label, err)
		}
		if n != 0 {
			t.Fatalf("%s: expected 0 bytes returned on busy, got %d", label, n)
		}
		if meta == nil {
			t.Fatalf("%s: expected non-nil CheckpointMeta on busy", label)
		}
		if buf.Len() == 0 {
			t.Fatalf("%s: expected compacted WAL to have been written to writer", label)
		}
		if !IsValidSQLiteWALData(buf.Bytes()) {
			t.Fatalf("%s: expected valid SQLite WAL data in writer", label)
		}
		// Crucial: a busy checkpoint must not advance nextFrameIdx, or the
		// next attempt would scan from a stale offset and drop frames.
		// (cm.salt may be populated by line 168 even on busy -- harmless
		// because it's only consulted when nextFrameIdx > 0.)
		if cm.nextFrameIdx != 0 {
			t.Fatalf("%s: expected nextFrameIdx to remain 0, got %d", label, cm.nextFrameIdx)
		}
		return buf.Bytes()
	}

	// Two consecutive busy checkpoints with no intervening writes must
	// produce identical compacted WAL bytes -- proving the second attempt
	// re-scanned from offset 0 just like the first.
	first := doBusyCheckpoint("first")
	second := doBusyCheckpoint("second")
	if !bytes.Equal(first, second) {
		t.Fatalf("compacted WAL differs across consecutive busy checkpoints (len %d vs %d)",
			len(first), len(second))
	}

	// Release the blocking reader and give defer rs.Close() time to run.
	cancelFunc()
	time.Sleep(500 * time.Millisecond)

	// Recovery: the next checkpoint must succeed and truncate the WAL.
	var buf bytes.Buffer
	meta, n, err := cm.Checkpoint(&buf, 5*time.Second)
	if err != nil {
		t.Fatalf("recovery: unexpected error: %s", err.Error())
	}
	if meta == nil || !meta.Success() {
		t.Fatalf("recovery: expected successful (truncated) checkpoint, got meta=%s", meta)
	}
	if n == 0 {
		t.Fatal("recovery: expected non-zero bytes returned")
	}
	if !IsValidSQLiteWALData(buf.Bytes()) {
		t.Fatal("recovery: expected valid SQLite WAL data in writer")
	}
	sz, err := fsutil.FileSize(db.WALPath())
	if err != nil {
		t.Fatalf("failed to stat WAL file: %s", err.Error())
	}
	if sz != 0 {
		t.Fatalf("recovery: expected WAL file size to be 0 after truncate, got %d", sz)
	}
}

// Test_CheckpointManager_Checkpoint_Blocked_ReadLastPage tests that if a checkpoint is
// blocked by a long-running read of the last page of the WAL file, the checkpoint
// still succeeds.
func Test_CheckpointManager_Checkpoint_Blocked_ReadLastPage(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()

	_, err = db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("alice")`)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	// Issue a long-running read that should block the checkpoint.
	qr := &command.Request{
		Statements: []*command.Statement{
			{
				Sql:        "SELECT * FROM foo",
				ForceStall: true,
			},
		},
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	go func() {
		db.QueryWithContext(ctx, qr, false)
	}()
	time.Sleep(2 * time.Second)

	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}
	defer cm.Close()

	var buf bytes.Buffer
	meta, _, err := cm.Checkpoint(&buf, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error checkpoint: %s", err.Error())
	}
	if meta == nil {
		t.Fatal("expected non-nil CheckpointMeta on successful checkpoint")
	}
}

func Test_CheckpointManager_Close(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()

	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}

	if err := cm.Close(); err != nil {
		t.Fatalf("expected Close() to return nil, got %s", err.Error())
	}
}
