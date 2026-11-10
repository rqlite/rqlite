package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db/wal"
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
// than the last page, the checkpoint fails with ErrDatabaseCheckpointBusy.
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

	// Insert a row or the long-running read I'm about to kick off would just complete
	// and the checkpoint would complete. After all, a SELECT * on an empty table would not
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
		// nextFrameIdx never advanced in the busy branch, so the salt-check
		// block at entry is never reached: WALReset must be false.
		if meta.WALReset {
			t.Fatalf("%s: expected WALReset=false on busy, got true", label)
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
	if meta.WALReset {
		t.Fatalf("recovery: expected WALReset=false (nextFrameIdx was 0), got true")
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
	if meta.Moved != meta.Pages {
		t.Fatalf("checkpoint did not move all pages from WAL")
	}
	// First call: nextFrameIdx is 0 on entry, so the salt-check block is
	// skipped and WALReset must be false even though this attempt drives
	// the manager into the pnCkpt == pnLog state where reset detection
	// matters on subsequent calls.
	if meta.WALReset {
		t.Fatalf("expected WALReset=false on first checkpoint, got true (meta=%s)", meta)
	}
}

// Test_CheckpointManager_Checkpoint_WALReset_Detected verifies that when a
// previous checkpoint left the manager tracking partial-checkpoint state
// (nextFrameIdx > 0, salt saved) and SQLite subsequently resets the WAL,
// the next checkpoint detects the reset via the salt comparison and surfaces
// WALReset=true on the returned CheckpointManagerMeta.
func Test_CheckpointManager_Checkpoint_WALReset_Detected(t *testing.T) {
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
	if _, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("alice")`); err != nil {
		t.Fatalf("failed to insert: %s", err.Error())
	}

	// Reader holding its mark at the end of the WAL: blocks TRUNCATE but
	// not page movement -- drives the manager into pnCkpt == pnLog, which
	// is the only branch that saves nextFrameIdx > 0 and a salt.
	qr := &command.Request{
		Statements: []*command.Statement{
			{
				Sql:        "SELECT * FROM foo",
				ForceStall: true,
			},
		},
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		db.QueryWithContext(ctx, qr, false)
	}()
	time.Sleep(2 * time.Second)

	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}
	defer cm.Close()

	// First checkpoint: pnCkpt == pnLog, no truncate. Manager records
	// nextFrameIdx > 0 and the current salt for the next round.
	var buf bytes.Buffer
	meta, n, err := cm.Checkpoint(&buf, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("first checkpoint: unexpected error: %s", err.Error())
	}
	if meta == nil {
		t.Fatal("first checkpoint: expected non-nil meta")
	}
	if meta.WALReset {
		t.Fatal("first checkpoint: WALReset must be false on the very first attempt")
	}
	if !cm.resetWatch.armed {
		t.Fatal("first checkpoint: expected resetWatch to be armed (pnCkpt == pnLog path)")
	}
	if cm.resetWatch.resumeFrameIdx == 0 {
		t.Fatalf("first checkpoint: expected resumeFrameIdx > 0 (pnCkpt == pnLog path), got 0 (meta=%s)", meta)
	}
	// Every page was moved but the WAL was not truncated, so the compacted
	// WAL went to the writer and its size must be reported.
	if buf.Len() == 0 {
		t.Fatal("first checkpoint: expected compacted WAL data to be written")
	}
	if int(n) != buf.Len() {
		t.Fatalf("first checkpoint: expected %d compacted WAL bytes to be reported, got %d", buf.Len(), n)
	}

	// Release the reader. The previous checkpoint moved all frames to the
	// database, so SQLite considers the WAL fully consumed: the next write
	// restarts at frame 0 with a new salt rather than appending.
	cancelFunc()
	time.Sleep(500 * time.Millisecond)

	if _, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("carol")`); err != nil {
		t.Fatalf("failed to insert after reader released: %s", err.Error())
	}

	// Next checkpoint: nextFrameIdx > 0 on entry, header salt has changed,
	// so the salt-check block must trip and report WALReset.
	buf.Reset()
	meta, _, err = cm.Checkpoint(&buf, 5*time.Second)
	if err != nil {
		t.Fatalf("second checkpoint: unexpected error: %s", err.Error())
	}
	if meta == nil {
		t.Fatal("second checkpoint: expected non-nil meta")
	}
	if !meta.WALReset {
		t.Fatalf("second checkpoint: expected WALReset=true after WAL reset, got false (meta=%s)", meta)
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

// Test_CheckpointManager_Checkpoint_RolledBackFrames tests that a checkpoint
// handles a WAL which has frames after its last commit frame. SQLite may write
// a transaction's frames to the WAL before the transaction commits, and if the
// transaction is then rolled back those frames stay in the WAL after the last
// commit frame. A smaller committed transaction that follows overwrites only
// some of them. The compacted WAL must carry just the committed change.
func Test_CheckpointManager_Checkpoint_RolledBackFrames(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()
	mustExecute(db, `CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)

	// Checkpoint the table creation, so the test starts from a WAL with no
	// frames in it, and keep a copy of the database as it stands now. The
	// compacted WAL produced later is replayed against that copy.
	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}
	if _, _, err := cm.Checkpoint(nil, 5*time.Second); err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
	}
	base := mustTempFile()
	defer RemoveFiles(base)
	mustCopyFile(base, path)

	// Put frames into the WAL that belong to no committed transaction. A
	// transaction inserts 1000 rows, which take ids 1 to 1000, and then
	// inserts id 1 again. That last statement fails, so the whole transaction
	// is rolled back. The PRAGMA makes SQLite write the transaction's frames to
	// the WAL before it commits, so the rollback leaves them in the WAL with no
	// commit frame after them.
	mustExecute(db, "PRAGMA cache_size=5")
	stmts := make([]*command.Statement, 0, 1001)
	for i := range 1000 {
		stmts = append(stmts, &command.Statement{Sql: fmt.Sprintf("INSERT INTO foo(name) VALUES('%0500d')", i)})
	}
	stmts = append(stmts, &command.Statement{Sql: "INSERT INTO foo(id, name) VALUES(1, 'dup')"})
	results, err := db.Execute(&command.Request{Transaction: true, Statements: stmts}, false)
	if err != nil {
		t.Fatalf("failed to execute transaction: %s", err.Error())
	}
	if results[len(results)-1].GetE().GetError() == "" {
		t.Fatal("expected the last statement of the transaction to fail")
	}

	// Commit one small transaction. SQLite writes it where the rolled-back
	// transaction's frames begin, so it overwrites the first of them and its
	// commit frame is followed by the rest. The row takes id 1, since the
	// rollback left the table empty.
	mustExecute(db, `INSERT INTO foo(name) VALUES('fiona')`)

	// The checkpoint must compact the WAL by ignoring the frames after the
	// commit frame, and then succeed.
	var buf bytes.Buffer
	meta, n, err := cm.Checkpoint(&buf, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
	}
	if !meta.Success() {
		t.Fatalf("expected checkpoint to succeed, got meta=%s", meta)
	}
	if n == 0 || int(n) != buf.Len() {
		t.Fatalf("expected %d compacted WAL bytes to be reported, got %d", buf.Len(), n)
	}

	// Replay the compacted WAL against the copy. It must carry only the
	// committed transaction, so the copy must then hold only that row.
	if err := os.WriteFile(base+"-compacted", buf.Bytes(), 0644); err != nil {
		t.Fatalf("failed to write compacted WAL: %s", err.Error())
	}
	if err := ReplayWAL(base, []string{base + "-compacted"}, false); err != nil {
		t.Fatalf("failed to replay compacted WAL: %s", err.Error())
	}
	replayed, err := Open(base, false, true)
	if err != nil {
		t.Fatalf("failed to open replayed database: %s", err.Error())
	}
	defer replayed.Close()
	rows := mustQuery(replayed, "SELECT id, name FROM foo")
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected rows after replay: got %s, want %s", got, exp)
	}
}

// Test_CheckpointManager_Checkpoint_RolledBackFrames_Resume tests a checkpoint
// which resumes part-way through the WAL at the slot where a rolled-back
// transaction's frames begin. A reader blocks truncation, so the first
// checkpoint leaves the manager resuming at the slot after the last commit
// frame. A transaction then rolls back, leaving its frames from that slot on.
// A checkpoint with nothing committed after them must ignore them. After a
// commit overwrites the first of them, the checkpoint must carry that commit
// and ignore the rest.
func Test_CheckpointManager_Checkpoint_RolledBackFrames_Resume(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()
	mustExecute(db, `CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)

	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}
	if _, _, err := cm.Checkpoint(nil, 5*time.Second); err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
	}

	// Keep a copy of the database as it is before the writes, to replay the
	// compacted WALs against later.
	base := mustTempFile()
	defer RemoveFiles(base)
	mustCopyFile(base, path)
	mustExecute(db, `INSERT INTO foo(name) VALUES('fiona')`)

	// A reader which blocks truncation. It starts after the write, so its
	// snapshot includes every frame and the checkpoint can move every page.
	qr := &command.Request{
		Statements: []*command.Statement{{Sql: "SELECT * FROM foo", ForceStall: true}},
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		db.QueryWithContext(ctx, qr, false)
	}()
	time.Sleep(2 * time.Second)

	var bufA bytes.Buffer
	meta, _, err := cm.Checkpoint(&bufA, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to checkpoint with reader open: %s", err.Error())
	}
	if meta.Success() || meta.Moved != meta.Pages {
		t.Fatalf("expected a checkpoint which moved every page but did not truncate, got meta=%s", meta)
	}
	resumeIdx := cm.resetWatch.resumeFrameIdx
	if resumeIdx == 0 {
		t.Fatal("expected the checkpoint manager to resume part-way through the WAL")
	}

	// Have SQLite write the transaction's frames to the WAL before the
	// transaction commits, so that rolling it back leaves them in the WAL.
	// They begin at the resume slot, with no commit frame after them. The
	// checkpoint must ignore them, carry no frames, and leave the resume slot
	// where it was.
	mustExecute(db, "PRAGMA cache_size=5")
	stmts := make([]*command.Statement, 0, 1001)
	for i := range 1000 {
		stmts = append(stmts, &command.Statement{Sql: fmt.Sprintf("INSERT INTO foo(name) VALUES('%0500d')", i)})
	}
	stmts = append(stmts, &command.Statement{Sql: "INSERT INTO foo(id, name) VALUES(1, 'dup')"})
	results, err := db.Execute(&command.Request{Transaction: true, Statements: stmts}, false)
	if err != nil {
		t.Fatalf("failed to execute transaction: %s", err.Error())
	}
	if results[len(results)-1].GetE().GetError() == "" {
		t.Fatal("expected the last statement of the transaction to fail")
	}
	var bufB bytes.Buffer
	meta, _, err = cm.Checkpoint(&bufB, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to checkpoint with rolled-back frames at the resume slot: %s", err.Error())
	}
	if meta.Success() || meta.Moved != meta.Pages {
		t.Fatalf("expected a checkpoint which moved every page but did not truncate, got meta=%s", meta)
	}
	if bufB.Len() != wal.WALHeaderSize {
		t.Fatalf("expected a compacted WAL with no frames, got %d bytes", bufB.Len())
	}
	if cm.resetWatch.resumeFrameIdx != resumeIdx {
		t.Fatalf("rolled-back frames moved the resume slot from %d to %d", resumeIdx, cm.resetWatch.resumeFrameIdx)
	}

	// A commit overwrites the first of the rolled-back frames. It happens while
	// the reader still holds its mark, so SQLite appends to the WAL rather than
	// restarting it. Once the reader is gone, the checkpoint resumes at the
	// same slot, must carry that commit, ignore the rolled-back frames after
	// it, and truncate the WAL.
	mustExecute(db, `INSERT INTO foo(name) VALUES('declan')`)
	cancelFunc()
	<-readerDone
	var bufC bytes.Buffer
	meta, _, err = cm.Checkpoint(&bufC, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint after a commit over rolled-back frames: %s", err.Error())
	}
	if !meta.Success() {
		t.Fatalf("expected checkpoint to succeed, got meta=%s", meta)
	}

	// Replay the two compacted WALs which carry frames against the copy, which
	// must then hold exactly the committed rows.
	walA, walC := base+"-a", base+"-c"
	if err := os.WriteFile(walA, bufA.Bytes(), 0644); err != nil {
		t.Fatalf("failed to write compacted WAL: %s", err.Error())
	}
	if err := os.WriteFile(walC, bufC.Bytes(), 0644); err != nil {
		t.Fatalf("failed to write compacted WAL: %s", err.Error())
	}
	if err := ReplayWAL(base, []string{walA, walC}, false); err != nil {
		t.Fatalf("failed to replay compacted WALs: %s", err.Error())
	}
	replayed, err := Open(base, false, true)
	if err != nil {
		t.Fatalf("failed to open replayed database: %s", err.Error())
	}
	defer replayed.Close()
	rows := mustQuery(replayed, "SELECT id, name FROM foo")
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"declan"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected rows after replay: got %s, want %s", got, exp)
	}
}

// Test_CheckpointManager_Checkpoint_OpenTransactionCommits tests that frames
// a checkpoint ignored, because their transaction was still open, are carried
// by the next checkpoint once that transaction commits. A second connection
// holds the transaction, since a checkpoint cannot run on the connection with
// the open transaction. Replaying both compacted WALs must give every row.
func Test_CheckpointManager_Checkpoint_OpenTransactionCommits(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()
	mustExecute(db, `CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)

	// Checkpoint the table creation, and keep a copy of the database as it
	// stands now to replay the compacted WALs against later.
	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}
	if _, _, err := cm.Checkpoint(nil, 5*time.Second); err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
	}
	base := mustTempFile()
	defer RemoveFiles(base)
	mustCopyFile(base, path)
	mustExecute(db, `INSERT INTO foo(name) VALUES('fiona')`)

	// Open a transaction on a second connection and insert 1000 rows. The
	// PRAGMA makes SQLite write the transaction's frames to the WAL before it
	// commits, so they follow the last commit frame.
	txDB, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open second database connection: %s", err.Error())
	}
	defer txDB.Close()
	mustExecute(txDB, "PRAGMA cache_size=5")
	mustExecute(txDB, "BEGIN")
	stmts := make([]*command.Statement, 0, 1000)
	for i := range 1000 {
		stmts = append(stmts, &command.Statement{Sql: fmt.Sprintf("INSERT INTO foo(name) VALUES('%0500d')", i)})
	}
	if _, err := txDB.Execute(&command.Request{Statements: stmts}, false); err != nil {
		t.Fatalf("failed to execute inside transaction: %s", err.Error())
	}
	const frameSize = wal.WALFrameHeaderSize + 4096
	if sz := mustFileSize(db.WALPath()); sz <= wal.WALHeaderSize+frameSize {
		t.Fatalf("expected the open transaction to have written frames to the WAL, WAL is %d bytes", sz)
	}

	// The open transaction blocks truncation. The checkpoint must carry only
	// the frame of the committed row, and resume at the frame after it.
	var bufA bytes.Buffer
	meta, _, err := cm.Checkpoint(&bufA, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to checkpoint with a transaction open: %s", err.Error())
	}
	if meta.Success() || meta.Moved != meta.Pages {
		t.Fatalf("expected a checkpoint which moved every page but did not truncate, got meta=%s", meta)
	}
	if bufA.Len() != wal.WALHeaderSize+frameSize {
		t.Fatalf("expected a compacted WAL with one frame, got %d bytes", bufA.Len())
	}
	if cm.resetWatch.resumeFrameIdx != 1 {
		t.Fatalf("expected the checkpoint manager to resume at frame 1, got %d", cm.resetWatch.resumeFrameIdx)
	}

	// Commit the transaction. The next checkpoint must carry its frames and
	// truncate the WAL.
	mustExecute(txDB, "COMMIT")
	var bufB bytes.Buffer
	meta, _, err = cm.Checkpoint(&bufB, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint after COMMIT: %s", err.Error())
	}
	if !meta.Success() {
		t.Fatalf("expected checkpoint to succeed, got meta=%s", meta)
	}

	// Replay the two compacted WALs against the copy, which must then hold
	// every row.
	walA, walB := base+"-a", base+"-b"
	if err := os.WriteFile(walA, bufA.Bytes(), 0644); err != nil {
		t.Fatalf("failed to write compacted WAL: %s", err.Error())
	}
	if err := os.WriteFile(walB, bufB.Bytes(), 0644); err != nil {
		t.Fatalf("failed to write compacted WAL: %s", err.Error())
	}
	if err := ReplayWAL(base, []string{walA, walB}, false); err != nil {
		t.Fatalf("failed to replay compacted WALs: %s", err.Error())
	}
	replayed, err := Open(base, false, true)
	if err != nil {
		t.Fatalf("failed to open replayed database: %s", err.Error())
	}
	defer replayed.Close()
	rows := mustQuery(replayed, "SELECT COUNT(*) FROM foo")
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1001]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected rows after replay: got %s, want %s", got, exp)
	}
}

// Test_CheckpointManager_Checkpoint_OpenTransactionOnWriter tests a checkpoint
// attempted while an explicit transaction is open on the write connection. It
// must fail with an error the caller may retry, report no bytes written, and
// leave the WAL and the manager's resume state as they were. Once the
// transaction commits, the next checkpoint must carry every committed row.
func Test_CheckpointManager_Checkpoint_OpenTransactionOnWriter(t *testing.T) {
	path := mustTempFile()
	defer RemoveFiles(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()
	mustExecute(db, `CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)

	// Checkpoint the table creation, and keep a copy of the database as it
	// stands now to replay the compacted WAL against later.
	cm, err := NewCheckpointManager(db)
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %s", err.Error())
	}
	if _, _, err := cm.Checkpoint(nil, 5*time.Second); err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
	}
	base := mustTempFile()
	defer RemoveFiles(base)
	mustCopyFile(base, path)

	// Commit one row, then open a transaction and write a second row inside it.
	mustExecute(db, `INSERT INTO foo(name) VALUES('fiona')`)
	mustExecute(db, "BEGIN")
	mustExecute(db, `INSERT INTO foo(name) VALUES('declan')`)
	walSz := mustFileSize(db.WALPath())

	// The checkpoint must fail. The error must not be one which reports itself
	// as not retryable, since the checkpoint will work once the transaction ends.
	var buf bytes.Buffer
	_, n, err := cm.Checkpoint(&buf, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected checkpoint to fail with a transaction open on the write connection")
	}
	var re RetryableError
	if errors.As(err, &re) && !re.Retryable() {
		t.Fatalf("expected an error which may be retried, got %s", err.Error())
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes reported on error, got %d", n)
	}
	if cm.resetWatch.armed {
		t.Fatal("expected the failed checkpoint to leave the manager's resume state unset")
	}
	if sz := mustFileSize(db.WALPath()); sz != walSz {
		t.Fatalf("expected the failed checkpoint to leave the WAL at %d bytes, got %d", walSz, sz)
	}

	// Commit the transaction. The next checkpoint must succeed, and its
	// compacted WAL must carry both rows.
	mustExecute(db, "COMMIT")
	buf.Reset()
	meta, n, err := cm.Checkpoint(&buf, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint after COMMIT: %s", err.Error())
	}
	if !meta.Success() {
		t.Fatalf("expected checkpoint to succeed, got meta=%s", meta)
	}
	if n == 0 || int(n) != buf.Len() {
		t.Fatalf("expected %d compacted WAL bytes to be reported, got %d", buf.Len(), n)
	}
	if err := os.WriteFile(base+"-compacted", buf.Bytes(), 0644); err != nil {
		t.Fatalf("failed to write compacted WAL: %s", err.Error())
	}
	if err := ReplayWAL(base, []string{base + "-compacted"}, false); err != nil {
		t.Fatalf("failed to replay compacted WAL: %s", err.Error())
	}
	replayed, err := Open(base, false, true)
	if err != nil {
		t.Fatalf("failed to open replayed database: %s", err.Error())
	}
	defer replayed.Close()
	rows := mustQuery(replayed, "SELECT id, name FROM foo")
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"declan"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected rows after replay: got %s, want %s", got, exp)
	}
}

// Test_WALResetWatch tests the watch through its states. Unarmed, it reports
// frame 0 and no reset. Armed, it reports the resume frame for as long as the
// salt is unchanged. Once the salt changes it reports a reset and frame 0, and
// disarms itself, so the next check reports no reset. Disarm returns an armed
// watch to the unarmed state.
func Test_WALResetWatch(t *testing.T) {
	saltA, saltB := wal.Salt{1, 2}, wal.Salt{1, 3}
	check := func(w *WALResetWatch, salt wal.Salt, expIdx int64, expReset bool) {
		t.Helper()
		idx, reset := w.Check(salt)
		if idx != expIdx || reset != expReset {
			t.Fatalf("expected frame index %d and reset %t, got %d and %t", expIdx, expReset, idx, reset)
		}
	}

	// The zero value is unarmed.
	var w WALResetWatch
	check(&w, saltA, 0, false)

	// Armed, the watch keeps reporting the resume frame while the salt holds.
	w.Arm(saltA, 7)
	check(&w, saltA, 7, false)
	check(&w, saltA, 7, false)

	// A changed salt is a reset, and the watch disarms itself.
	check(&w, saltB, 0, true)
	check(&w, saltB, 0, false)
	check(&w, saltA, 0, false)

	// Arming again replaces the salt and the resume frame.
	w.Arm(saltB, 3)
	check(&w, saltB, 3, false)

	// Disarm clears the watch, even for the salt it was armed with.
	w.Disarm()
	check(&w, saltB, 0, false)
}
