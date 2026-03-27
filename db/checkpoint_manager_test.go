package db

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

func Test_CheckpointManager_Create(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

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
	defer os.Remove(path)

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
	n, err := cm.Checkpoint(&buf, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
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
	sz, err := fileSize(db.WALPath())
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
	n, err = cm.Checkpoint(&buf, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
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
}

func Test_CheckpointManager_Checkpoint_NoWriter_OK(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

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

	_, err = cm.Checkpoint(nil, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to checkpoint: %s", err.Error())
	}

	// Confirm that the WAL file is zero bytes long after truncate checkpoint.
	sz, err := fileSize(db.WALPath())
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

func Test_CheckpointManager_Checkpoint_Blocked(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

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
	_, err = cm.Checkpoint(&buf, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected checkpoint to fail due to blocking read")
	}
	if err != ErrDatabaseCheckpointBusy {
		t.Fatalf("expected ErrDatabaseCheckpointBusy, got %s", err.Error())
	}
}

func Test_CheckpointManager_Close(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

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
