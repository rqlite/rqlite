package db

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v9/command/proto"
	"github.com/rqlite/rqlite/v9/db/wal"
	"github.com/rqlite/rqlite/v9/internal/rsum"
)

// Test_WALDatabaseCheckpointOKNoWrites tests that a checkpoint succeeds
// even when data has been written to the database.
func Test_WALDatabaseCheckpointOKNoWrites(t *testing.T) {
	t.Skip()
	path := mustTempFile()
	defer os.Remove(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	defer db.Close()
	meta, err := db.Checkpoint(CheckpointTruncate)
	if err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode with nonexistent WAL: %s", err.Error())
	}
	if !meta.Success() {
		t.Fatalf("expected checkpoint to complete successfully")
	}
	if meta.Moved != 0 {
		t.Fatalf("expected MOVED to be 0, got %d", meta.Moved)
	}
	if meta.Pages != 0 {
		t.Fatalf("expected PAGES to be 0, got %d", meta.Pages)
	}
}

// Test_WALDatabaseCheckpointOK tests that a checkpoint succeeds
// with a write.
func Test_WALDatabaseCheckpointOK(t *testing.T) {
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

	meta, err := db.Checkpoint(CheckpointTruncate)
	if err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
	if !meta.Success() {
		t.Fatalf("expected checkpoint to complete successfully")
	}
	if meta.Moved != 0 {
		t.Fatalf("expected MOVED to be 0 since WAL was truncated, got %d", meta.Moved)
	}
	if meta.Pages != 0 {
		t.Fatalf("expected PAGES to be 0 since WAL was truncated, got %d", meta.Pages)
	}

	// Ensure idempotency by checkpointing again.
	meta, err = db.Checkpoint(CheckpointTruncate)
	if err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
	if !meta.Success() {
		t.Fatalf("expected checkpoint to complete successfully")
	}
}

// Test_WALDatabaseCheckpointOK_NoWALChange tests that a checkpoint
// that is blocked by a long-running read does not result in a
// change to the WAL file. This is to show that we can safely retry
// the truncate checkpoint later.
func Test_WALDatabaseCheckpointOK_NoWALChange(t *testing.T) {
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
	go func() {
		db.Query(qr, false)
	}()
	time.Sleep(2 * time.Second)

	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("alice")`)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	// Get the hash of the WAL file before the checkpoint.
	h1, err := rsum.MD5(db.WALPath())
	if err != nil {
		t.Fatalf("failed to hash WAL file: %s", err.Error())
	}

	_, err = db.ExecuteStringStmt(`PRAGMA BUSY_TIMEOUT = 1`)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	meta, err := db.Checkpoint(CheckpointTruncate)
	if err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
	if meta.Success() {
		t.Fatalf("expected checkpoint to be unsuccessful due to blocking read")
	}
	if meta.Moved == 0 {
		t.Fatalf("expected MOVED to be > 0 since some pages should have been moved")
	}
	if meta.Pages == 0 {
		t.Fatalf("expected PAGES to be > 0 since WAL should have pages")
	}
	if meta.Moved >= meta.Pages {
		t.Fatalf("expected MOVED to be < PAGES since checkpoint incomplete")
	}

	// Check hash again.
	h2, err := rsum.MD5(db.WALPath())
	if err != nil {
		t.Fatalf("failed to hash WAL file: %s", err.Error())
	}

	if h1 != h2 {
		t.Fatalf("expected WAL file to be unchanged after incomplete checkpoint")
	}
}

// Test_WALDatabaseCheckpointOKDelete tests that a checkpoint returns no error
// even when the database is opened in DELETE mode.
func Test_WALDatabaseCheckpointOKDelete(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

	db, err := Open(path, false, false)
	if err != nil {
		t.Fatalf("failed to open database in DELETE mode: %s", err.Error())
	}
	if db.WALEnabled() {
		t.Fatalf("WAL mode enabled")
	}
	defer db.Close()
	if _, err := db.Checkpoint(CheckpointTruncate); err != nil {
		t.Fatalf("failed to checkpoint database in DELETE mode: %s", err.Error())
	}
}

// Test_WALDatabaseCheckpoint_Restart tests that a checkpoint restart
// returns no error and that the WAL file is not modified even though
// all the WAL pages are copied to the database file. Then Truncate
// is called and the WAL file is deleted.
func Test_WALDatabaseCheckpoint_RestartTruncate(t *testing.T) {
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
	for i := 0; i < 50; i++ {
		_, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
		if err != nil {
			t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
		}
	}

	walPreBytes := mustReadBytes(db.WALPath())
	if meta, err := db.Checkpoint(CheckpointRestart); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	} else if !meta.Success() {
		t.Fatalf("expected checkpoint to complete successfully")
	} else if meta.Moved == 0 {
		t.Fatalf("expected some pages to be moved during RESTART checkpoint")
	} else if meta.Pages == 0 {
		t.Fatalf("expected some pages to be in the WAL during RESTART checkpoint")
	}
	walPostBytes := mustReadBytes(db.WALPath())
	if !bytes.Equal(walPreBytes, walPostBytes) {
		t.Fatalf("wal file should be unchanged after checkpoint restart")
	}

	// query the data to make sure all is well.
	rows, err := db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to execute query on single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[50]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected %s, got %s", exp, got)
	}

	if meta, err := db.Checkpoint(CheckpointTruncate); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	} else if !meta.Success() {
		t.Fatalf("expected checkpoint to complete successfully")
	} else if meta.Moved != 0 {
		t.Fatalf("expected 0 pages to be moved during checkpoint truncate since nowrite since restart checkpoint")
	}
	sz, err := fileSize(db.WALPath())
	if err != nil {
		t.Fatalf("wal file should be deleted after checkpoint truncate")
	}
	if sz != 0 {
		t.Fatalf("wal file should be zero length after checkpoint truncate")
	}

	// query the data to make sure all is still well.
	rows, err = db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to execute query on single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[50]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected %s, got %s", exp, got)
	}
}

// Test_WALDatabaseCheckpoint_RestartTimeout tests that a restart checkpoint
// does time out as expected if there is a long running read.
func Test_WALDatabaseCheckpoint_RestartTimeout(t *testing.T) {
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
	for i := 0; i < 50; i++ {
		_, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
		if err != nil {
			t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
		}
	}

	blockingDB, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open blocking database in WAL mode: %s", err.Error())
	}
	defer blockingDB.Close()
	_, err = blockingDB.QueryStringStmt(`BEGIN TRANSACTION`)
	if err != nil {
		t.Fatalf("failed to execute query on single node: %s", err.Error())
	}
	rows, err := blockingDB.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to execute query on single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[50]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected %s, got %s", exp, got)
	}

	meta, err := db.CheckpointWithTimeout(CheckpointRestart, 250*time.Millisecond)
	if err != nil {
		t.Fatal("expected no error when checkpoint times out due to a blocking read transaction")
	}
	if meta.Success() {
		t.Fatal("expected checkpoint to be unsuccessful")
	}

	// Get some information on the WAL file before the checkpoint. The goal here is
	// to confirm that after a non-completing RESTART checkpoint, a write does
	// not RESET the WAL file.
	walSzPre := mustFileSize(db.WALPath())
	hdrPre := mustGetWALHeader(db.WALPath())
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	// Check that the WAL file has grown, because we want to ensure that the next write
	// is appended to the WAL file, and doesn't overwrite the first page.
	walSzPost := mustFileSize(db.WALPath())
	hdrPost := mustGetWALHeader(db.WALPath())
	if walSzPost <= walSzPre {
		t.Fatalf("wal file should have grown after post-failed-checkpoint write")
	}
	if !bytes.Equal(hdrPre, hdrPost) {
		t.Fatalf("wal file header should be unchanged after post-failed-checkpoint write")
	}

	blockingDB.Close()
	if _, err := db.CheckpointWithTimeout(CheckpointRestart, 250*time.Millisecond); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
}

// Test_WALDatabaseCheckpoint_TruncateTimeout tests that a truncate checkpoint
// does time out as expected if there is a long running read. It also confirms
// that the WAL file is not modified as a result of this failure.
func Test_WALDatabaseCheckpoint_TruncateTimeout(t *testing.T) {
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
	for i := 0; i < 50; i++ {
		_, err := db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
		if err != nil {
			t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
		}
	}

	preWALBytes := mustReadBytes(db.WALPath())
	blockingDB, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open blocking database in WAL mode: %s", err.Error())
	}
	defer blockingDB.Close()
	_, err = blockingDB.QueryStringStmt(`BEGIN TRANSACTION`)
	if err != nil {
		t.Fatalf("failed to execute query on single node: %s", err.Error())
	}
	rows, err := blockingDB.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to execute query on single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[50]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected %s, got %s", exp, got)
	}

	meta, err := db.CheckpointWithTimeout(CheckpointRestart, 250*time.Millisecond)
	if err != nil {
		t.Fatal("expected no error due to failure to checkpoint due to COMMIT")
	}
	if meta.Success() {
		t.Fatal("expected checkpoint to be unsuccessful")
	}

	postWALBytes := mustReadBytes(db.WALPath())
	if !bytes.Equal(preWALBytes, postWALBytes) {
		t.Fatalf("wal file should be unchanged after checkpoint failure")
	}

	// Confirm that the next write to the WAL is appended to the WAL file, and doesn't
	// overwrite the first page i.e. that the WAL is not reset.
	hdrPre := mustGetWALHeader(db.WALPath())
	_, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}
	rows, err = db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
	if err != nil {
		t.Fatalf("failed to execute query on single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[51]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected %s, got %s", exp, got)
	}
	hdrPost := mustGetWALHeader(db.WALPath())
	if !bytes.Equal(hdrPre, hdrPost) {
		t.Fatalf("wal file header should be unchanged after post-failed-TRUNCATE checkpoint write")
	}

	blockingDB.Close()
	if _, err := db.CheckpointWithTimeout(CheckpointTruncate, 250*time.Millisecond); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
	if mustFileSize(db.WALPath()) != 0 {
		t.Fatalf("wal file should be zero length after checkpoint truncate")
	}
}

func mustReadBytes(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return b
}

func mustGetWALHeader(path string) []byte {
	fd, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	hdr := make([]byte, wal.WALHeaderSize)
	_, err = io.ReadFull(fd, hdr)
	if err != nil {
		panic(err)
	}
	return hdr
}
