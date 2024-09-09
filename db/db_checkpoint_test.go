package db

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/db/wal"
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
	if err := db.Checkpoint(CheckpointTruncate); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode with nonexistent WAL: %s", err.Error())
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
	if err := db.Checkpoint(CheckpointTruncate); err != nil {
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
	if err := db.Checkpoint(CheckpointRestart); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
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

	if err := db.Checkpoint(CheckpointTruncate); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
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

	if err := db.CheckpointWithTimeout(CheckpointRestart, 250*time.Millisecond); err == nil {
		t.Fatal("expected error due to failure to checkpoint")
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
	if err := db.CheckpointWithTimeout(CheckpointRestart, 250*time.Millisecond); err != nil {
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

	if err := db.CheckpointWithTimeout(CheckpointTruncate, 250*time.Millisecond); err == nil {
		t.Fatal("expected error due to failure to checkpoint")
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
	if err := db.CheckpointWithTimeout(CheckpointTruncate, 250*time.Millisecond); err != nil {
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
