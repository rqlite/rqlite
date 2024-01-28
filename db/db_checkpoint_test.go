package db

import (
	"bytes"
	"os"
	"testing"
	"time"
)

// Test_WALDatabaseCheckpointOKNoWAL tests that a checkpoint succeeds
// even when no WAL file exists.
func Test_WALDatabaseCheckpointOKNoWAL(t *testing.T) {
	path := mustTempFile()
	defer os.Remove(path)

	db, err := Open(path, false, true)
	if err != nil {
		t.Fatalf("failed to open database in WAL mode: %s", err.Error())
	}
	if !db.WALEnabled() {
		t.Fatalf("WAL mode not enabled")
	}
	if fileExists(db.WALPath()) {
		t.Fatalf("WAL file exists when no writes have happened")
	}
	defer db.Close()
	if err := db.Checkpoint(CheckpointTruncate); err != nil {
		t.Fatalf("failed to checkpoint database in WAL mode with non-existent WAL: %s", err.Error())
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

	walPreBytes, err := os.ReadFile(db.WALPath())
	if err != nil {
		t.Fatalf("failed to read wal file: %s", err.Error())
	}
	if err := db.Checkpoint(CheckpointRestart); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
	walPostBytes, err := os.ReadFile(db.WALPath())
	if err != nil {
		t.Fatalf("failed to read wal file: %s", err.Error())
	}
	if !bytes.Equal(walPreBytes, walPostBytes) {
		t.Fatalf("wal files should be identical after checkpoint restart")
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

	if err := db.CheckpointWithTimeout(CheckpointTruncate, 250*time.Millisecond); err == nil {
		t.Fatal("expected error due to failure to checkpoint")
	}

	blockingDB.Close()
	if err := db.CheckpointWithTimeout(CheckpointTruncate, 250*time.Millisecond); err != nil {
		t.Fatalf("failed to checkpoint database: %s", err.Error())
	}
}
