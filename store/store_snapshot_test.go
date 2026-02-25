package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/random"
)

// Test_SingleNodeSnapshot tests that the Store correctly takes a snapshot
// and recovers from it.
func Test_SingleNodeSnapshot(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}
	_, _, err := s.Execute(context.Background(), executeRequestFromStrings(queries, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	rows, _, _, err := s.Query(context.Background(), queryRequestFromString("SELECT * FROM foo", false, false))
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Snap the node and write to disk.
	fsm := NewFSM(s)
	f, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("failed to snapshot node: %s", err.Error())
	}

	snapDir := t.TempDir()
	snapFile, err := os.Create(filepath.Join(snapDir, "snapshot"))
	if err != nil {
		t.Fatalf("failed to create snapshot file: %s", err.Error())
	}
	defer snapFile.Close()
	sink := &mockSnapshotSink{snapFile, nil}
	if err := f.Persist(sink); err != nil {
		t.Fatalf("failed to persist snapshot to disk: %s", err.Error())
	}

	// Check restoration.
	snapFile, err = os.Open(filepath.Join(snapDir, "snapshot"))
	if err != nil {
		t.Fatalf("failed to open snapshot file: %s", err.Error())
	}
	defer snapFile.Close()
	if err := fsm.Restore(snapFile); err != nil {
		t.Fatalf("failed to restore snapshot from disk: %s", err.Error())
	}

	// Ensure database is back in the correct state.
	r, _, _, err := s.Query(context.Background(), queryRequestFromString("SELECT * FROM foo", false, false))
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeUserSnapshot_CAS(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Ensures there is something to snapshot.
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
	}, false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}

	if err := s.snapshotCAS.Begin("snapshot-test"); err != nil {
		t.Fatalf("failed to begin snapshot CAS: %s", err.Error())
	}

	// Ensures there is something to snapshot.
	er = executeRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Snapshot(0); err == nil {
		t.Fatalf("expected error snapshotting single-node store with CAS")
	}
	s.snapshotCAS.End()
	er = executeRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(2, "declan")`,
	}, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}
}

func Test_SingleNodeUserSnapshot_Sync(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
	}, false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}

	// Register a channel, and close it, allowing snapshotting to proceed.
	ch := make(chan chan struct{})
	s.RegisterSnapshotSync(ch)
	called := false
	go func() {
		c := <-ch
		called = true
		close(c)
	}()
	er = executeRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store with sync: %s", err.Error())
	}
	if !called {
		t.Fatalf("expected sync function to be called")
	}

	// Register a channel, but don't close it, which should cause a timeout.
	er = executeRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(2, "declan")`,
	}, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Snapshot(0); err == nil {
		t.Fatalf("snapshotting succeeded, expected failure due to sync timeout")
	}
}

// Test_SingleNode_ErrNoWALToSnapshot tests that Snapshot returns ErrNoWALToSnapshot
// when there is no WAL data to snapshot. This happens when the only new Raft log
// entries since the last snapshot don't modify the database (e.g. Noop commands).
func Test_SingleNode_ErrNoWALToSnapshot(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Write some data to the database and take a snapshot. This should succeed.
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}

	// Write a Noop to the Raft log — this creates a new log entry but doesn't
	// change the database, so there is no WAL data.
	mustNoop(s, "test-noop")

	// Snapshot should now return ErrNoWALToSnapshot because there is no data
	// in the WAL to snapshot.
	if err := s.Snapshot(0); err != ErrNoWALToSnapshot {
		t.Fatalf("expected ErrNoWALToSnapshot, got: %v", err)
	}
}

func Test_SingleNode_WALTriggeredSnapshot(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	s.SnapshotThreshold = 8192
	s.SnapshotInterval = 500 * time.Millisecond
	s.SnapshotThresholdWALSize = 4096
	s.SnapshotReapThreshold = 2

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	nSnaps := stats.Get(numWALSnapshots).String()

	for i := 0; i < 100; i++ {
		_, _, err := s.Execute(context.Background(), executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
		if err != nil {
			t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
		}
	}

	// Ensure WAL-triggered snapshots take place.
	f := func() bool {
		return stats.Get(numWALSnapshots).String() != nSnaps
	}
	testPoll(t, f, 100*time.Millisecond, 2*time.Second)

	// Sanity-check the contents of the Store. There should be two
	// files -- a SQLite database file, and a directory named after
	// the most recent snapshot. This basically checks that reaping
	// is working, as it can be tricky on Windows due to stricter
	// file deletion rules.
	time.Sleep(5 * time.Second) // Tricky to know when all snapshots are done. Just wait.
	snaps, err := s.snapshotStore.List()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err.Error())
	}
	if len(snaps) != 1 {
		t.Fatalf("wrong number of snapshots: %d", len(snaps))
	}
	snapshotDir := filepath.Join(s.raftDir, snapshotsDirName)
	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		t.Fatalf("failed to read snapshot store dir: %s", err.Error())
	}
	if len(files) != 1 {
		t.Fatalf("wrong number of snapshot store entries: %d", len(files))
	}
	if files[0].Name() != snaps[0].ID {
		t.Fatalf("snapshot store entry name %s does not match snapshot ID %s", files[0].Name(), snaps[0].ID)
	}
}

func Test_SingleNode_SnapshotFail_Blocked(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	s.SnapshotThreshold = 8192
	s.SnapshotInterval = time.Hour
	s.NoSnapshotOnClose = true
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	er = executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		qr := queryRequestFromString("SELECT * FROM foo", false, false)
		qr.GetRequest().Statements[0].ForceStall = true

		blockingDB, err := db.Open(s.dbPath, false, true)
		if err != nil {
			t.Errorf("failed to open blocking DB connection: %s", err.Error())
		}
		defer blockingDB.Close()

		_, err = blockingDB.QueryWithContext(ctx, qr.GetRequest(), false)
		if err != nil {
			t.Errorf("failed to execute stalled query on blocking DB connection: %s", err.Error())
		}
	}()
	time.Sleep(1 * time.Second)

	er = executeRequestFromString(`INSERT INTO foo(name) VALUES("bob")`, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	if err := s.Snapshot(0); err == nil {
		t.Fatalf("expected error snapshotting single-node store with stalled query")
	}

	// Shutdown the blocking query so we can clean up. Windows in particular.
	cancelFunc()
	<-ctx.Done()
}

// Test_SingleNode_SnapshotFail_Blocked_Retry tests that a snapshot operation
// that requires a forced checkpoint and truncation does succeed once the
// blocking query unblocks.
func Test_SingleNode_SnapshotFail_Blocked_Retry(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	s.SnapshotThreshold = 8192
	s.SnapshotInterval = time.Hour
	s.NoSnapshotOnClose = true
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	er = executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		qr := queryRequestFromString("SELECT * FROM foo", false, false)
		qr.GetRequest().Statements[0].ForceStall = true

		blockingDB, err := db.Open(s.dbPath, false, true)
		if err != nil {
			t.Errorf("failed to open blocking DB connection: %s", err.Error())
		}
		defer blockingDB.Close()

		_, err = blockingDB.QueryWithContext(ctx, qr.GetRequest(), false)
		if err != nil {
			t.Errorf("failed to execute stalled query on blocking DB connection: %s", err.Error())
		}
	}()
	time.Sleep(1 * time.Second)

	success := false
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := s.Snapshot(0); err != nil {
			t.Errorf("failed to snapshot single-node store with released stalled query: %s", err.Error())
		} else {
			success = true
		}
	})
	time.Sleep(1 * time.Second)
	cancelFunc()
	wg.Wait()
	if !success {
		t.Fatalf("expected snapshot to succeed after blocking query released")
	}

	// Again, this time with a persistent snapshot.
	er = executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	ctx, cancelFunc = context.WithCancel(context.Background())
	go func() {
		qr := queryRequestFromString("SELECT * FROM foo", false, false)
		qr.GetRequest().Statements[0].ForceStall = true

		blockingDB, err := db.Open(s.dbPath, false, true)
		if err != nil {
			t.Errorf("failed to open blocking DB connection: %s", err.Error())
		}
		defer blockingDB.Close()

		_, err = blockingDB.QueryWithContext(ctx, qr.GetRequest(), false)
		if err != nil {
			t.Errorf("failed to execute stalled query on blocking DB connection: %s", err.Error())
		}
	}()
	time.Sleep(1 * time.Second)

	success = false
	var wg2 sync.WaitGroup
	wg2.Go(func() {
		if err := s.Snapshot(0); err != nil {
			t.Errorf("failed to snapshot single-node store with second released stalled query: %s", err.Error())
		} else {
			success = true
		}
	})
	time.Sleep(1 * time.Second)
	cancelFunc()
	wg2.Wait()
	if !success {
		t.Fatalf("expected snapshot to succeed after blocking query released")
	}
}

func Test_SingleNode_SnapshotWithAutoOptimize_Stress(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	s.SnapshotThreshold = 50
	s.SnapshotInterval = 100 * time.Millisecond
	s.AutoOptimizeInterval = 500 * time.Millisecond

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Create a table
	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Create an index on name
	er = executeRequestFromString(`CREATE INDEX foo_name ON foo(name)`, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Insert a bunch of data concurrently, putting some load on the Store.
	var wg sync.WaitGroup
	wg.Add(5)
	insertFn := func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_, _, err := s.Execute(context.Background(), executeRequestFromString(fmt.Sprintf(`INSERT INTO foo(name) VALUES("%s")`, random.String()), false, false))
			if err != nil {
				t.Errorf("failed to execute INSERT on single node: %s", err.Error())
			}
		}
	}
	for i := 0; i < 5; i++ {
		go insertFn()
	}
	wg.Wait()

	// Query the data, make sure it looks good after all this.
	qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, true)
	qr.Level = proto.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(context.Background(), qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2500]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Restart the Store, make sure all still looks good.
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close store: %s", err.Error())
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	r, _, _, err = s.Query(context.Background(), qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2500]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_SingleNode_DatabaseFileModified tests that a full snapshot is taken
// when the underlying database file is modified by some process external
// to the Store. Such changes are officially unsupported, but if the Store
// detects such a change, it will take a full snapshot to ensure the Snapshot
// remains consistent.
func Test_SingleNode_DatabaseFileModified(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)

	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Insert a record and trigger a snapshot to get a full snapshot.
	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}
	if s.numFullSnapshots != 1 {
		t.Fatalf("expected 1 full snapshot, got %d", s.numFullSnapshots)
	}

	insertSnap := func() {
		t.Helper()
		_, _, err := s.Execute(context.Background(), executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
		if err != nil {
			t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
		}
		if err := s.Snapshot(0); err != nil {
			t.Fatalf("failed to snapshot single-node store: %s", err.Error())
		}
	}

	// Insert a record, trigger a snapshot. It should be an incremental snapshot.
	insertSnap()
	if s.numFullSnapshots != 1 {
		t.Fatalf("expected 1 full snapshot, got %d", s.numFullSnapshots)
	}

	// Insert a record, trigger a snapshot. It shouldn't be a full snapshot.
	insertSnap()
	if s.numFullSnapshots != 1 {
		t.Fatalf("expected 1 full snapshot, got %d", s.numFullSnapshots)
	}

	lt, err := s.db.DBLastModified()
	if err != nil {
		t.Fatalf("failed to get last modified time of database: %s", err.Error())
	}

	// Touch the database file to make it newer than Store's record of last
	// modified time and then trigger a snapshot. It should be a full snapshot.
	if err := os.Chtimes(s.dbPath, time.Time{}, lt.Add(time.Second)); err != nil {
		t.Fatalf("failed to change database file times: %s", err.Error())
	}
	insertSnap()
	if s.numFullSnapshots != 2 {
		t.Fatalf("expected 2 full snapshots, got %d", s.numFullSnapshots)
	}

	// Insert a record, trigger a snapshot. We should be back to incremental snapshots.
	insertSnap()
	if s.numFullSnapshots != 2 {
		t.Fatalf("expected 2 full snapshots, got %d", s.numFullSnapshots)
	}

	// Modify just the access time, and trigger a snapshot. It should still be
	// an incremental snapshot.
	lt, err = s.db.DBLastModified()
	if err != nil {
		t.Fatalf("failed to get last modified time of database: %s", err.Error())
	}
	if err := os.Chtimes(s.dbPath, lt.Add(time.Second), time.Time{}); err != nil {
		t.Fatalf("failed to change database file times: %s", err.Error())
	}
	insertSnap()
	if s.numFullSnapshots != 2 {
		t.Fatalf("expected 2 full snapshots, got %d", s.numFullSnapshots)
	}

	// Just a final check...
	if s.numSnapshots.Load() != 6 {
		t.Fatalf("expected 6 snapshots in total, got %d", s.numSnapshots.Load())
	}
}

func Test_SingleNodeDBAppliedIndex_SnapshotRestart(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	// Open the store, ensure DBAppliedIndex is at initial value.
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	if got, exp := s.DBAppliedIndex(), uint64(0); exp != got {
		t.Fatalf("wrong DB applied index, got: %d, exp %d", got, exp)
	}

	// Execute a command, and ensure DBAppliedIndex is updated.
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
	}, false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if got, exp := s.DBAppliedIndex(), uint64(3); exp != got {
		t.Fatalf("wrong DB applied index, got: %d, exp %d", got, exp)
	}

	// Snapshot the Store.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot store: %s", err.Error())
	}

	// Restart the node, and ensure DBAppliedIndex is set to the correct value even
	// with a snapshot in place, and no log entries need to be replayed.
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	if got, exp := s.DBAppliedIndex(), uint64(3); exp != got {
		t.Fatalf("wrong DB applied index after restart, got: %d, exp %d", got, exp)
	}
}

// Test_SingleNodeSnapshot_FSM_ReleaseOnly tests that the Store responds correctly
// if Persist is never called, only Release. Because this is such a critically
// important piece of code, it looks into the internals of the Store during the
// test.
func Test_SingleNodeSnapshot_FSM_ReleaseOnly(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.NoSnapshotOnClose = true
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(name) VALUES("fiona")`,
	}
	_, _, err := s.Execute(context.Background(), executeRequestFromStrings(queries, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Snap the node.
	fsm := NewFSM(s)
	f, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("failed to snapshot node: %s", err.Error())
	}

	// Do nothing with the Snapshot, just release it, Store should remain in
	// FullNeeded more.
	f.Release()
	fn, err := s.snapshotStore.FullNeeded()
	if err != nil {
		t.Fatalf("failed to check FullNeeded: %s", err.Error())
	}
	if !fn {
		t.Fatal("expected Snapshot Store to require full snapshot")
	}

	// Next, successfully snapshot and insert more data.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot store: %s", err.Error())
	}
	queries = []string{
		`INSERT INTO foo(name) VALUES("fiona")`,
	}
	_, _, err = s.Execute(context.Background(), executeRequestFromStrings(queries, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	fn, err = s.snapshotStore.FullNeeded()
	if err != nil {
		t.Fatalf("failed to check FullNeeded: %s", err.Error())
	}
	if fn {
		t.Fatal("expected Snapshot Store to not require full snapshot")
	}

	// Snap the node again.
	f, err = fsm.Snapshot()
	if err != nil {
		t.Fatalf("failed to snapshot node: %s", err.Error())
	}

	// Confirm staged WALs exist.
	wals, err := s.StagedWALs()
	if err != nil {
		t.Fatalf("failed to get staged WALs: %s", err.Error())
	}
	if exp, got := 1, len(wals); exp != got {
		t.Fatalf("unexpected number of staged WALs\nexp: %d\ngot: %d", exp, got)
	}

	// Do nothing with the Snapshot, just release it, Store should keep the
	// Staged WALs intact.
	f.Release()
	wals, err = s.StagedWALs()
	if err != nil {
		t.Fatalf("failed to get staged WALs: %s", err.Error())
	}
	if exp, got := 1, len(wals); exp != got {
		t.Fatalf("unexpected number of staged WALs\nexp: %d\ngot: %d", exp, got)
	}

	queries = []string{
		`INSERT INTO foo(name) VALUES("fiona")`,
	}
	_, _, err = s.Execute(context.Background(), executeRequestFromStrings(queries, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Snap the node again, this time have the Sink return an error.
	f, err = fsm.Snapshot()
	if err != nil {
		t.Fatalf("failed to snapshot node: %s", err.Error())
	}

	snapDir := t.TempDir()
	snapFile, err := os.Create(filepath.Join(snapDir, "snapshot"))
	if err != nil {
		t.Fatalf("failed to create snapshot file: %s", err.Error())
	}
	defer snapFile.Close()
	sink := &mockSnapshotSink{snapFile, fmt.Errorf("mock write error")}
	if err := f.Persist(sink); err == nil {
		t.Fatalf("expected error when persisting snapshot to disk, got nil")
	}

	// Release it, check that we have the right number of WALs staged.
	f.Release()
	wals, err = s.StagedWALs()
	if err != nil {
		t.Fatalf("failed to get staged WALs: %s", err.Error())
	}
	if exp, got := 2, len(wals); exp != got {
		t.Fatalf("unexpected number of staged WALs\nexp: %d\ngot: %d", exp, got)
	}

	// Finish by successfully snapping the node.
	queries = []string{
		`INSERT INTO foo(name) VALUES("fiona")`,
	}
	_, _, err = s.Execute(context.Background(), executeRequestFromStrings(queries, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot store: %s", err.Error())
	}

	rows, _, _, err := s.Query(context.Background(), queryRequestFromString("SELECT COUNT(*) FROM foo", false, false))
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[4]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Now remove the "clean snapshot" marker so that the node will restore from the
	// Snapshot we just took, and ensure the data is still correct after restoration.
	if err := os.Remove(s.cleanSnapshotPath); err != nil {
		t.Fatalf("failed to remove clean snapshot marker: %s", err.Error())
	}

	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	query := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
	query.Level = proto.ConsistencyLevel_STRONG
	rows, _, _, err = s.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[4]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}
