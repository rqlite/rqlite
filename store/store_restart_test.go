package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v9/command/proto"
)

// Test_OpenStoreCloseStartupSingleNode tests various restart scenarios.
func Test_OpenStoreCloseStartupSingleNode(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Reopen it and confirm data still there.
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	testPoll(t, func() bool {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
		qr.Level = command.ConsistencyLevel_STRONG
		r, _, _, err := s.Query(qr)
		return err == nil && asJSON(r) == `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]`
	}, 100*time.Millisecond, 5*time.Second)
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Confirm we started by not restoring from the snapshot.
	if s.numSnapshotsStart.Load() != 0 {
		t.Fatalf("expected snapshot start count to be 0, got %d", s.numSnapshotsStart.Load())
	}
	if s.numSnapshotsSkipped.Load() != 1 {
		t.Fatalf("expected snapshot skipped count to be 1, got %d", s.numSnapshotsSkipped.Load())
	}

	// Re-test adding an explicit snapshot to the mix. Not entirely necessary, since
	// we snapshot previously on close anyway.
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	mustNoop(s, "1234") // Ensures there is something to snapshot.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to take user-requested snapshot: %s", err.Error())
	}
	// Insert new records so we have something to snapshot.
	queryTest := func(s *Store, c int) {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
		qr.Level = command.ConsistencyLevel_STRONG
		r, _, _, err := s.Query(qr)
		if err != nil {
			t.Fatalf("failed to query single node: %s", err.Error())
		}
		if exp, got := `["COUNT(*)"]`, asJSON(r[0].Columns); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := fmt.Sprintf(`[[%d]]`, c), asJSON(r[0].Values); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
	for i := 0; i < 9; i++ {
		er := executeRequestFromStrings([]string{
			`INSERT INTO foo(name) VALUES("fiona")`,
		}, false, false)
		if _, _, err := s.Execute(er); err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	queryTest(s, 10)

	// This next block tests that everything works when there is a combination
	// of snapshot data and some entries in the log that need to be replayed
	// af start-up.
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	queryTest(s, 10)

	// Trigger another snapshot.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to take user-requested snapshot: %s", err.Error())
	}

	// Close and re-open to make sure all data is there after starting up
	// with a snapshot.
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	testPoll(t, func() bool {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
		qr.Level = command.ConsistencyLevel_NONE
		r, _, _, err := s.Query(qr)
		return err == nil && asJSON(r) == `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[10]]}]`
	}, 100*time.Millisecond, 5*time.Second)
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Write one more record, and then reopen again, ensure all data is there.
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	_, _, err = s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	testPoll(t, func() bool {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
		qr.Level = command.ConsistencyLevel_NONE
		r, _, _, err := s.Query(qr)
		return err == nil && asJSON(r) == `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[11]]}]`
	}, 100*time.Millisecond, 5*time.Second)

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
}

func test_SnapshotStress(t *testing.T, s *Store) {
	s.SnapshotInterval = 100 * time.Millisecond
	s.SnapshotThreshold = 4

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromString(
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Write a bunch of rows, ensure they are all there.
	for i := 0; i < 1000; i++ {
		er := executeRequestFromString(
			fmt.Sprintf(`INSERT INTO foo(name) VALUES("fiona-%d")`, i),
			false, false)
		_, _, err := s.Execute(er)
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1000]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Close and re-open to make sure all data is there recovering from snapshot.
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

	qr = queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1000]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_StoreSnapshotStressSingleNode tests that a high-rate of snapshotting
// works fine with an on-disk setup.
func Test_StoreSnapshotStressSingleNode(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	test_SnapshotStress(t, s)
}

func Test_StoreLoad_Restart(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	err := s.Load(loadRequestFromFile(filepath.Join("testdata", "load.sqlite")))
	if err != nil {
		t.Fatalf("failed to load: %s", err.Error())
	}

	// Check store can be re-opened.
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
}

// Test_OpenStoreCloseUserSnapshot tests that user-requested snapshots
// work fine.
func Test_OpenStoreCloseUserSnapshot(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromString(
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, _, err = s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Take a snapshot.
	if err := s.Snapshot(1); err != nil {
		t.Fatalf("failed to take user-requested snapshot: %s", err.Error())
	}

	// Check store can be re-opened and has the correct data.
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

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_Store_RestoreNoCleanSnapshot tests that a full restore from snapshot on open works correctly
// under various conditions that should trigger it.
func Test_Store_RestoreNoCleanSnapshot(t *testing.T) {
	testCases := []struct {
		name     string
		tamperFn func(t *testing.T, s *Store)
	}{
		{
			name: "NoCleanSnapshot",
			tamperFn: func(t *testing.T, s *Store) {
				// Remove clean snapshot marker to force a full restore.
				if err := os.Remove(s.cleanSnapshotPath); err != nil {
					t.Fatalf("failed to remove clean snapshot during testing: %s", err.Error())
				}
			},
		},
		{
			name: "CorruptCleanSnapshot",
			tamperFn: func(t *testing.T, s *Store) {
				// Corrupt the clean snapshot marker to force a full restore.
				f, err := os.OpenFile(s.cleanSnapshotPath, os.O_WRONLY, 0644)
				if err != nil {
					t.Fatalf("failed to open clean snapshot during testing: %s", err.Error())
				}
				defer f.Close()
				if _, err := f.Write([]byte("FOOBAR")); err != nil {
					t.Fatalf("failed to corrupt clean snapshot: %s", err.Error())
				}
			},
		},
		{
			name: "SQLiteBad",
			tamperFn: func(t *testing.T, s *Store) {
				// Modify the SQLite file so it doesn't match the clean snapshot expectations.
				f, err := os.OpenFile(s.dbPath, os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					t.Fatalf("failed to open snapshot during testing: %s", err.Error())
				}
				defer f.Close()
				if _, err := f.Write([]byte("CORRUPT")); err != nil {
					t.Fatalf("failed to corrupt database file: %s", err.Error())
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s, ln := mustNewStore(t)
			defer ln.Close()
			if err := s.Open(); err != nil {
				t.Fatalf("failed to open single-node store: %s", err.Error())
			}
			if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
				t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
			}
			defer s.Close(true)
			if _, err := s.WaitForLeader(10 * time.Second); err != nil {
				t.Fatalf("Error waiting for leader: %s", err)
			}

			er := executeRequestFromString(
				`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
				false, false)
			_, _, err := s.Execute(er)
			if err != nil {
				t.Fatalf("failed to execute on single node: %s", err.Error())
			}
			_, _, err = s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
			if err != nil {
				t.Fatalf("failed to execute on single node: %s", err.Error())
			}

			if err := s.Close(true); err != nil {
				t.Fatalf("failed to close single-node store: %s", err.Error())
			}

			tc.tamperFn(t, s)

			if err := s.Open(); err != nil {
				t.Fatalf("failed to open single-node store: %s", err.Error())
			}
			defer s.Close(true)
			if _, err := s.WaitForLeader(10 * time.Second); err != nil {
				t.Fatalf("Error waiting for leader: %s", err)
			}

			qr := queryRequestFromString("SELECT * FROM foo", false, false)
			qr.Level = command.ConsistencyLevel_STRONG
			r, _, _, err := s.Query(qr)
			if err != nil {
				t.Fatalf("failed to query single node: %s", err.Error())
			}
			if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(r); exp != got {
				t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
			}

			if s.numSnapshotsStart.Load() != 1 {
				t.Fatalf("expected snapshot start count to be 2, got %d", s.numSnapshotsStart.Load())
			}
			if s.numSnapshotsSkipped.Load() != 0 {
				t.Fatalf("expected snapshot skipped count to be 0, got %d", s.numSnapshotsSkipped.Load())
			}
		})
	}
}

// Test_Store_Restore_NoSnapshotOnClose tests that when no snapshot takes place
// on close, restore still works from just the Raft log. This tests a node shutting
// down uncleanly.
func Test_Store_Restore_NoSnapshotOnClose(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	s.NoSnapshotOnClose = true
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromString(
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, _, err = s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
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

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_Store_Restore_NoSnapshotOnClose_Snapshot(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	s.NoSnapshotOnClose = true
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromString(
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Take a snapshot so that restart will involve not restoring the snapshot,
	// but still replaying some log entries.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to take snapshot: %s", err.Error())
	}

	// Insert a Raft log entry after taking the snapshot.
	_, _, err = s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
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

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	if s.numSnapshotsStart.Load() != 0 {
		t.Fatalf("expected snapshot start count to be 0, got %d", s.numSnapshotsStart.Load())
	}
	if s.numSnapshotsSkipped.Load() != 1 {
		t.Fatalf("expected snapshot skipped count to be 1")
	}
}
