package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/rsum"
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
	_, _, err := s.Execute(context.Background(), er)
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
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false, false)
		qr.Level = command.ConsistencyLevel_STRONG
		r, _, _, err := s.Query(context.Background(), qr)
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

	// Ensures there is something to snapshot.
	er = executeRequestFromStrings([]string{
		`UPDATE foo SET name='fiona-updated' WHERE id=1`,
	}, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to take user-requested snapshot: %s", err.Error())
	}
	// Insert new records so we have something to snapshot.
	queryTest := func(s *Store, c int) {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false, false)
		qr.Level = command.ConsistencyLevel_STRONG
		r, _, _, err := s.Query(context.Background(), qr)
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
	for range 9 {
		er := executeRequestFromStrings([]string{
			`INSERT INTO foo(name) VALUES("fiona")`,
		}, false, false)
		if _, _, err := s.Execute(context.Background(), er); err != nil {
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

	// Write data so there is WAL data to snapshot.
	er = executeRequestFromStrings([]string{
		`INSERT INTO foo(name) VALUES("snapshot-trigger")`,
	}, false, false)
	_, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

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
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false, false)
		qr.Level = command.ConsistencyLevel_NONE
		r, _, _, err := s.Query(context.Background(), qr)
		return err == nil && asJSON(r) == `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[11]]}]`
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
	_, _, err = s.Execute(context.Background(), executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	testPoll(t, func() bool {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false, false)
		qr.Level = command.ConsistencyLevel_NONE
		r, _, _, err := s.Query(context.Background(), qr)
		return err == nil && asJSON(r) == `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[12]]}]`
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
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Write a bunch of rows, ensure they are all there.
	for i := range 1000 {
		er := executeRequestFromString(
			fmt.Sprintf(`INSERT INTO foo(name) VALUES("fiona-%d")`, i),
			false, false)
		_, _, err := s.Execute(context.Background(), er)
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(context.Background(), qr)
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

	qr = queryRequestFromString("SELECT COUNT(*) FROM foo", false, false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err = s.Query(context.Background(), qr)
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

	err := s.Load(context.Background(), loadRequestFromFile(filepath.Join("testdata", "load.sqlite")))
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
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, _, err = s.Execute(context.Background(), executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
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

	qr := queryRequestFromString("SELECT * FROM foo", false, false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(context.Background(), qr)
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
		{
			name: "SQLiteModTime",
			tamperFn: func(t *testing.T, s *Store) {
				now := time.Now().Add(1 * time.Hour)
				if err := os.Chtimes(s.dbPath, now, now); err != nil {
					t.Fatalf("failed to change mod time of database file: %s", err.Error())
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
			_, _, err := s.Execute(context.Background(), er)
			if err != nil {
				t.Fatalf("failed to execute on single node: %s", err.Error())
			}
			_, _, err = s.Execute(context.Background(), executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
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

			qr := queryRequestFromString("SELECT * FROM foo", false, false, false)
			qr.Level = command.ConsistencyLevel_STRONG
			r, _, _, err := s.Query(context.Background(), qr)
			if err != nil {
				t.Fatalf("failed to query single node: %s", err.Error())
			}
			if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(r); exp != got {
				t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
			}

			if s.numSnapshotsStart.Load() != 1 {
				t.Fatalf("expected snapshot start count to be 1, got %d", s.numSnapshotsStart.Load())
			}
			if s.numSnapshotsSkipped.Load() != 0 {
				t.Fatalf("expected snapshot skipped count to be 0, got %d", s.numSnapshotsSkipped.Load())
			}
		})
	}
}

func Test_Store_RestoreNoCleanSnapshot_CRCNotExist(t *testing.T) {
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

	er := executeRequestFromString(
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Close the store, which will give us a snapshot on shutdown.
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Now remove the CRC32 from the fingerprint to simulate an older version.
	fp := &FileFingerprint{}
	if err := fp.ReadFromFile(s.cleanSnapshotPath); err != nil {
		t.Fatalf("failed to read clean snapshot fingerprint: %s", err.Error())
	}
	fp.CRC32 = 0
	if err := fp.WriteToFile(s.cleanSnapshotPath); err != nil {
		t.Fatalf("failed to write corrupted clean snapshot fingerprint: %s", err.Error())
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to re-open single-node store: %s", err.Error())
	}
	defer s.Close(true)
}

func Test_Store_RestoreNoCleanSnapshot_CRCBad(t *testing.T) {
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

	er := executeRequestFromString(
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Close the store, which will give us a snapshot on shutdown.
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Look inside the Store, and manually verify that the snapshot CRC32
	// is good.
	fp := &FileFingerprint{}
	if err := fp.ReadFromFile(s.cleanSnapshotPath); err != nil {
		t.Fatalf("failed to read clean snapshot fingerprint: %s", err.Error())
	}
	crc32, err := rsum.CRC32(s.dbPath)
	if err != nil {
		t.Fatalf("failed to calculate CRC32 of database file: %s", err.Error())
	}
	if fp.CRC32 != crc32 {
		t.Fatalf("expected CRC32 in fingerprint to match database file")
	}

	// Now corrupt the CRC32 in the fingerprint.
	fp.CRC32 ^= 0xFFFFFFFF
	if err := fp.WriteToFile(s.cleanSnapshotPath); err != nil {
		t.Fatalf("failed to write corrupted clean snapshot fingerprint: %s", err.Error())
	}

	// Set a handler to ensure the Store goroutine responds to the bad CRC32.
	ch := make(chan struct{})
	s.crcBadHandler = func(_, _ uint32) {
		close(ch)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to re-open single-node store: %s", err.Error())
	}
	defer s.Close(true)

	// Wait for the CRC bad handler to be invoked.
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for CRC bad handler to be invoked")
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
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, _, err = s.Execute(context.Background(), executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
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

	qr := queryRequestFromString("SELECT * FROM foo", false, false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(context.Background(), qr)
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
	_, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Take a snapshot so that restart will involve not restoring the snapshot,
	// but still replaying some log entries.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to take snapshot: %s", err.Error())
	}

	// Insert a Raft log entry after taking the snapshot.
	_, _, err = s.Execute(context.Background(), executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
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

	qr := queryRequestFromString("SELECT * FROM foo", false, false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(context.Background(), qr)
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

// Test_Store_RestoreSnapshotAheadOfDB tests that a node does not fast-restart
// with a SQLite file which is older than the newest snapshot in the Snapshot
// Store. See https://github.com/rqlite/rqlite/issues/2747.
//
// Raft's InstallSnapshot makes a received snapshot durable and visible -- by
// calling sink.Close() -- before handing it to the FSM to be restored. If that
// restore never happens, because the node dies in that window or because opening
// the snapshot fails, the Snapshot Store is left ahead of the SQLite file. The
// clean-snapshot fingerprint records only the size, mod time and CRC32 of the
// SQLite file, so it still matches, and on restart the node takes the fast-restart
// path and adopts the index and term of the newest snapshot in the Snapshot Store
// -- an index its database has never seen. Every row committed at or below that
// index is then invisible and, because Raft comes up with lastApplied already at
// that index, is never replayed.
func Test_Store_RestoreSnapshotAheadOfDB(t *testing.T) {
	// A node holding a row the node under test will never receive. Its FSM
	// snapshot stands in for the snapshot a Leader would install.
	src, srcLn := mustNewStore(t)
	defer srcLn.Close()
	src.NoSnapshotOnClose = true
	if err := src.Open(); err != nil {
		t.Fatalf("failed to open source store: %s", err.Error())
	}
	defer src.Close(true)
	if err := src.Bootstrap(NewServer(src.ID(), src.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap source store: %s", err.Error())
	}
	if _, err := src.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader on source store: %s", err)
	}
	mustExecute(t, src, []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "declan")`,
	})

	s, ln := mustNewStore(t)
	defer ln.Close()
	s.NoSnapshotOnClose = true
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	mustExecute(t, s, []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	})

	// Snapshot the node, which writes its clean-snapshot fingerprint. The SQLite
	// file and the newest snapshot in the Snapshot Store now agree with one another.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}
	metas, err := s.snapshotStore.List()
	if err != nil {
		t.Fatalf("failed to list snapshots: %s", err.Error())
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 snapshot in store, got %d", len(metas))
	}
	latest := metas[0]

	// Install a newer snapshot into the Snapshot Store, without restoring it into
	// the FSM. This leaves behind exactly what Raft's installSnapshot leaves behind
	// when sink.Close() returns but the FSM restore which should follow it does not
	// happen.
	installIdx := latest.Index + 100
	sink, err := s.snapshotStore.Create(latest.Version, installIdx, latest.Term,
		latest.Configuration, latest.ConfigurationIndex, nil)
	if err != nil {
		t.Fatalf("failed to create snapshot sink: %s", err.Error())
	}
	installSnap, err := NewFSM(src).Snapshot()
	if err != nil {
		t.Fatalf("failed to snapshot source node: %s", err.Error())
	}
	if err := installSnap.Persist(sink); err != nil {
		t.Fatalf("failed to persist installed snapshot: %s", err.Error())
	}
	installSnap.Release()
	if err := sink.Close(); err != nil {
		t.Fatalf("failed to close snapshot sink: %s", err.Error())
	}

	// Restart the node.
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to reopen single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// The node adopts installIdx as its FSM and database applied index, so every
	// row in the snapshot at that index must actually be in its database.
	if got, exp := s.DBAppliedIndex(), installIdx; got < exp {
		t.Fatalf("wrong DB applied index after restart, got: %d, exp at least %d", got, exp)
	}
	qr := queryRequestFromString("SELECT * FROM foo", false, false, false)
	qr.Level = command.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(context.Background(), qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	exp := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"declan"]]}]`
	if got := asJSON(r); exp != got {
		t.Fatalf("node is hiding rows committed at or below its own applied index of %d\nexp: %s\ngot: %s",
			installIdx, exp, got)
	}

	// The snapshot was never restored into the FSM, so the fast-restart path must
	// have been declined and a full restore performed instead.
	if exp, got := uint64(1), s.numSnapshotsStart.Load(); exp != got {
		t.Fatalf("expected snapshot start count to be %d, got %d", exp, got)
	}
	if exp, got := uint64(0), s.numSnapshotsSkipped.Load(); exp != got {
		t.Fatalf("expected snapshot skipped count to be %d, got %d", exp, got)
	}
}
