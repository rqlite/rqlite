package store

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v8/command/proto"
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
	_, err := s.Execute(er)
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
		qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
		r, err := s.Query(qr)
		return err == nil && asJSON(r) == `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]`
	}, 100*time.Millisecond, 5*time.Second)
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Re-test adding a snapshot to the mix.
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to take user-requested snapshot: %s", err.Error())
	}
	queryTest := func(s *Store, c int) {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
		qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
		r, err := s.Query(qr)
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
		if _, err := s.Execute(er); err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	queryTest(s, 10)

	// This next block tests an important code path -- that a is taken, and then
	// the snapshot proof which is in the Snapshot store represents just the
	// database file. So we want to hang onto the database file, but delete
	// the WAL file that will be left around. Otherwise we'd have the main
	// database file and a WAL file, and the Raft log entries would be replayed
	// giving us a extra records.
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
		qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
		r, err := s.Query(qr)
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
	_, err = s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	testPoll(t, func() bool {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
		qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
		r, err := s.Query(qr)
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
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Write a bunch of rows, ensure they are all there.
	for i := 0; i < 1000; i++ {
		er := executeRequestFromString(
			fmt.Sprintf(`INSERT INTO foo(name) VALUES("fiona-%d")`, i),
			false, false)
		_, err := s.Execute(er)
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err := s.Query(qr)
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
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
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, err = s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}
