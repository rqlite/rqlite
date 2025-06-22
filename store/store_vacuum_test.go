package store

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/db"
	"github.com/rqlite/rqlite/v8/rarchive"
)

// Test_SingleNodeBackupBinary tests that requesting a binary-formatted
// backup works as expected.
func Test_SingleNodeBackupBinaryVacuum(t *testing.T) {
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

	dump := `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE foo (id integer not null primary key, name text);
INSERT INTO "foo" VALUES(1,'fiona');
COMMIT;
`
	_, _, err := s.Execute(executeRequestFromString(dump, false, false))
	if err != nil {
		t.Fatalf("failed to load simple dump: %s", err.Error())
	}

	checkDB := func(path string) {
		t.Helper()
		// Open the backup file using the DB layer and check the data.
		dstDB, err := db.Open(path, false, false)
		if err != nil {
			t.Fatalf("unable to open backup database, %s", err.Error())
		}
		defer dstDB.Close()
		qr := queryRequestFromString("SELECT * FROM foo", false, false)
		qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
		r, _, err := s.Query(qr)
		if err != nil {
			t.Fatalf("failed to query single node: %s", err.Error())
		}
		if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}

	/////////////////////////////////////////////////////////////////////
	// Vacuumed backup, database may be not be bit-for-bit identical, but
	// records should be OK.
	vf, err := os.CreateTemp("", "rqlite-baktest-")
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(vf.Name())
	defer vf.Close()
	if err := s.Backup(backupRequestBinary(true, true, false), vf); err != nil {
		t.Fatalf("Backup failed %s", err.Error())
	}
	checkDB(vf.Name())

	/////////////////////////////////////////////////////////////////////
	// Compressed and vacuumed backup.
	gzf, err := os.CreateTemp("", "rqlite-baktest-")
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(gzf.Name())
	defer gzf.Close()
	if err := s.Backup(backupRequestBinary(true, true, true), gzf); err != nil {
		t.Fatalf("Compressed backup failed %s", err.Error())
	}

	// Gzip decompress file to a new temp file
	guzf, err := rarchive.Gunzip(gzf.Name())
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(guzf)
	checkDB(guzf)
}

// Test_OpenStoreSingleNode_VacuumFullNeeded tests that running a VACUUM
// does not mean a full snapshot is needed.
func Test_OpenStoreSingleNode_VacuumFullNeeded(t *testing.T) {
	s, ln := mustNewStore(t)
	defer s.Close(true)
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
	_, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot store: %s", err.Error())
	}
	if fn, err := s.snapshotStore.FullNeeded(); err != nil {
		t.Fatalf("failed to determine full snapshot needed: %s", err.Error())
	} else if fn {
		t.Fatalf("full snapshot marked as needed")
	}
	if err := s.Vacuum(); err != nil {
		t.Fatalf("failed to vacuum database: %s", err.Error())
	}
	if fn, err := s.snapshotStore.FullNeeded(); err != nil {
		t.Fatalf("failed to determine full snapshot needed: %s", err.Error())
	} else if fn {
		t.Fatalf("full snapshot marked as needed")
	}
}

func Test_OpenStoreSingleNode_VacuumTimes(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer s0.Close(true)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	_, err := s0.LastVacuumTime()
	if err == nil {
		t.Fatal("expected error getting last vacuum time")
	}
	_, err = s0.getKeyTime(baseVacuumTimeKey)
	if err == nil {
		t.Fatal("expected error getting base time")
	}

	s1, ln1 := mustNewStore(t)
	s1.AutoVacInterval = time.Hour
	defer s1.Close(true)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}

	time.Sleep(100 * time.Millisecond)
	now := time.Now()
	_, err = s1.LastVacuumTime()
	if err == nil {
		t.Fatal("expected error getting last vacuum time")
	}
	bt, err := s1.getKeyTime(baseVacuumTimeKey)
	if err != nil {
		t.Fatalf("error getting base time: %s", err.Error())
	}

	if !bt.Before(now) {
		t.Fatal("expected last base time to be before now")
	}
}

func Test_SingleNodeExplicitVacuumOK(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	s.SnapshotThreshold = 65536
	s.SnapshotInterval = 1 * time.Hour
	s.SnapshotThresholdWALSize = 1024 * 1024 * 1024 // Ensure WAL size doesn't trigger.
	s.AutoVacInterval = 1 * time.Hour

	doVacuum := func() {
		er := executeRequestFromString(`VACUUM`, false, false)
		_, _, err := s.Execute(er)
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	doQuery := func() {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, true)
		qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
		r, _, err := s.Query(qr)
		if err != nil {
			t.Fatalf("failed to query single node: %s", err.Error())
		}
		if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[100]]}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}

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
	_, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	for i := 0; i < 100; i++ {
		_, _, err := s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
		if err != nil {
			t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
		}
	}

	// Do a snapshot, so that the next snapshot will have to deal with a WAL that has been
	// modified by the upcoming VACUUM.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}

	// VACUUM, and then query the data, make sure it looks good.
	doVacuum()
	doQuery()
	if fn, err := s.snapshotStore.FullNeeded(); err != nil {
		t.Fatalf("failed to check if snapshot store needs a full snapshot: %s", err.Error())
	} else if fn {
		t.Fatalf("expected snapshot store to not need a full snapshot post explicit VACUUM")
	}

	// The next snapshot will be incremental.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}
	doQuery()

	// Restart node
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
	doQuery()
}

func Test_SingleNodeExplicitVacuumOK_Stress(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	s.SnapshotThreshold = 50
	s.SnapshotInterval = 100 * time.Millisecond
	s.AutoVacInterval = 1 * time.Hour

	doVacuum := func() {
		er := executeRequestFromString(`VACUUM`, false, false)
		_, _, err := s.Execute(er)
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	doQuery := func() {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, true)
		qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
		r, _, err := s.Query(qr)
		if err != nil {
			t.Fatalf("failed to query single node: %s", err.Error())
		}
		if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2500]]}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}

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
	_, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Insert a bunch of data concurrently, and do VACUUMs; putting some load on the Store.
	var wg sync.WaitGroup
	wg.Add(6)
	insertFn := func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_, _, err := s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
			if err != nil {
				t.Errorf("failed to execute INSERT on single node: %s", err.Error())
			}
		}
	}
	for i := 0; i < 5; i++ {
		go insertFn()
	}
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			doVacuum()
			time.Sleep(time.Second)
		}
	}()

	wg.Wait()
	if s.WaitForAllApplied(5*time.Second) != nil {
		t.Fatalf("failed to wait for all data to be applied")
	}

	// Query the data, make sure it looks good after all this.
	doQuery()

	// Restart the Store, make sure it still works.
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
	doQuery()
}

// Test_SingleNode_SnapshotWithAutoVac tests that a Store correctly operates
// when performing both Snapshots and Auto-Vacuums.
func Test_SingleNode_SnapshotWithAutoVac(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	s.SnapshotThreshold = 8192 // Ensures Snapshot not triggered during testing.
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

	// Create a table, and insert some data.
	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	for i := 0; i < 100; i++ {
		_, _, err := s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
		if err != nil {
			t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
		}
	}

	// Force an initial snapshot, shouldn't need a full snapshot afterwards
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}
	if fn, err := s.snapshotStore.FullNeeded(); err != nil {
		t.Fatalf("failed to check if snapshot store needs a full snapshot: %s", err.Error())
	} else if fn {
		t.Fatalf("expected snapshot store to not need a full snapshot")
	}

	// Enable auto-vacuuming. Need to go under the covers to init the vacuum times.
	s.AutoVacInterval = 1 * time.Nanosecond
	if err := s.initVacuumTime(); err != nil {
		t.Fatalf("failed to initialize vacuum time: %s", err.Error())
	}
	time.Sleep(1 * time.Second)
	if n, err := s.autoVacNeeded(time.Now()); err != nil {
		t.Fatalf("failed to check if auto-vacuum is needed: %s", err.Error())
	} else if !n {
		t.Fatalf("expected auto-vacuum to be needed")
	}

	// Force another snapshot, this time a VACUUM should be triggered.
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}
	if exp, got := 1, s.numAutoVacuums; exp != got {
		t.Fatalf("expected %d auto-vacuums, got %d", exp, got)
	}

	s.AutoVacInterval = 1 * time.Hour
	if n, err := s.autoVacNeeded(time.Now()); err != nil {
		t.Fatalf("failed to check if auto-vacuum is needed: %s", err.Error())
	} else if n {
		t.Fatalf("auto-vacuum should not be needed")
	}

	// Query the data, make sure it looks good after all this.
	qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, true)
	r, _, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[100]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Make sure the node works across a restart
	preAVT, err := s.LastVacuumTime()
	if err != nil {
		t.Fatalf("failed to get last vacuum time: %s", err.Error())
	}

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

	postAVT, err := s.LastVacuumTime()
	if err != nil {
		t.Fatalf("failed to get last vacuum time: %s", err.Error())
	}
	if preAVT != postAVT {
		t.Fatalf("expected last vacuum time to be the same across restarts")
	}
	if n, err := s.autoVacNeeded(time.Now()); err != nil {
		t.Fatalf("failed to check if auto-vacuum is needed: %s", err.Error())
	} else if n {
		t.Fatalf("auto-vacuum should not be needed")
	}

	// Query the data again, make sure it still looks good after all this.
	r, _, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[100]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNode_SnapshotWithAutoVac_Stress(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	s.SnapshotThreshold = 50
	s.SnapshotInterval = 100 * time.Millisecond
	s.AutoVacInterval = 500 * time.Millisecond

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
	_, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Insert a bunch of data concurrently, putting some load on the Store.
	var wg sync.WaitGroup
	wg.Add(5)
	insertFn := func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_, _, err := s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
			if err != nil {
				t.Errorf("failed to execute INSERT on single node: %s", err.Error())
			}
		}
	}
	for i := 0; i < 5; i++ {
		go insertFn()
	}
	wg.Wait()
	if s.WaitForAllApplied(5*time.Second) != nil {
		t.Fatalf("failed to wait for all data to be applied")
	}

	// Query the data, make sure it looks good after all this.
	qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, _, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2500]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Restart the Store, make sure it still works.
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
	r, _, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2500]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}
