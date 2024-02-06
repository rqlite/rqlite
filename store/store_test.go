package store

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/encoding"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/db"
	"github.com/rqlite/rqlite/v8/random"
	"github.com/rqlite/rqlite/v8/testdata/chinook"
)

// Test_StoreSingleNode tests that a single node basically operates.
func Test_OpenStoreSingleNode(t *testing.T) {
	s, ln := mustNewStore(t)
	defer s.Close(true)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}

	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	_, err = s.LeaderAddr()
	if err != nil {
		t.Fatalf("failed to get leader address: %s", err.Error())
	}
	id, err := waitForLeaderID(s, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to retrieve leader ID: %s", err.Error())
	}
	if got, exp := id, s.raftID; got != exp {
		t.Fatalf("wrong leader ID returned, got: %s, exp %s", got, exp)
	}
}

// Test_SingleNodeSQLitePath ensures that basic functionality works when the SQLite database path
// is explicitly specificed.
func Test_SingleNodeOnDiskSQLitePath(t *testing.T) {
	s, ln, path := mustNewStoreSQLitePath(t)
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
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Confirm SQLite file was actually created at supplied path.
	if !pathExists(path) {
		t.Fatalf("SQLite file does not exist at %s", path)
	}
}

func Test_SingleNodeDBAppliedIndex(t *testing.T) {
	s, ln, _ := mustNewStoreSQLitePath(t)
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
	if exp, got := s.DBAppliedIndex(), uint64(0); exp != got {
		t.Fatalf("wrong DB applied index, got: %d, exp %d", got, exp)
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
	}, false, false)
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if exp, got := s.DBAppliedIndex(), uint64(3); exp != got {
		t.Fatalf("wrong DB applied index, got: %d, exp %d", got, exp)
	}
	er = executeRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if exp, got := s.DBAppliedIndex(), uint64(4); exp != got {
		t.Fatalf("wrong DB applied index, got: %d, exp %d", got, exp)
	}
	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	_, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := s.DBAppliedIndex(), uint64(4); exp != got {
		t.Fatalf("wrong DB applied index, got: %d, exp %d", got, exp)
	}
}

func Test_SingleNodeTempFileCleanup(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	// Create temporary files in the Store directory.
	for _, pattern := range []string{
		restoreScratchPattern,
		backupScatchPattern,
		bootScatchPattern,
	} {
		f, err := os.CreateTemp(s.dbDir, pattern)
		if err != nil {
			t.Fatalf("failed to create temporary file: %s", err.Error())
		}
		f.Close()
	}

	// Open the Store, which should clean up the temporary files.
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)

	// Confirm temporary files have been cleaned up.
	for _, pattern := range []string{
		restoreScratchPattern,
		backupScatchPattern,
		bootScatchPattern,
	} {
		matches, err := filepath.Glob(filepath.Join(s.dbDir, pattern))
		if err != nil {
			t.Fatalf("failed to glob temporary files: %s", err.Error())
		}
		if len(matches) != 0 {
			t.Fatalf("temporary files not cleaned up: %s", matches)
		}
	}
}

// Test_SingleNodeBackupBinary tests that requesting a binary-formatted
// backup works as expected.
func Test_SingleNodeBackupBinary(t *testing.T) {
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
	_, err := s.Execute(executeRequestFromString(dump, false, false))
	if err != nil {
		t.Fatalf("failed to load simple dump: %s", err.Error())
	}

	/////////////////////////////////////////////////////////////////////
	// Non-vacuumed backup, database should be bit-for-bit identical.
	f, err := os.CreateTemp("", "rqlite-baktest-")
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := s.Backup(backupRequestBinary(true, false, false), f); err != nil {
		t.Fatalf("Backup failed %s", err.Error())
	}
	if !filesIdentical(f.Name(), s.dbPath) {
		t.Fatalf("backup file not identical to database file")
	}

	/////////////////////////////////////////////////////////////////////
	// Compressed backup.
	gzf, err := os.CreateTemp("", "rqlite-baktest-")
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(gzf.Name())
	defer gzf.Close()
	if err := s.Backup(backupRequestBinary(true, false, true), gzf); err != nil {
		t.Fatalf("Compressed backup failed %s", err.Error())
	}

	// Gzip decompress file to a new temp file
	guzf, err := os.CreateTemp("", "rqlite-baktest-")
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(guzf.Name())
	defer guzf.Close()
	if err := gunzipFile(guzf, gzf); err != nil {
		t.Fatalf("Failed to gunzip backup file %s", err.Error())
	}
	if !filesIdentical(guzf.Name(), s.dbPath) {
		t.Fatalf("backup file not identical to database file")
	}
}

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
	_, err := s.Execute(executeRequestFromString(dump, false, false))
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
		r, err := s.Query(qr)
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
	guzf, err := os.CreateTemp("", "rqlite-baktest-")
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(guzf.Name())
	defer guzf.Close()
	if err := gunzipFile(guzf, gzf); err != nil {
		t.Fatalf("Failed to gunzip backup file %s", err.Error())
	}
	checkDB(guzf.Name())
}

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
	_, err := s.Execute(executeRequestFromStrings(queries, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	_, err = s.Query(queryRequestFromString("SELECT * FROM foo", false, false))
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
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
	sink := &mockSnapshotSink{snapFile}
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
	r, err := s.Query(queryRequestFromString("SELECT * FROM foo", false, false))
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

// Test_StoreSingleNodeNotOpen tests that various methods called on a
// closed Store return ErrNotOpen.
func Test_StoreSingleNodeNotOpen(t *testing.T) {
	s, ln := mustNewStore(t)
	defer s.Close(true)
	defer ln.Close()

	a, err := s.LeaderAddr()
	if err != nil {
		t.Fatalf("failed to get leader address: %s", err.Error())
	}
	if a != "" {
		t.Fatalf("non-empty Leader return for non-open store: %s", a)
	}

	_, err = s.WaitForLeader(1 * time.Second)
	if err != ErrWaitForLeaderTimeout {
		t.Fatalf("wrong wait-for-leader error received for non-open store: %s", err)
	}

	if _, err := s.Stats(); err != nil {
		t.Fatalf("stats fetch returned error for non-open store: %s", err)
	}

	// Check key methods handle being called when Store is not open.

	if err := s.Join(joinRequest("id", "localhost", true)); err != ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if err := s.Notify(notifyRequest("id", "localhost")); err != ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if err := s.Remove(nil); err != ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if _, err := s.Nodes(); err != ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if err := s.Backup(nil, nil); err != ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}

	if _, err := s.Execute(nil); err != ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if _, err := s.Query(nil); err != ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
	if err := s.Load(nil); err != ErrNotOpen {
		t.Fatalf("wrong error received for non-open store: %s", err)
	}
}

// Test_OpenStoreCloseSingleNode tests a single node store can be
// opened, closed, and then reopened.
func Test_OpenStoreCloseSingleNode(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if !s.open {
		t.Fatalf("store not marked as open")
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

	fsmIdx, err := s.WaitForAppliedFSM(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for fsmIndex: %s", err.Error())
	}

	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}
	if s.open {
		t.Fatalf("store still marked as open")
	}

	// Reopen it and confirm data still there.
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Wait until the log entries have been applied to the voting follower,
	// and then query.
	if _, err := s.WaitForFSMIndex(fsmIdx, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}
}

// Test_StoreLeaderObservation tests observing leader changes works.
func Test_StoreLeaderObservation(t *testing.T) {
	s, ln := mustNewStore(t)
	defer s.Close(true)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}

	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	countCh := make(chan int, 2)
	s.RegisterLeaderChange(ch1)
	s.RegisterLeaderChange(ch2)

	go func() {
		<-ch1
		countCh <- 1
	}()
	go func() {
		<-ch2
		countCh <- 2
	}()

	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}

	count := 0
	for {
		select {
		case <-countCh:
			count++
			if count == 2 {
				return
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for all observations")
		}
	}
}

// Test_StoreReady tests that the Store correctly implements the Ready method.
func Test_StoreReady(t *testing.T) {
	s, ln := mustNewStore(t)
	defer s.Close(true)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	if !s.Ready() {
		t.Fatalf("store not marked as ready even though Leader is set")
	}

	ch1 := make(chan struct{})
	s.RegisterReadyChannel(ch1)
	if s.Ready() {
		t.Fatalf("store marked as ready even though registered channel is open")
	}
	close(ch1)
	testPoll(t, s.Ready, 100*time.Millisecond, 2*time.Second)

	ch2 := make(chan struct{})
	s.RegisterReadyChannel(ch2)
	ch3 := make(chan struct{})
	s.RegisterReadyChannel(ch3)
	if s.Ready() {
		t.Fatalf("store marked as ready even though registered channels are open")
	}
	close(ch2)
	close(ch3)
	testPoll(t, s.Ready, 100*time.Millisecond, 2*time.Second)
}

// Test_SingleNodeExecuteQuery tests that a Store correctly responds to a simple
// Execute and Query request.
func Test_SingleNodeExecuteQuery(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err := s.Query(qr)
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

// Test_SingleNodeExecuteQueryFail ensures database level errors are presented by the store.
func Test_SingleNodeExecuteQueryFail(t *testing.T) {
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

	er := executeRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	r, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if exp, got := "no such table: foo", r[0].Error; exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_SingleNodeExecuteQueryTx tests that a Store correctly responds to a simple
// Execute and Query request, when the request is wrapped in a transaction.
func Test_SingleNodeExecuteQueryTx(t *testing.T) {
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

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, true)
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, true)
	var r []*proto.QueryRows

	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	_, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}

	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	_, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}

	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
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

// Test_SingleNodeRequest tests simple requests that contain both
// queries and execute statements.
func Test_SingleNodeRequest(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	tests := []struct {
		stmts       []string
		expected    string
		associative bool
	}{
		{
			stmts:    []string{},
			expected: `[]`,
		},
		{
			stmts: []string{
				`SELECT * FROM foo`,
			},
			expected: `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`,
		},
		{
			stmts: []string{
				`INSERT INTO foo(id, name) VALUES(1234, "dana")`,
				`INSERT INTO foo(id, name) VALUES(5678, "bob")`,
			},
			expected: `[{"last_insert_id":1234,"rows_affected":1},{"last_insert_id":5678,"rows_affected":1}]`,
		},
		{
			stmts: []string{
				`SELECT * FROM foo WHERE name='fiona'`,
				`INSERT INTO foo(id, name) VALUES(66, "declan")`,
			},
			expected: `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]},{"last_insert_id":66,"rows_affected":1}]`,
		},
		{
			stmts: []string{
				`INSERT INTO foo(id, name) VALUES(77, "fiona")`,
				`SELECT COUNT(*) FROM foo`,
			},
			expected: `[{"last_insert_id":77,"rows_affected":1},{"columns":["COUNT(*)"],"types":["integer"],"values":[[5]]}]`,
		},
		{
			stmts: []string{
				`INSERT INTO foo(id, name) VALUES(88, "fiona")`,
				`nonsense SQL`,
				`SELECT COUNT(*) FROM foo WHERE name='fiona'`,
				`SELECT * FROM foo WHERE name='declan'`,
			},
			expected:    `[{"last_insert_id":88,"rows_affected":1,"rows":null},{"error":"near \"nonsense\": syntax error"},{"types":{"COUNT(*)":"integer"},"rows":[{"COUNT(*)":3}]},{"types":{"id":"integer","name":"text"},"rows":[{"id":66,"name":"declan"}]}]`,
			associative: true,
		},
	}

	for _, tt := range tests {
		eqr := executeQueryRequestFromStrings(tt.stmts, proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, false, false)
		r, err := s.Request(eqr)
		if err != nil {
			t.Fatalf("failed to execute request on single node: %s", err.Error())
		}

		var exp string
		var got string
		if tt.associative {
			exp, got = tt.expected, asJSONAssociative(r)
		} else {
			exp, got = tt.expected, asJSON(r)
		}
		if exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
}

// Test_SingleNodeRequest tests simple requests that contain both
// queries and execute statements, when wrapped in a transaction.
func Test_SingleNodeRequestTx(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
	}, false, false)
	_, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	tests := []struct {
		stmts    []string
		expected string
		tx       bool
	}{
		{
			stmts:    []string{},
			expected: `[]`,
		},
		{
			stmts: []string{
				`INSERT INTO foo(id, name) VALUES(1, "declan")`,
				`SELECT * FROM foo`,
			},
			expected: `[{"last_insert_id":1,"rows_affected":1},{"columns":["id","name"],"types":["integer","text"],"values":[[1,"declan"]]}]`,
		},
		{
			stmts: []string{
				`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
				`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
				`SELECT COUNT(*) FROM foo`,
			},
			expected: `[{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"}]`,
			tx:       true,
		},
		{
			// Since the above transaction should be rolled back, there will be only one row in the table.
			stmts: []string{
				`SELECT COUNT(*) FROM foo`,
			},
			expected: `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]`,
		},
	}

	for _, tt := range tests {
		eqr := executeQueryRequestFromStrings(tt.stmts, proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, false, tt.tx)
		r, err := s.Request(eqr)
		if err != nil {
			t.Fatalf("failed to execute request on single node: %s", err.Error())
		}

		if exp, got := tt.expected, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
}

// Test_SingleNodeRequestParameters tests simple requests that contain
// parameters.
func Test_SingleNodeRequestParameters(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	tests := []struct {
		request  *proto.ExecuteQueryRequest
		expected string
	}{
		{
			request: &proto.ExecuteQueryRequest{
				Request: &proto.Request{
					Statements: []*proto.Statement{
						{
							Sql: "SELECT * FROM foo WHERE id = ?",
							Parameters: []*proto.Parameter{
								{
									Value: &proto.Parameter_I{
										I: 1,
									},
								},
							},
						},
					},
				},
			},
			expected: `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`,
		},
		{
			request: &proto.ExecuteQueryRequest{
				Request: &proto.Request{
					Statements: []*proto.Statement{
						{
							Sql: "SELECT id FROM foo WHERE name = :qux",
							Parameters: []*proto.Parameter{
								{
									Value: &proto.Parameter_S{
										S: "fiona",
									},
									Name: "qux",
								},
							},
						},
					},
				},
			},
			expected: `[{"columns":["id"],"types":["integer"],"values":[[1]]}]`,
		},
	}

	for _, tt := range tests {
		r, err := s.Request(tt.request)
		if err != nil {
			t.Fatalf("failed to execute request on single node: %s", err.Error())
		}

		if exp, got := tt.expected, asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}
}

// Test_SingleNodeFK tests that basic foreign-key related functionality works.
func Test_SingleNodeFK(t *testing.T) {
	s, ln := mustNewStoreFK(t)
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

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`CREATE TABLE bar (fooid INTEGER NOT NULL PRIMARY KEY, FOREIGN KEY(fooid) REFERENCES foo(id))`,
	}, false, false)
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	res, _ := s.Execute(executeRequestFromString("INSERT INTO bar(fooid) VALUES(1)", false, false))
	if got, exp := asJSON(res), `[{"error":"FOREIGN KEY constraint failed"}]`; exp != got {
		t.Fatalf("unexpected results for execute\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_SingleNodeExecuteQueryLevels test that various read-consistency
// levels work as expected.
func Test_SingleNodeOnDiskFileExecuteQuery(t *testing.T) {
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

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Every query should return the same results, so use a function for the check.
	check := func(r []*proto.QueryRows) {
		if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	check(r)

	qr = queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	check(r)

	qr = queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	check(r)

	qr = queryRequestFromString("SELECT * FROM foo", false, true)
	qr.Timings = true
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	check(r)

	qr = queryRequestFromString("SELECT * FROM foo", true, false)
	qr.Request.Transaction = true
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	check(r)
}

// Test_SingleNodeExecuteQueryFreshness tests that freshness is ignored on the Leader.
func Test_SingleNodeExecuteQueryFreshness(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	_, err = s0.WaitForAppliedFSM(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for fsmIndex: %s", err.Error())
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	qr.Freshness = mustParseDuration("1ns").Nanoseconds()
	r, err := s0.Query(qr)
	if err != nil {
		t.Fatalf("failed to query leader node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	rr := executeQueryRequestFromString("SELECT * FROM foo", proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE,
		false, false)
	rr.Freshness = mustParseDuration("1ns").Nanoseconds()
	eqr, err := s0.Request(rr)
	if err != nil {
		t.Fatalf("failed to query leader node: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(eqr); exp != got {
		t.Fatalf("unexpected results for request\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_SingleNodeBackup tests that a Store correctly backs up its data
// in text format.
func Test_SingleNodeBackupText(t *testing.T) {
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
	_, err := s.Execute(executeRequestFromString(dump, false, false))
	if err != nil {
		t.Fatalf("failed to load simple dump: %s", err.Error())
	}

	f, err := os.CreateTemp("", "rqlite-baktest-")
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(f.Name())
	s.logger.Printf("backup file is %s", f.Name())

	if err := s.Backup(backupRequestSQL(true), f); err != nil {
		t.Fatalf("Backup failed %s", err.Error())
	}

	// Check the backed up data
	bkp, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("Backup Failed: unable to read backup file, %s", err.Error())
	}
	if ret := bytes.Compare(bkp, []byte(dump)); ret != 0 {
		t.Fatalf("Backup Failed: backup bytes are not same")
	}
}

// Test_SingleNodeSingleCommandTrigger tests that a SQLite trigger works.
func Test_SingleNodeSingleCommandTrigger(t *testing.T) {
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
CREATE TABLE foo (id integer primary key asc, name text);
INSERT INTO "foo" VALUES(1,'bob');
INSERT INTO "foo" VALUES(2,'alice');
INSERT INTO "foo" VALUES(3,'eve');
CREATE TABLE bar (nameid integer, age integer);
INSERT INTO "bar" VALUES(1,44);
INSERT INTO "bar" VALUES(2,46);
INSERT INTO "bar" VALUES(3,8);
CREATE VIEW foobar as select name as Person, Age as age from foo inner join bar on foo.id == bar.nameid;
CREATE TRIGGER new_foobar instead of insert on foobar begin insert into foo (name) values (new.Person); insert into bar (nameid, age) values ((select id from foo where name == new.Person), new.Age); end;
COMMIT;
`
	_, err := s.Execute(executeRequestFromString(dump, false, false))
	if err != nil {
		t.Fatalf("failed to load dump with trigger: %s", err.Error())
	}

	// Check that the VIEW and TRIGGER are OK by using both.
	er := executeRequestFromString("INSERT INTO foobar VALUES('jason', 16)", false, true)
	r, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to insert into view on single node: %s", err.Error())
	}
	if exp, got := int64(3), r[0].GetLastInsertId(); exp != got {
		t.Fatalf("unexpected results for query\nexp: %d\ngot: %d", exp, got)
	}
}

// Test_SingleNodeLoadText tests that a Store correctly loads data in SQL
// text format.
func Test_SingleNodeLoadText(t *testing.T) {
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
	_, err := s.Execute(executeRequestFromString(dump, false, false))
	if err != nil {
		t.Fatalf("failed to load simple dump: %s", err.Error())
	}

	// Check that data were loaded correctly.
	qr := queryRequestFromString("SELECT * FROM foo", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err := s.Query(qr)
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

// Test_SingleNodeLoadTextNoStatements tests that a Store correctly loads data in SQL
// text format, when there are no statements.
func Test_SingleNodeLoadTextNoStatements(t *testing.T) {
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
COMMIT;
`
	_, err := s.Execute(executeRequestFromString(dump, false, false))
	if err != nil {
		t.Fatalf("failed to load dump with no commands: %s", err.Error())
	}
}

// Test_SingleNodeLoadTextEmpty tests that a Store correctly loads data in SQL
// text format, when the text is empty.
func Test_SingleNodeLoadTextEmpty(t *testing.T) {
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

	dump := ``
	_, err := s.Execute(executeRequestFromString(dump, false, false))
	if err != nil {
		t.Fatalf("failed to load empty dump: %s", err.Error())
	}
}

// Test_SingleNodeLoadTextChinook tests loading and querying of the Chinook DB.
func Test_SingleNodeLoadTextChinook(t *testing.T) {
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

	_, err := s.Execute(executeRequestFromString(chinook.DB, false, false))
	if err != nil {
		t.Fatalf("failed to load chinook dump: %s", err.Error())
	}

	// Check that data were loaded correctly.

	qr := queryRequestFromString("SELECT count(*) FROM track", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[3503]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	qr = queryRequestFromString("SELECT count(*) FROM album", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[347]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	qr = queryRequestFromString("SELECT count(*) FROM artist", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[275]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_SingleNodeLoadBinary tests that a Store correctly loads data in SQLite
// binary format from a file.
func Test_SingleNodeLoadBinary(t *testing.T) {
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

	// Load a dataset, to check it's erased by the SQLite file load.
	dump := `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE bar (id integer not null primary key, name text);
INSERT INTO "bar" VALUES(1,'declan');
COMMIT;
`
	_, err := s.Execute(executeRequestFromString(dump, false, false))
	if err != nil {
		t.Fatalf("failed to load simple dump: %s", err.Error())
	}

	// Check that data were loaded correctly.
	qr := queryRequestFromString("SELECT * FROM bar", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"declan"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	err = s.Load(loadRequestFromFile(filepath.Join("testdata", "load.sqlite")))
	if err != nil {
		t.Fatalf("failed to load SQLite file: %s", err.Error())
	}

	// Loading a database should mark that the snapshot store needs a Full Snapshot
	fn, err := s.snapshotStore.FullNeeded()
	if err != nil {
		t.Fatalf("failed to check if snapshot store needs a full snapshot: %s", err.Error())
	}
	if !fn {
		t.Fatalf("expected snapshot store to need a full snapshot")
	}

	// Check that data were loaded correctly.
	qr = queryRequestFromString("SELECT * FROM foo WHERE id=2", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[2,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	qr = queryRequestFromString("SELECT count(*) FROM foo", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[3]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Check pre-existing data is gone.
	qr = queryRequestFromString("SELECT * FROM bar", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `{"error":"no such table: bar"}`, asJSON(r[0]); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeAutoRestore(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	path := mustCopyFileToTempFile(filepath.Join("testdata", "load.sqlite"))
	if err := s.SetRestorePath(path); err != nil {
		t.Fatalf("failed to set restore path: %s", err.Error())
	}

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

	testPoll(t, s.Ready, 100*time.Millisecond, 2*time.Second)
	qr := queryRequestFromString("SELECT * FROM foo WHERE id=2", false, true)
	r, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[2,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeSetRestoreFailStoreOpen(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)

	path := mustCopyFileToTempFile(filepath.Join("testdata", "load.sqlite"))
	defer os.Remove(path)
	if err := s.SetRestorePath(path); err == nil {
		t.Fatalf("expected error setting restore path on open store")
	}
}

func Test_SingleNodeSetRestoreFailBadFile(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)

	path := mustCreateTempFile()
	defer os.Remove(path)
	os.WriteFile(path, []byte("not valid SQLite data"), 0644)
	if err := s.SetRestorePath(path); err == nil {
		t.Fatalf("expected error setting restore path with invalid file")
	}
}

// Test_SingleNodeBoot tests that a Store correctly boots from SQLite data.
func Test_SingleNodeBoot(t *testing.T) {
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

	// Load a dataset, to check it's erased by the SQLite file load.
	dump := `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE bar (id integer not null primary key, name text);
INSERT INTO "bar" VALUES(1,'declan');
COMMIT;
`
	_, err := s.Execute(executeRequestFromString(dump, false, false))
	if err != nil {
		t.Fatalf("failed to load simple dump: %s", err.Error())
	}

	// Check that data were loaded correctly.
	qr := queryRequestFromString("SELECT * FROM bar", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"declan"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	f, err := os.Open(filepath.Join("testdata", "load.sqlite"))
	if err != nil {
		t.Fatalf("failed to open SQLite file: %s", err.Error())
	}
	defer f.Close()

	// Load the SQLite file in bypass mode, check that the right number
	// of snapshots were created, and that the right amount of data was
	// loaded.
	numSnapshots := s.numSnapshots
	n, err := s.ReadFrom(f)
	if err != nil {
		t.Fatalf("failed to load SQLite file via Reader: %s", err.Error())
	}
	sz := mustFileSize(filepath.Join("testdata", "load.sqlite"))
	if n != sz {
		t.Fatalf("expected %d bytes to be read, got %d", sz, n)
	}
	if s.numSnapshots != numSnapshots+1 {
		t.Fatalf("expected 1 extra snapshot, got %d", s.numSnapshots)
	}

	// Check that data were loaded correctly.
	qr = queryRequestFromString("SELECT * FROM foo WHERE id=2", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[2,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	qr = queryRequestFromString("SELECT count(*) FROM foo", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[3]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Check pre-existing data is gone.
	qr = queryRequestFromString("SELECT * FROM bar", false, true)
	qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `{"error":"no such table: bar"}`, asJSON(r[0]); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Confirm that after restarting the Store, the data still looks good.
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close store: %s", err.Error())
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	qr = queryRequestFromString("SELECT count(*) FROM foo", false, true)
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[3]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

}

func Test_SingleNodeBoot_InvalidFail_WALOK(t *testing.T) {
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

	// Ensure WAL SQLite data is not accepted.
	b := make([]byte, 1024)
	mustReadRandom(b)
	r := bytes.NewReader(b)
	if _, err := s.ReadFrom(r); err == nil {
		t.Fatalf("expected error reading from invalid SQLite file")
	}

	// Ensure WAL-enabled SQLite file is accepted.
	f, err := os.Open(filepath.Join("testdata", "wal-enabled.sqlite"))
	if err != nil {
		t.Fatalf("failed to open SQLite file: %s", err.Error())
	}
	defer f.Close()
	if _, err := s.ReadFrom(f); err != nil {
		t.Fatalf("error reading from WAL-enabled SQLite file")
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

	mustNoop(s, "1")
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}

	if err := s.snapshotCAS.Begin(); err != nil {
		t.Fatalf("failed to begin snapshot CAS: %s", err.Error())
	}
	mustNoop(s, "2")
	if err := s.Snapshot(0); err == nil {
		t.Fatalf("expected error snapshotting single-node store with CAS")
	}
	s.snapshotCAS.End()
	mustNoop(s, "3")
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}
}

func Test_SingleNode_WALTriggeredSnapshot(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	s.SnapshotThreshold = 8192
	s.SnapshotInterval = 500 * time.Millisecond
	s.SnapshotThresholdWALSize = 4096

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
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	nSnaps := stats.Get(numWALSnapshots).String()

	for i := 0; i < 100; i++ {
		_, err := s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
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
	// files -- a SQLite database file, and a diretory named after
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
	if len(files) != 2 {
		t.Fatalf("wrong number of snapshot store files: %d", len(files))
	}
	for _, f := range files {
		if !strings.Contains(f.Name(), snaps[0].ID) {
			t.Fatalf("wrong snapshot store file: %s", f.Name())
		}
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

// Test_OpenStoreSingleNode_VacuumFullNeeded tests that running a VACUUM
// means that a full snapshot is needed.
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
	_, err := s.Execute(er)
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
	} else if !fn {
		t.Fatalf("full snapshot not marked as needed")
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
		_, err := s.Execute(er)
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	doQuery := func() {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, true)
		qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
		r, err := s.Query(qr)
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
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	for i := 0; i < 100; i++ {
		_, err := s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
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

	// Restart test
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
		_, err := s.Execute(er)
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	doQuery := func() {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, true)
		qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
		r, err := s.Query(qr)
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
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Insert a bunch of data concurrently, and do VACUUMs; putting some load on the Store.
	var wg sync.WaitGroup
	wg.Add(6)
	insertFn := func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_, err := s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
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
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	for i := 0; i < 100; i++ {
		_, err := s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
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
	r, err := s.Query(qr)
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
	r, err = s.Query(qr)
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
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Insert a bunch of data concurrently, putting some load on the Store.
	var wg sync.WaitGroup
	wg.Add(5)
	insertFn := func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_, err := s.Execute(executeRequestFromString(`INSERT INTO foo(name) VALUES("fiona")`, false, false))
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
	r, err := s.Query(qr)
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
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2500]]}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeSelfJoinNoChangeOK(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)

	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Self-join should not be a problem. It should just be ignored.
	err := s0.Join(joinRequest(s0.ID(), s0.Addr(), true))
	if err != nil {
		t.Fatalf("received error for non-changing self-join")
	}
	if got, exp := s0.numIgnoredJoins, 1; got != exp {
		t.Fatalf("wrong number of ignored joins, exp %d, got %d", exp, got)
	}
}

func Test_SingleNodeStepdown(t *testing.T) {
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

	// Tell leader to step down. Should fail as there is no other node available.
	if err := s.Stepdown(true); err == nil {
		t.Fatalf("single node stepped down OK")
	}
}

func Test_SingleNodeStepdownNoWaitOK(t *testing.T) {
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

	// Tell leader to step down without waiting.
	if err := s.Stepdown(false); err != nil {
		t.Fatalf("single node reported error stepping down even when told not to wait")
	}
}

func Test_SingleNodeWaitForRemove(t *testing.T) {
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

	// Should timeout waiting for removal of ourselves
	err := s.WaitForRemoval(s.ID(), time.Second)
	// if err is nil then fail the test
	if err == nil {
		t.Fatalf("no error waiting for removal of non-existent node")
	}
	if !errors.Is(err, ErrWaitForRemovalTimeout) {
		t.Fatalf("waiting for removal resulted in wrong error: %s", err.Error())
	}

	// should be no error waiting for removal of non-existent node
	err = s.WaitForRemoval("non-existent-node", time.Second)
	if err != nil {
		t.Fatalf("error waiting for removal of non-existent node: %s", err.Error())
	}
}

func Test_SingleNodeNoop(t *testing.T) {
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

	af, err := s.Noop("1")
	if err != nil {
		t.Fatalf("failed to write noop command: %s", err.Error())
	}
	if af.Error() != nil {
		t.Fatalf("expected nil apply future error")
	}
	if s.numNoops != 1 {
		t.Fatalf("noop count is wrong, got: %d", s.numNoops)
	}
}

func Test_IsLeader(t *testing.T) {
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

	if !s.IsLeader() {
		t.Fatalf("single node is not leader!")
	}
	if !s.HasLeader() {
		t.Fatalf("single node does not have leader!")
	}
}

func Test_IsVoter(t *testing.T) {
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

	v, err := s.IsVoter()
	if err != nil {
		t.Fatalf("failed to check if single node is a voter: %s", err.Error())
	}
	if !v {
		t.Fatalf("single node is not a voter!")
	}
}

func Test_RWROCount(t *testing.T) {
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

	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		false, false)
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	tests := []struct {
		name  string
		stmts []string
		expRW int
		expRO int
	}{
		{
			name:  "Empty SQL",
			stmts: []string{""},
		},
		{
			name:  "Junk SQL",
			stmts: []string{"asdkflj asgkdj"},
			expRW: 1,
		},
		{
			name:  "CREATE TABLE statement, already exists",
			stmts: []string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"},
			expRW: 1,
		},
		{
			name:  "Single INSERT",
			stmts: []string{"INSERT INTO foo(id, name) VALUES(1, 'fiona')"},
			expRW: 1,
		},
		{
			name:  "Single INSERT, non-existent table",
			stmts: []string{"INSERT INTO qux(id, name) VALUES(1, 'fiona')"},
			expRW: 1,
		},
		{
			name:  "Single SELECT",
			stmts: []string{"SELECT * FROM foo"},
			expRO: 1,
		},
		{
			name:  "Single SELECT from non-existent table",
			stmts: []string{"SELECT * FROM qux"},
			expRW: 1, // Yeah, this is unfortunate, but it's how SQLite works.
		},
		{
			name:  "Double SELECT",
			stmts: []string{"SELECT * FROM foo", "SELECT * FROM foo WHERE id = 1"},
			expRO: 2,
		},
		{
			name:  "Mix queries and executes",
			stmts: []string{"SELECT * FROM foo", "INSERT INTO foo(id, name) VALUES(1, 'fiona')"},
			expRW: 1,
			expRO: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rwN, roN := s.RORWCount(executeQueryRequestFromStrings(tt.stmts, proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE, false, false))
			if rwN != tt.expRW {
				t.Fatalf("wrong number of RW statements, exp %d, got %d", tt.expRW, rwN)
			}
			if roN != tt.expRO {
				t.Fatalf("wrong number of RO statements, exp %d, got %d", tt.expRO, roN)
			}
		})
	}

}

func Test_State(t *testing.T) {
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

	state := s.State()
	if state != Leader {
		t.Fatalf("single node returned incorrect state (not Leader): %v", s)
	}
}

func mustNewStoreAtPathsLn(id, dataPath, sqlitePath string, fk bool) (*Store, net.Listener) {
	cfg := NewDBConfig()
	cfg.FKConstraints = fk
	cfg.OnDiskPath = sqlitePath

	ly := mustMockLayer("localhost:0")
	s := New(ly, &Config{
		DBConf: cfg,
		Dir:    dataPath,
		ID:     id,
	})
	if s == nil {
		panic("failed to create new store")
	}
	return s, ly
}

func mustNewStore(t *testing.T) (*Store, net.Listener) {
	return mustNewStoreAtPathsLn(random.String(), t.TempDir(), "", false)
}

func mustNewStoreFK(t *testing.T) (*Store, net.Listener) {
	return mustNewStoreAtPathsLn(random.String(), t.TempDir(), "", true)
}

func mustNewStoreSQLitePath(t *testing.T) (*Store, net.Listener, string) {
	dataDir := t.TempDir()
	sqliteDir := t.TempDir()
	sqlitePath := filepath.Join(sqliteDir, "explicit-path.db")
	s, ln := mustNewStoreAtPathsLn(random.String(), dataDir, sqlitePath, true)
	return s, ln, sqlitePath
}

type mockSnapshotSink struct {
	*os.File
}

func (m *mockSnapshotSink) ID() string {
	return "1"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

type mockLayer struct {
	ln net.Listener
}

func mustMockLayer(addr string) Layer {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic("failed to create new listner")
	}
	return &mockLayer{ln}
}

func (m *mockLayer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}

func (m *mockLayer) Accept() (net.Conn, error) { return m.ln.Accept() }

func (m *mockLayer) Close() error { return m.ln.Close() }

func (m *mockLayer) Addr() net.Addr { return m.ln.Addr() }

func mustNoop(s *Store, id string) {
	af, err := s.Noop(id)
	if err != nil {
		panic("failed to write noop command")
	}
	if af.Error() != nil {
		panic("expected nil apply future error")
	}
}

func mustCreateTempFile() string {
	f, err := os.CreateTemp("", "rqlite-temp")
	if err != nil {
		panic("failed to create temporary file")
	}
	f.Close()
	return f.Name()
}

func mustCreateTempFD() *os.File {
	f, err := os.CreateTemp("", "rqlite-temp")
	if err != nil {
		panic("failed to create temporary file")
	}
	return f
}

func mustWriteFile(path, contents string) {
	err := os.WriteFile(path, []byte(contents), 0644)
	if err != nil {
		panic("failed to write to file")
	}
}

func mustReadFile(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		panic("failed to read file")
	}
	return b
}

func mustReadRandom(b []byte) {
	_, err := rand.Read(b)
	if err != nil {
		panic("failed to read random bytes")
	}
}

func mustCopyFileToTempFile(path string) string {
	f := mustCreateTempFile()
	mustWriteFile(f, string(mustReadFile(path)))
	return f
}

func mustParseDuration(t string) time.Duration {
	d, err := time.ParseDuration(t)
	if err != nil {
		panic("failed to parse duration")
	}
	return d
}

func mustFileSize(path string) int64 {
	n, err := fileSize(path)
	if err != nil {
		panic("failed to get file size")
	}
	return n
}

func executeRequestFromString(s string, timings, tx bool) *proto.ExecuteRequest {
	return executeRequestFromStrings([]string{s}, timings, tx)
}

// executeRequestFromStrings converts a slice of strings into a proto.ExecuteRequest
func executeRequestFromStrings(s []string, timings, tx bool) *proto.ExecuteRequest {
	stmts := make([]*proto.Statement, len(s))
	for i := range s {
		stmts[i] = &proto.Statement{
			Sql: s[i],
		}
	}
	return &proto.ExecuteRequest{
		Request: &proto.Request{
			Statements:  stmts,
			Transaction: tx,
		},
		Timings: timings,
	}
}

func queryRequestFromString(s string, timings, tx bool) *proto.QueryRequest {
	return queryRequestFromStrings([]string{s}, timings, tx)
}

// queryRequestFromStrings converts a slice of strings into a proto.QueryRequest
func queryRequestFromStrings(s []string, timings, tx bool) *proto.QueryRequest {
	stmts := make([]*proto.Statement, len(s))
	for i := range s {
		stmts[i] = &proto.Statement{
			Sql: s[i],
		}
	}
	return &proto.QueryRequest{
		Request: &proto.Request{
			Statements:  stmts,
			Transaction: tx,
		},
		Timings: timings,
	}
}

func executeQueryRequestFromString(s string, lvl proto.QueryRequest_Level, timings, tx bool) *proto.ExecuteQueryRequest {
	return executeQueryRequestFromStrings([]string{s}, lvl, timings, tx)
}

func executeQueryRequestFromStrings(s []string, lvl proto.QueryRequest_Level, timings, tx bool) *proto.ExecuteQueryRequest {
	stmts := make([]*proto.Statement, len(s))
	for i := range s {
		stmts[i] = &proto.Statement{
			Sql: s[i],
		}
	}
	return &proto.ExecuteQueryRequest{
		Request: &proto.Request{
			Statements:  stmts,
			Transaction: tx,
		},
		Timings: timings,
		Level:   lvl,
	}
}

func backupRequestSQL(leader bool) *proto.BackupRequest {
	return &proto.BackupRequest{
		Format: proto.BackupRequest_BACKUP_REQUEST_FORMAT_SQL,
		Leader: leader,
	}
}

func backupRequestBinary(leader, vacuum, compress bool) *proto.BackupRequest {
	return &proto.BackupRequest{
		Format:   proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY,
		Leader:   leader,
		Vacuum:   vacuum,
		Compress: compress,
	}
}

func loadRequestFromFile(path string) *proto.LoadRequest {
	return &proto.LoadRequest{
		Data: mustReadFile(path),
	}
}

func joinRequest(id, addr string, voter bool) *proto.JoinRequest {
	return &proto.JoinRequest{
		Id:      id,
		Address: addr,
		Voter:   voter,
	}
}

func notifyRequest(id, addr string) *proto.NotifyRequest {
	return &proto.NotifyRequest{
		Id:      id,
		Address: addr,
	}
}

func removeNodeRequest(id string) *proto.RemoveNodeRequest {
	return &proto.RemoveNodeRequest{
		Id: id,
	}
}

func gunzipFile(dst, src *os.File) error {
	r, err := os.Open(src.Name())
	if err != nil {
		return err
	}
	defer r.Close()
	gzr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gzr.Close()
	_, err = io.Copy(dst, gzr)
	return err
}

func filesIdentical(path1, path2 string) bool {
	b1, err := os.ReadFile(path1)
	if err != nil {
		return false
	}
	b2, err := os.ReadFile(path2)
	if err != nil {
		return false
	}
	return bytes.Equal(b1, b2)
}

// waitForLeaderID waits until the Store's LeaderID is set, or the timeout
// expires. Because setting Leader ID requires Raft to set the cluster
// configuration, it's not entirely deterministic when it will be set.
func waitForLeaderID(s *Store, timeout time.Duration) (string, error) {
	tck := time.NewTicker(100 * time.Millisecond)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			id, err := s.LeaderID()
			if err != nil {
				return "", err
			}
			if id != "" {
				return id, nil
			}
		case <-tmr.C:
			return "", fmt.Errorf("timeout expired")
		}
	}
}

func asJSON(v interface{}) string {
	enc := encoding.Encoder{}
	b, err := enc.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}

func asJSONAssociative(v interface{}) string {
	enc := encoding.Encoder{
		Associative: true,
	}
	b, err := enc.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}

func testPoll(t *testing.T, f func() bool, p time.Duration, d time.Duration) {
	t.Helper()
	tck := time.NewTicker(p)
	defer tck.Stop()
	tmr := time.NewTimer(d)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if f() {
				return
			}
		case <-tmr.C:
			t.Fatalf("timeout expired: %s", t.Name())
		}
	}
}
