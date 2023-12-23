package store

import (
	"bytes"
	"crypto/rand"
	"errors"
	"expvar"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command"
	"github.com/rqlite/rqlite/v8/command/encoding"
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
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

	f, err := os.CreateTemp("", "rqlite-baktest-")
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(f.Name())

	if err := s.Backup(backupRequestBinary(true), f); err != nil {
		t.Fatalf("Backup failed %s", err.Error())
	}

	// Open the backup file using the DB layer and check the data.
	db, err := db.Open(f.Name(), false, false)
	if err != nil {
		t.Fatalf("unable to open backup database, %s", err.Error())
	}
	defer db.Close()
	var buf bytes.Buffer
	w := &buf
	if err := db.Dump(w); err != nil {
		t.Fatalf("unable to dump backup database, %s", err.Error())
	}
	if buf.String() != dump {
		t.Fatalf("backup dump is not as expected, got %s", buf.String())
	}
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
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
	var r []*command.QueryRows

	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	_, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}

	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	_, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}

	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
				`SELECT * FROM foo`,
				`INSERT INTO foo(id, name) VALUES(66, "declan")`,
			},
			expected: `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]},{"last_insert_id":66,"rows_affected":1}]`,
		},
		{
			stmts: []string{
				`INSERT INTO foo(id, name) VALUES(77, "fiona")`,
				`SELECT COUNT(*) FROM foo`,
			},
			expected: `[{"last_insert_id":77,"rows_affected":1},{"columns":["COUNT(*)"],"types":["integer"],"values":[[3]]}]`,
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
		eqr := executeQueryRequestFromStrings(tt.stmts, command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, false, false)
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
		eqr := executeQueryRequestFromStrings(tt.stmts, command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, false, tt.tx)
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
		request  *command.ExecuteQueryRequest
		expected string
	}{
		{
			request: &command.ExecuteQueryRequest{
				Request: &command.Request{
					Statements: []*command.Statement{
						{
							Sql: "SELECT * FROM foo WHERE id = ?",
							Parameters: []*command.Parameter{
								{
									Value: &command.Parameter_I{
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
			request: &command.ExecuteQueryRequest{
				Request: &command.Request{
					Statements: []*command.Statement{
						{
							Sql: "SELECT id FROM foo WHERE name = :qux",
							Parameters: []*command.Parameter{
								{
									Value: &command.Parameter_S{
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
	check := func(r []*command.QueryRows) {
		if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err := s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	check(r)

	qr = queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	check(r)

	qr = queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	check(r)

	qr = queryRequestFromString("SELECT * FROM foo", false, true)
	qr.Timings = true
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	check(r)

	qr = queryRequestFromString("SELECT * FROM foo", true, false)
	qr.Request.Transaction = true
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	check(r)
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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

	// Check that data were loaded correctly.
	qr = queryRequestFromString("SELECT * FROM foo WHERE id=2", false, true)
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	r, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `{"error":"no such table: bar"}`, asJSON(r[0]); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeBoot_Fail(t *testing.T) {
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

	// Ensure invalid SQLite data is not accepted.
	b := make([]byte, 1024)
	mustReadRandom(b)
	r := bytes.NewReader(b)
	if _, err := s.ReadFrom(r); err == nil {
		t.Fatalf("expected error reading from invalid SQLite file")
	}

	// Ensure WAL-enabled SQLite file is not accepted.
	f, err := os.Open(filepath.Join("testdata", "wal-enabled.sqlite"))
	if err != nil {
		t.Fatalf("failed to open SQLite file: %s", err.Error())
	}
	defer f.Close()
	if _, err := s.ReadFrom(f); err == nil {
		t.Fatalf("expected error reading from WAL-enabled SQLite file")
	}
}

// Test_SingleNodeRecoverNoChange tests a node recovery that doesn't
// actually change anything.
func Test_SingleNodeRecoverNoChange(t *testing.T) {
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

	queryTest := func() {
		t.Helper()
		qr := queryRequestFromString("SELECT * FROM foo", false, false)
		qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
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

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	queryTest()
	id, addr := s.ID(), s.Addr()
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Set up for Recovery during open
	peers := fmt.Sprintf(`[{"id": "%s","address": "%s"}]`, id, addr)
	peersPath := filepath.Join(s.Path(), "/raft/peers.json")
	peersInfo := filepath.Join(s.Path(), "/raft/peers.info")
	mustWriteFile(peersPath, peers)
	if err := s.Open(); err != nil {
		t.Fatalf("failed to re-open single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	queryTest()
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	if pathExists(peersPath) {
		t.Fatalf("Peers JSON exists at %s", peersPath)
	}
	if !pathExists(peersInfo) {
		t.Fatalf("Peers info does not exist at %s", peersInfo)
	}
}

// Test_SingleNodeRecoverNetworkChange tests a node recovery that
// involves a changed-network address.
func Test_SingleNodeRecoverNetworkChange(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	queryTest := func(s *Store) {
		qr := queryRequestFromString("SELECT * FROM foo", false, false)
		qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
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

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	queryTest(s0)

	id := s0.ID()
	if err := s0.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Create a new node, at the same path. Will presumably have a different
	// Raft network address, since they are randomly assigned.
	sR, srLn := mustNewStoreAtPathsLn(id, s0.Path(), "", true)
	defer srLn.Close()
	if IsNewNode(sR.Path()) {
		t.Fatalf("store detected incorrectly as new")
	}

	// Set up for Recovery during open
	peers := fmt.Sprintf(`[{"id": "%s","address": "%s"}]`, s0.ID(), srLn.Addr().String())
	peersPath := filepath.Join(sR.Path(), "/raft/peers.json")
	peersInfo := filepath.Join(sR.Path(), "/raft/peers.info")
	mustWriteFile(peersPath, peers)
	if err := sR.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}

	if _, err := sR.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader on recovered node: %s", err)
	}

	queryTest(sR)
	if err := sR.Close(true); err != nil {
		t.Fatalf("failed to close single-node recovered store: %s", err.Error())
	}

	if pathExists(peersPath) {
		t.Fatalf("Peers JSON exists at %s", peersPath)
	}
	if !pathExists(peersInfo) {
		t.Fatalf("Peers info does not exist at %s", peersInfo)
	}
}

// Test_SingleNodeRecoverNetworkChangeSnapshot tests a node recovery that
// involves a changed-network address, with snapshots underneath.
func Test_SingleNodeRecoverNetworkChangeSnapshot(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	s0.SnapshotThreshold = 4
	s0.SnapshotInterval = 100 * time.Millisecond
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	queryTest := func(s *Store, c int) {
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false)
		qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
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

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	queryTest(s0, 1)

	for i := 0; i < 9; i++ {
		er := executeRequestFromStrings([]string{
			`INSERT INTO foo(name) VALUES("fiona")`,
		}, false, false)
		if _, err := s0.Execute(er); err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	queryTest(s0, 10)

	// Wait for a snapshot to take place.
	for {
		time.Sleep(100 * time.Millisecond)
		s0.numSnapshotsMu.Lock()
		ns := s0.numSnapshots
		s0.numSnapshotsMu.Unlock()
		if ns > 0 {
			break
		}
	}

	id := s0.ID()
	if err := s0.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Create a new node, at the same path. Will presumably have a different
	// Raft network address, since they are randomly assigned.
	sR, srLn := mustNewStoreAtPathsLn(id, s0.Path(), "", true)
	if IsNewNode(sR.Path()) {
		t.Fatalf("store detected incorrectly as new")
	}

	// Set up for Recovery during open
	peers := fmt.Sprintf(`[{"id": "%s","address": "%s"}]`, id, srLn.Addr().String())
	peersPath := filepath.Join(sR.Path(), "/raft/peers.json")
	peersInfo := filepath.Join(sR.Path(), "/raft/peers.info")
	mustWriteFile(peersPath, peers)
	if err := sR.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}

	if _, err := sR.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader on recovered node: %s", err)
	}
	queryTest(sR, 10)
	if err := sR.Close(true); err != nil {
		t.Fatalf("failed to close single-node recovered store: %s", err.Error())
	}

	if pathExists(peersPath) {
		t.Fatalf("Peers JSON exists at %s", peersPath)
	}
	if !pathExists(peersInfo) {
		t.Fatalf("Peers info does not exist at %s", peersInfo)
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

func Test_MultiNodeJoinRemove(t *testing.T) {
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

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)
	if err := s1.Bootstrap(NewServer(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}

	// Get sorted list of cluster nodes.
	storeNodes := []string{s0.ID(), s1.ID()}
	sort.StringSlice(storeNodes).Sort()

	// Join the second node to the first.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	_, err := s1.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader address on follower: %s", err.Error())
	}

	nodes, err := s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}

	if len(nodes) != len(storeNodes) {
		t.Fatalf("size of cluster is not correct")
	}
	if storeNodes[0] != nodes[0].ID || storeNodes[1] != nodes[1].ID {
		t.Fatalf("cluster does not have correct nodes")
	}

	// Should timeout waiting for removal of other node
	err = s0.WaitForRemoval(s1.ID(), time.Second)
	// if err is nil then fail the test
	if err == nil {
		t.Fatalf("no error waiting for removal of non-existent node")
	}
	if !errors.Is(err, ErrWaitForRemovalTimeout) {
		t.Fatalf("waiting for removal resulted in wrong error: %s", err.Error())
	}

	// Remove a node.
	if err := s0.Remove(removeNodeRequest(s1.ID())); err != nil {
		t.Fatalf("failed to remove %s from cluster: %s", s1.ID(), err.Error())
	}

	nodes, err = s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes post remove: %s", err.Error())
	}
	if len(nodes) != 1 {
		t.Fatalf("size of cluster is not correct post remove")
	}
	if s0.ID() != nodes[0].ID {
		t.Fatalf("cluster does not have correct nodes post remove")
	}

	// Should be no error now waiting for removal of other node
	err = s0.WaitForRemoval(s1.ID(), time.Second)
	// if err is nil then fail the test
	if err != nil {
		t.Fatalf("error waiting for removal of removed node")
	}
}

func Test_MultiNodeStepdown(t *testing.T) {
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

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s2.Close(true)

	// Form the 3-node cluster
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	if _, err := s2.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Tell leader to step down. After this finishes there should be a new Leader.
	if err := s0.Stepdown(true); err != nil {
		t.Fatalf("leader failed to step down: %s", err.Error())
	}

	check := func() bool {
		leader, err := s1.WaitForLeader(10 * time.Second)
		if err != nil || leader == s0.Addr() {
			return false
		}
		return true
	}
	testPoll(t, check, 250*time.Millisecond, 10*time.Second)
}

func Test_MultiNodeStoreNotifyBootstrap(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s2.Close(true)

	s0.BootstrapExpect = 3
	if err := s0.Notify(notifyRequest(s0.ID(), ln0.Addr().String())); err != nil {
		t.Fatalf("failed to notify store: %s", err.Error())
	}
	if err := s0.Notify(notifyRequest(s0.ID(), ln0.Addr().String())); err != nil {
		t.Fatalf("failed to notify store -- not idempotent: %s", err.Error())
	}
	if err := s0.Notify(notifyRequest(s1.ID(), ln1.Addr().String())); err != nil {
		t.Fatalf("failed to notify store: %s", err.Error())
	}
	if err := s0.Notify(notifyRequest(s2.ID(), ln2.Addr().String())); err != nil {
		t.Fatalf("failed to notify store: %s", err.Error())
	}

	// Check that the cluster bootstrapped properly.
	reportedLeaders := make([]string, 3)
	for i, n := range []*Store{s0, s1, s2} {
		check := func() bool {
			nodes, err := n.Nodes()
			return err == nil && len(nodes) == 3
		}
		testPoll(t, check, 250*time.Millisecond, 10*time.Second)

		var err error
		reportedLeaders[i], err = n.WaitForLeader(10 * time.Second)
		if err != nil {
			t.Fatalf("failed to get leader on node %d (id=%s): %s", i, n.raftID, err.Error())
		}
	}
	if reportedLeaders[0] != reportedLeaders[1] || reportedLeaders[0] != reportedLeaders[2] {
		t.Fatalf("leader not the same on each node")
	}

	// Calling Notify() on a node that is part of a cluster should
	// be a no-op.
	if err := s0.Notify(notifyRequest(s1.ID(), ln1.Addr().String())); err != nil {
		t.Fatalf("failed to notify store that is part of cluster: %s", err.Error())
	}
}

// Test_MultiNodeStoreAutoRestoreBootstrap tests that a cluster will
// bootstrap correctly when each node is supplied with an auto-restore
// file. Only one node should do able to restore from the file.
func Test_MultiNodeStoreAutoRestoreBootstrap(t *testing.T) {
	ResetStats()
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	s2, ln2 := mustNewStore(t)
	defer ln2.Close()

	path0 := mustCopyFileToTempFile(filepath.Join("testdata", "load.sqlite"))
	path1 := mustCopyFileToTempFile(filepath.Join("testdata", "load.sqlite"))
	path2 := mustCopyFileToTempFile(filepath.Join("testdata", "load.sqlite"))

	s0.SetRestorePath(path0)
	s1.SetRestorePath(path1)
	s2.SetRestorePath(path2)

	for i, s := range []*Store{s0, s1, s2} {
		if err := s.Open(); err != nil {
			t.Fatalf("failed to open store %d: %s", i, err.Error())
		}
		defer s.Close(true)
	}

	// Trigger a bootstrap.
	s0.BootstrapExpect = 3
	for _, s := range []*Store{s0, s1, s2} {
		if err := s0.Notify(notifyRequest(s.ID(), s.ly.Addr().String())); err != nil {
			t.Fatalf("failed to notify store: %s", err.Error())
		}
	}

	// Wait for the cluster to bootstrap.
	_, err := s0.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader: %s", err.Error())
	}

	// Ultimately there is a hard-to-control timing issue here. Knowing
	// exactly when the leader has applied the restore is difficult, so
	// just wait a bit.
	time.Sleep(2 * time.Second)

	if !s0.Ready() {
		t.Fatalf("node is not ready")
	}

	qr := queryRequestFromString("SELECT * FROM foo WHERE id=2", false, true)
	r, err := s0.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[2,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	if pathExists(path0) || pathExists(path1) || pathExists(path2) {
		t.Fatalf("an auto-restore file was not removed")
	}

	numAuto := stats.Get(numAutoRestores).(*expvar.Int).Value()
	numAutoSkipped := stats.Get(numAutoRestoresSkipped).(*expvar.Int).Value()
	if exp, got := int64(1), numAuto; exp != got {
		t.Fatalf("unexpected number of auto-restores\nexp: %d\ngot: %d", exp, got)
	}
	if exp, got := int64(2), numAutoSkipped; exp != got {
		t.Fatalf("unexpected number of auto-restores skipped\nexp: %d\ngot: %d", exp, got)
	}
}

func Test_MultiNodeJoinNonVoterRemove(t *testing.T) {
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

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	// Get sorted list of cluster nodes.
	storeNodes := []string{s0.ID(), s1.ID()}
	sort.StringSlice(storeNodes).Sort()

	// Join the second node to the first.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), false)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Check leader state on follower.
	got, err := s1.LeaderAddr()
	if err != nil {
		t.Fatalf("failed to get leader address: %s", err.Error())
	}
	if exp := s0.Addr(); got != exp {
		t.Fatalf("wrong leader address returned, got: %s, exp %s", got, exp)
	}
	id, err := waitForLeaderID(s1, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to retrieve leader ID: %s", err.Error())
	}
	if got, exp := id, s0.raftID; got != exp {
		t.Fatalf("wrong leader ID returned, got: %s, exp %s", got, exp)
	}

	nodes, err := s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}

	if len(nodes) != len(storeNodes) {
		t.Fatalf("size of cluster is not correct")
	}
	if storeNodes[0] != nodes[0].ID || storeNodes[1] != nodes[1].ID {
		t.Fatalf("cluster does not have correct nodes")
	}

	// Remove the non-voter.
	if err := s0.Remove(removeNodeRequest(s1.ID())); err != nil {
		t.Fatalf("failed to remove %s from cluster: %s", s1.ID(), err.Error())
	}

	nodes, err = s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes post remove: %s", err.Error())
	}
	if len(nodes) != 1 {
		t.Fatalf("size of cluster is not correct post remove")
	}
	if s0.ID() != nodes[0].ID {
		t.Fatalf("cluster does not have correct nodes post remove")
	}
}

func Test_MultiNodeExecuteQuery(t *testing.T) {
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

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t)
	defer ln2.Close()
	if err := s2.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s2.Close(true)

	// Join the second node to the first as a voting node.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	// Join the third node to the first as a non-voting node.
	if err := s0.Join(joinRequest(s2.ID(), s2.Addr(), false)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	s0FsmIdx, err := s0.WaitForAppliedFSM(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for fsmIndex: %s", err.Error())
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
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

	// Wait until the log entries have been applied to the voting follower,
	// and then query.
	if _, err := s1.WaitForFSMIndex(s0FsmIdx, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}

	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	_, err = s1.Query(qr)
	if err == nil {
		t.Fatalf("successfully queried non-leader node")
	}
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	_, err = s1.Query(qr)
	if err == nil {
		t.Fatalf("successfully queried non-leader node")
	}
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err = s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Wait until the 3 log entries have been applied to the non-voting follower,
	// and then query.
	if err := s2.WaitForAppliedIndex(3, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}

	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	_, err = s1.Query(qr)
	if err == nil {
		t.Fatalf("successfully queried non-voting node with Weak")
	}
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	_, err = s1.Query(qr)
	if err == nil {
		t.Fatalf("successfully queried non-voting node with Strong")
	}
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err = s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query non-voting node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
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
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
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
}

func Test_MultiNodeExecuteQueryFreshness(t *testing.T) {
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

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	// Join the second node to the first.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err := s0.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
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

	// Wait until the 3 log entries have been applied to the follower,
	// and then query.
	if err := s1.WaitForAppliedIndex(3, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}

	// "Weak" consistency queries with 1 nanosecond freshness should pass, because freshness
	// is ignored in this case.
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	qr.Freshness = mustParseDuration("1ns").Nanoseconds()
	_, err = s0.Query(qr)
	if err != nil {
		t.Fatalf("Failed to ignore freshness if level is Weak: %s", err.Error())
	}
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	// "Strong" consistency queries with 1 nanosecond freshness should pass, because freshness
	// is ignored in this case.
	_, err = s0.Query(qr)
	if err != nil {
		t.Fatalf("Failed to ignore freshness if level is Strong: %s", err.Error())
	}

	// Kill leader.
	s0.Close(true)

	// "None" consistency queries should still work.
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	qr.Freshness = 0
	r, err = s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Wait for the freshness interval to pass.
	time.Sleep(mustParseDuration("1s"))

	// "None" consistency queries with 1 nanosecond freshness should fail, because at least
	// one nanosecond *should* have passed since leader died (surely!).
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	qr.Freshness = mustParseDuration("1ns").Nanoseconds()
	_, err = s1.Query(qr)
	if err == nil {
		t.Fatalf("freshness violating query didn't return an error")
	}
	if err != ErrStaleRead {
		t.Fatalf("freshness violating query returned wrong error: %s", err.Error())
	}

	// Freshness of 0 is ignored.
	qr.Freshness = 0
	r, err = s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// "None" consistency queries with 1 hour freshness should pass, because it should
	// not be that long since the leader died.
	qr.Freshness = mustParseDuration("1h").Nanoseconds()
	r, err = s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query follower node: %s", err.Error())
	}
	if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	// Check Stale-read detection works with Requests too.
	eqr := executeQueryRequestFromString("SELECT * FROM foo", command.QueryRequest_QUERY_REQUEST_LEVEL_NONE,
		false, false)
	eqr.Freshness = mustParseDuration("1ns").Nanoseconds()
	_, err = s1.Request(eqr)
	if err == nil {
		t.Fatalf("freshness violating request didn't return an error")
	}
	if err != ErrStaleRead {
		t.Fatalf("freshness violating request returned wrong error: %s", err.Error())
	}
	eqr.Freshness = 0
	eqresp, err := s1.Request(eqr)
	if err != nil {
		t.Fatalf("inactive freshness violating request returned an error")
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]`, asJSON(eqresp); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_StoreLogTruncationMultinode(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	s0.SnapshotThreshold = 4
	s0.SnapshotInterval = 100 * time.Millisecond

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

	nSnaps := stats.Get(numSnapshots).String()

	// Write more than s.SnapshotThreshold statements.
	queries := []string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(3, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(4, "fiona")`,
		`INSERT INTO foo(id, name) VALUES(5, "fiona")`,
	}
	for i := range queries {
		_, err := s0.Execute(executeRequestFromString(queries[i], false, false))
		if err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}

	// Wait for the snapshot to happen and log to be truncated.
	f := func() bool {
		return stats.Get(numSnapshots).String() != nSnaps
	}
	testPoll(t, f, 100*time.Millisecond, 2*time.Second)

	// Do one more execute, to ensure there is at least one log not snapshot.
	// Without this, there is no guarantee fsmIndex will be set on s1.
	_, err := s0.Execute(executeRequestFromString(`INSERT INTO foo(id, name) VALUES(6, "fiona")`, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Fire up new node and ensure it picks up all changes. This will
	// involve getting a snapshot and truncated log.
	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	// Join the second node to the first.
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	// Wait until the log entries have been applied to the follower,
	// and then query.
	if err := s1.WaitForAppliedIndex(8, 5*time.Second); err != nil {
		t.Fatalf("error waiting for follower to apply index: %s:", err.Error())
	}
	qr := queryRequestFromString("SELECT count(*) FROM foo", false, true)
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	r, err := s1.Query(qr)
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[6]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
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

func Test_RequiresLeader(t *testing.T) {
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
		name     string
		stmts    []string
		lvl      command.QueryRequest_Level
		requires bool
	}{
		{
			name:     "Empty SQL",
			stmts:    []string{""},
			requires: false,
		},
		{
			name:     "Junk SQL",
			stmts:    []string{"asdkflj asgkdj"},
			requires: true,
		},
		{
			name:     "CREATE TABLE statement, already exists",
			stmts:    []string{"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"},
			requires: true,
		},
		{
			name:     "CREATE TABLE statement, does not exists",
			stmts:    []string{"CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"},
			requires: true,
		},
		{
			name:     "Single INSERT",
			stmts:    []string{"INSERT INTO foo(id, name) VALUES(1, 'fiona')"},
			requires: true,
		},
		{
			name:     "Single INSERT, non-existent table",
			stmts:    []string{"INSERT INTO qux(id, name) VALUES(1, 'fiona')"},
			requires: true,
		},
		{
			name:     "Single SELECT with implicit NONE",
			stmts:    []string{"SELECT * FROM foo"},
			requires: false,
		},
		{
			name:     "Single SELECT with NONE",
			stmts:    []string{"SELECT * FROM foo"},
			lvl:      command.QueryRequest_QUERY_REQUEST_LEVEL_NONE,
			requires: false,
		},
		{
			name:     "Single SELECT from non-existent table with NONE",
			stmts:    []string{"SELECT * FROM qux"},
			lvl:      command.QueryRequest_QUERY_REQUEST_LEVEL_NONE,
			requires: true,
		},
		{
			name:     "Double SELECT with NONE",
			stmts:    []string{"SELECT * FROM foo", "SELECT * FROM foo WHERE id = 1"},
			lvl:      command.QueryRequest_QUERY_REQUEST_LEVEL_NONE,
			requires: false,
		},
		{
			name:     "Single SELECT with STRONG",
			stmts:    []string{"SELECT * FROM foo"},
			lvl:      command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG,
			requires: true,
		},
		{
			name:     "Single SELECT with WEAK",
			stmts:    []string{"SELECT * FROM foo"},
			lvl:      command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK,
			requires: true,
		},
		{
			name:     "Mix queries and executes with NONE",
			stmts:    []string{"SELECT * FROM foo", "INSERT INTO foo(id, name) VALUES(1, 'fiona')"},
			lvl:      command.QueryRequest_QUERY_REQUEST_LEVEL_NONE,
			requires: true,
		},
		{
			name:     "Mix queries and executes with WEAK",
			stmts:    []string{"SELECT * FROM foo", "INSERT INTO foo(id, name) VALUES(1, 'fiona')"},
			lvl:      command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK,
			requires: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requires := s.RequiresLeader(executeQueryRequestFromStrings(tt.stmts, tt.lvl, false, false))
			if requires != tt.requires {
				t.Fatalf(" test %s failed, unexpected requires: expected %v, got %v", tt.name, tt.requires, requires)
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

func mustCreateTempFile() string {
	f, err := os.CreateTemp("", "rqlite-temp")
	if err != nil {
		panic("failed to create temporary file")
	}
	f.Close()
	return f.Name()
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

func executeRequestFromString(s string, timings, tx bool) *command.ExecuteRequest {
	return executeRequestFromStrings([]string{s}, timings, tx)
}

// executeRequestFromStrings converts a slice of strings into a command.ExecuteRequest
func executeRequestFromStrings(s []string, timings, tx bool) *command.ExecuteRequest {
	stmts := make([]*command.Statement, len(s))
	for i := range s {
		stmts[i] = &command.Statement{
			Sql: s[i],
		}
	}
	return &command.ExecuteRequest{
		Request: &command.Request{
			Statements:  stmts,
			Transaction: tx,
		},
		Timings: timings,
	}
}

func queryRequestFromString(s string, timings, tx bool) *command.QueryRequest {
	return queryRequestFromStrings([]string{s}, timings, tx)
}

// queryRequestFromStrings converts a slice of strings into a command.QueryRequest
func queryRequestFromStrings(s []string, timings, tx bool) *command.QueryRequest {
	stmts := make([]*command.Statement, len(s))
	for i := range s {
		stmts[i] = &command.Statement{
			Sql: s[i],
		}
	}
	return &command.QueryRequest{
		Request: &command.Request{
			Statements:  stmts,
			Transaction: tx,
		},
		Timings: timings,
	}
}

func executeQueryRequestFromString(s string, lvl command.QueryRequest_Level, timings, tx bool) *command.ExecuteQueryRequest {
	return executeQueryRequestFromStrings([]string{s}, lvl, timings, tx)
}

func executeQueryRequestFromStrings(s []string, lvl command.QueryRequest_Level, timings, tx bool) *command.ExecuteQueryRequest {
	stmts := make([]*command.Statement, len(s))
	for i := range s {
		stmts[i] = &command.Statement{
			Sql: s[i],
		}
	}
	return &command.ExecuteQueryRequest{
		Request: &command.Request{
			Statements:  stmts,
			Transaction: tx,
		},
		Timings: timings,
		Level:   lvl,
	}
}

func backupRequestSQL(leader bool) *command.BackupRequest {
	return &command.BackupRequest{
		Format: command.BackupRequest_BACKUP_REQUEST_FORMAT_SQL,
		Leader: leader,
	}
}

func backupRequestBinary(leader bool) *command.BackupRequest {
	return &command.BackupRequest{
		Format: command.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY,
		Leader: leader,
	}
}

func loadRequestFromFile(path string) *command.LoadRequest {
	return &command.LoadRequest{
		Data: mustReadFile(path),
	}
}

func joinRequest(id, addr string, voter bool) *command.JoinRequest {
	return &command.JoinRequest{
		Id:      id,
		Address: addr,
		Voter:   voter,
	}
}

func notifyRequest(id, addr string) *command.NotifyRequest {
	return &command.NotifyRequest{
		Id:      id,
		Address: addr,
	}
}

func removeNodeRequest(id string) *command.RemoveNodeRequest {
	return &command.RemoveNodeRequest{
		Id: id,
	}
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
