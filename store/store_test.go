package store

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/command/encoding"
	"github.com/rqlite/rqlite/testdata/chinook"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func Test_StoreSingleNodeNotOpen(t *testing.T) {
	s, ln := mustNewStore(t, true)
	defer s.Close(true)
	defer ln.Close()

	a, err := s.LeaderAddr()
	if err != nil {
		t.Fatalf("failed to get leader address: %s", err.Error())
	}
	if a != "" {
		t.Fatalf("non-empty Leader return for non-open store: %s", a)
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

func Test_OpenStoreSingleNode(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func Test_OpenStoreCloseSingleNode(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func Test_StoreLeaderObservation(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func Test_StoreReady(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func Test_SingleNodeInMemExecuteQuery(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

// Test_SingleNodeInMemExecuteQueryFail ensures database level errors are presented by the store.
func Test_SingleNodeInMemExecuteQueryFail(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func Test_SingleNodeFileExecuteQuery(t *testing.T) {
	s, ln := mustNewStore(t, false)
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

func Test_SingleNodeExecuteQueryTx(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

// Test_SingleNodeInMemFK tests that basic foreign-key related functionality works.
func Test_SingleNodeInMemFK(t *testing.T) {
	s, ln := mustNewStoreFK(t, true)
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

// Test_SingleNodeSQLitePath ensures that basic functionality works when the SQLite database path
// is explicitly specificed.
func Test_SingleNodeSQLitePath(t *testing.T) {
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

func Test_SingleNodeBackupBinary(t *testing.T) {
	t.Parallel()

	s, ln := mustNewStore(t, false)
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

	f, err := ioutil.TempFile("", "rqlite-baktest-")
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(f.Name())
	t.Logf("backup file is %s", f.Name())

	if err := s.Backup(backupRequestBinary(true), f); err != nil {
		t.Fatalf("Backup failed %s", err.Error())
	}

	// Check the backed up data by reading back up file, underlying SQLite file,
	// and comparing the two.
	bkp, err := ioutil.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("Backup Failed: unable to read backup file, %s", err.Error())
	}

	dbFile, err := ioutil.ReadFile(filepath.Join(s.Path(), sqliteFile))
	if err != nil {
		t.Fatalf("Backup Failed: unable to read source SQLite file, %s", err.Error())
	}

	if ret := bytes.Compare(bkp, dbFile); ret != 0 {
		t.Fatalf("Backup Failed: backup bytes are not same")
	}
}

func Test_SingleNodeBackupText(t *testing.T) {
	t.Parallel()

	s, ln := mustNewStore(t, true)
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

	f, err := ioutil.TempFile("", "rqlite-baktest-")
	if err != nil {
		t.Fatalf("Backup Failed: unable to create temp file, %s", err.Error())
	}
	defer os.Remove(f.Name())
	s.logger.Printf("backup file is %s", f.Name())

	if err := s.Backup(backupRequestSQL(true), f); err != nil {
		t.Fatalf("Backup failed %s", err.Error())
	}

	// Check the backed up data
	bkp, err := ioutil.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("Backup Failed: unable to read backup file, %s", err.Error())
	}
	if ret := bytes.Compare(bkp, []byte(dump)); ret != 0 {
		t.Fatalf("Backup Failed: backup bytes are not same")
	}
}

func Test_SingleNodeSingleCommandTrigger(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func Test_SingleNodeLoadText(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func Test_SingleNodeLoadTextNoStatements(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func Test_SingleNodeLoadTextEmpty(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func Test_SingleNodeLoadTextChinook(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func Test_SingleNodeLoadBinary(t *testing.T) {
	s, ln := mustNewStore(t, true)
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
	s, ln := mustNewStore(t, true)
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

func Test_SingleNodeProvide(t *testing.T) {
	for _, inmem := range []bool{
		true,
		false,
	} {
		func() {
			s0, ln := mustNewStore(t, inmem)
			defer ln.Close()

			if err := s0.Open(); err != nil {
				t.Fatalf("failed to open single-node store: %s", err.Error())
			}
			if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
				t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
			}
			defer s0.Close(true)
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

			tempFile := mustCreateTempFile()
			defer os.Remove(tempFile)
			err = s0.Provide(tempFile)
			if err != nil {
				t.Fatalf("store failed to provide: %s", err.Error())
			}

			// Load the provided data into a new store and check it.
			s1, ln := mustNewStore(t, true)
			defer ln.Close()

			if err := s1.Open(); err != nil {
				t.Fatalf("failed to open single-node store: %s", err.Error())
			}
			if err := s1.Bootstrap(NewServer(s1.ID(), s1.Addr(), true)); err != nil {
				t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
			}
			defer s1.Close(true)
			if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
				t.Fatalf("Error waiting for leader: %s", err)
			}

			err = s1.Load(loadRequestFromFile(tempFile))
			if err != nil {
				t.Fatalf("failed to load provided SQLite data: %s", err.Error())
			}
			qr = queryRequestFromString("SELECT * FROM foo", false, false)
			qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
			r, err = s1.Query(qr)
			if err != nil {
				t.Fatalf("failed to query leader node: %s", err.Error())
			}
			if exp, got := `["id","name"]`, asJSON(r[0].Columns); exp != got {
				t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
			}
			if exp, got := `[[1,"fiona"]]`, asJSON(r[0].Values); exp != got {
				t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
			}
		}()
	}
}

func Test_SingleNodeInMemProvideNoData(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

	tmpFile := mustCreateTempFile()
	defer os.Remove(tmpFile)
	err := s.Provide(tmpFile)
	if err != nil {
		t.Fatalf("store failed to provide: %s", err.Error())
	}
}

// Test_SingleNodeRecoverNoChange tests a node recovery that doesn't
// actually change anything.
func Test_SingleNodeRecoverNoChange(t *testing.T) {
	s, ln := mustNewStore(t, true)
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
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Set up for Recovery during open
	peers := fmt.Sprintf(`[{"id": "%s","address": "%s"}]`, s.ID(), s.Addr())
	peersPath := filepath.Join(s.Path(), "/raft/peers.json")
	peersInfo := filepath.Join(s.Path(), "/raft/peers.info")
	mustWriteFile(peersPath, peers)
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
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
	s0, ln0 := mustNewStore(t, true)
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
	sR, srLn := mustNewStoreAtPathsLn(id, s0.Path(), "", true, false)
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
	s0, ln0 := mustNewStore(t, true)
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
	sR, srLn := mustNewStoreAtPathsLn(id, s0.Path(), "", true, false)
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
	s0, ln0 := mustNewStore(t, true)
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
	s, ln := mustNewStore(t, true)
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
	s, ln := mustNewStore(t, true)
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

func Test_MultiNodeJoinRemove(t *testing.T) {
	s0, ln0 := mustNewStore(t, true)
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

	s1, ln1 := mustNewStore(t, true)
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

	got, err := s1.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader address on follower: %s", err.Error())
	}
	// Check leader state on follower.
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
}

func Test_MultiNodeStepdown(t *testing.T) {
	s0, ln0 := mustNewStore(t, true)
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

	s1, ln1 := mustNewStore(t, true)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t, true)
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

	// Check for new leader.
	nl, err := s2.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	if nl == s0.Addr() {
		t.Fatalf("leader address not changed, was %s, is %s", s0.Addr(), nl)
	}
}

func Test_MultiNodeStoreNotifyBootstrap(t *testing.T) {
	s0, ln0 := mustNewStore(t, true)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)

	s1, ln1 := mustNewStore(t, true)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t, true)
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
	leader0, err := s0.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader: %s", err.Error())
	}
	nodes, err := s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}
	if len(nodes) != 3 {
		t.Fatalf("size of bootstrapped cluster is not correct")
	}
	leader1, err := s1.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader: %s", err.Error())
	}
	nodes, err = s1.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}
	if len(nodes) != 3 {
		t.Fatalf("size of bootstrapped cluster is not correct")
	}
	leader2, err := s2.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("failed to get leader: %s", err.Error())
	}
	nodes, err = s2.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}
	if len(nodes) != 3 {
		t.Fatalf("size of bootstrapped cluster is not correct")
	}

	if leader0 != leader1 || leader0 != leader2 {
		t.Fatalf("leader not the same on each node")
	}

	// Calling Notify() on a node that is part of a cluster should
	// be a no-op.
	if err := s0.Notify(notifyRequest(s1.ID(), ln1.Addr().String())); err != nil {
		t.Fatalf("failed to notify store that is part of cluster: %s", err.Error())
	}
}

func Test_MultiNodeJoinNonVoterRemove(t *testing.T) {
	s0, ln0 := mustNewStore(t, true)
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

	s1, ln1 := mustNewStore(t, true)
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
	s0, ln0 := mustNewStore(t, true)
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

	s1, ln1 := mustNewStore(t, true)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	s2, ln2 := mustNewStore(t, true)
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
	s0, ln0 := mustNewStore(t, true)
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
	s0, ln0 := mustNewStore(t, true)
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

	s1, ln1 := mustNewStore(t, true)
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
		t.Fatalf("freshness violating query didn't returned wrong error: %s", err.Error())
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
}

func Test_StoreLogTruncationMultinode(t *testing.T) {
	s0, ln0 := mustNewStore(t, true)
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

	nSnaps := stats.Get(numSnaphots).String()

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
		return stats.Get(numSnaphots).String() != nSnaps
	}
	testPoll(t, f, 100*time.Millisecond, 2*time.Second)

	// Do one more execute, to ensure there is at least one log not snapshot.
	// Without this, there is no guaratnee fsmIndex will be set on s1.
	_, err := s0.Execute(executeRequestFromString(`INSERT INTO foo(id, name) VALUES(6, "fiona")`, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Fire up new node and ensure it picks up all changes. This will
	// involve getting a snapshot and truncated log.
	s1, ln1 := mustNewStore(t, true)
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

func Test_SingleNodeSnapshotOnDisk(t *testing.T) {
	s, ln := mustNewStore(t, false)
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
	f, err := s.Snapshot()
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
	if err := s.Restore(snapFile); err != nil {
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

func Test_SingleNodeSnapshotInMem(t *testing.T) {
	s, ln := mustNewStore(t, true)
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
	f, err := s.Snapshot()
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
	if err := s.Restore(snapFile); err != nil {
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

	// Write a record, ensuring it works and can be queried back i.e. that
	// the system remains functional.
	_, err = s.Execute(executeRequestFromString(`INSERT INTO foo(id, name) VALUES(2, "fiona")`, false, false))
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	r, err = s.Query(queryRequestFromString("SELECT COUNT(*) FROM foo", false, false))
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["COUNT(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[2]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeRestoreNoncompressed(t *testing.T) {
	s, ln := mustNewStore(t, false)
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

	// Check restoration from a pre-compressed SQLite database snap.
	// This is to test for backwards compatilibty of this code.
	f, err := os.Open(filepath.Join("testdata", "noncompressed-sqlite-snap.bin"))
	if err != nil {
		t.Fatalf("failed to open snapshot file: %s", err.Error())
	}
	if err := s.Restore(f); err != nil {
		t.Fatalf("failed to restore noncompressed snapshot from disk: %s", err.Error())
	}

	// Ensure database is back in the expected state.
	r, err := s.Query(queryRequestFromString("SELECT count(*) FROM foo", false, false))
	if err != nil {
		t.Fatalf("failed to query single node: %s", err.Error())
	}
	if exp, got := `["count(*)"]`, asJSON(r[0].Columns); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[5000]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_SingleNodeNoop(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

	if err := s.Noop("1"); err != nil {
		t.Fatalf("failed to write noop command: %s", err.Error())
	}
	if s.numNoops != 1 {
		t.Fatalf("noop count is wrong, got: %d", s.numNoops)
	}
}

func Test_IsLeader(t *testing.T) {
	s, ln := mustNewStore(t, true)
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
	s, ln := mustNewStore(t, true)
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

func Test_State(t *testing.T) {
	s, ln := mustNewStore(t, true)
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

func mustNewStoreAtPathsLn(id, dataPath, sqlitePath string, inmem, fk bool) (*Store, net.Listener) {
	cfg := NewDBConfig(inmem)
	cfg.FKConstraints = fk
	cfg.OnDiskPath = sqlitePath

	ln := mustMockLister("localhost:0")
	s := New(ln, &Config{
		DBConf: cfg,
		Dir:    dataPath,
		ID:     id,
	})
	if s == nil {
		panic("failed to create new store")
	}
	return s, ln
}

func mustNewStore(t *testing.T, inmem bool) (*Store, net.Listener) {
	return mustNewStoreAtPathsLn(randomString(), t.TempDir(), "", inmem, false)
}

func mustNewStoreFK(t *testing.T, inmem bool) (*Store, net.Listener) {
	return mustNewStoreAtPathsLn(randomString(), t.TempDir(), "", inmem, true)
}

func mustNewStoreSQLitePath(t *testing.T) (*Store, net.Listener, string) {
	dataDir := t.TempDir()
	sqliteDir := t.TempDir()
	sqlitePath := filepath.Join(sqliteDir, "explicit-path.db")
	s, ln := mustNewStoreAtPathsLn(randomString(), dataDir, sqlitePath, false, true)
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

type mockListener struct {
	ln net.Listener
}

func mustMockLister(addr string) Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic("failed to create new listner")
	}
	return &mockListener{ln}
}

func (m *mockListener) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}

func (m *mockListener) Accept() (net.Conn, error) { return m.ln.Accept() }

func (m *mockListener) Close() error { return m.ln.Close() }

func (m *mockListener) Addr() net.Addr { return m.ln.Addr() }

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
	b, err := ioutil.ReadFile(path)
	if err != nil {
		panic("failed to read file")
	}
	return b
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

func executeRequestFromString(s string, timings, tx bool) *command.ExecuteRequest {
	return executeRequestFromStrings([]string{s}, timings, tx)
}

// queryRequestFromStrings converts a slice of strings into a command.ExecuteRequest
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

func randomString() string {
	var output strings.Builder
	chars := "abcdedfghijklmnopqrstABCDEFGHIJKLMNOP"

	for i := 0; i < 20; i++ {
		random := rand.Intn(len(chars))
		randomChar := chars[random]
		output.WriteString(string(randomChar))
	}
	return output.String()
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
