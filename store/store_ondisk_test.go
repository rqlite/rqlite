package store

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/db"
)

func Test_SingleNodeOnDiskFileExecuteQuery(t *testing.T) {
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

func Test_SingleNodeOnDiskBackupBinary(t *testing.T) {
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

func Test_SingleNodeOnDiskRestoreNoncompressed(t *testing.T) {
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

func Test_SingleNodeInMemOnDiskProvide(t *testing.T) {
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
