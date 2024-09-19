package store

import (
	"os"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/rarchive"
)

func test_SingleNodeProvide(t *testing.T, vacuum, compress bool) {
	s0, ln := mustNewStore(t)
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

	tmpFd := mustCreateTempFD()
	defer os.Remove(tmpFd.Name())
	defer tmpFd.Close()
	provider := NewProvider(s0, vacuum, compress)
	if err := provider.Provide(tmpFd); err != nil {
		t.Fatalf("failed to provide SQLite data: %s", err.Error())
	}

	// Load the provided data into a new store and check it.
	s1, ln := mustNewStore(t)
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

	s1file := tmpFd.Name()
	if compress {
		unCompressedFile, err := rarchive.Gunzip(tmpFd.Name())
		if err != nil {
			t.Fatalf("failed to gunzip provided SQLite data: %s", err.Error())
		}
		s1file = unCompressedFile
	}

	err = s1.Load(loadRequestFromFile(s1file))
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
}

// Test_SingleNodeProvide tests that the Store correctly implements
// the Provide method.
func Test_SingleNodeProvide(t *testing.T) {
	t.Run("NoVacuumNoCompress", func(t *testing.T) {
		test_SingleNodeProvide(t, false, false)
	})
	t.Run("VacuumNoCompress", func(t *testing.T) {
		test_SingleNodeProvide(t, true, false)
	})
	t.Run("NoVacuumCompress", func(t *testing.T) {
		test_SingleNodeProvide(t, false, true)
	})
	t.Run("VacuumCompress", func(t *testing.T) {
		test_SingleNodeProvide(t, true, true)
	})
}

func Test_SingleNodeProvideLastIndex(t *testing.T) {
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

	tmpFile := mustCreateTempFile()
	defer os.Remove(tmpFile)
	provider := NewProvider(s, false, false)

	lm, err := provider.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err.Error())
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if _, err := s.WaitForAppliedFSM(2 * time.Second); err != nil {
		t.Fatalf("failed to wait for FSM to apply")
	}

	newLI, err := provider.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err.Error())
	}
	if newLI <= lm {
		t.Fatalf("last index should have changed")
	}
	lm = newLI

	// Try various queries and commands which should not change the database.
	qr := queryRequestFromString("SELECT * FROM foo", false, false)
	qr.Level = command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	_, err = s.Query(qr)
	if err != nil {
		t.Fatalf("failed to query leader node: %s", err.Error())
	}
	if _, err := s.WaitForAppliedFSM(2 * time.Second); err != nil {
		t.Fatalf("failed to wait for FSM to apply")
	}
	newLI, err = provider.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err.Error())
	}
	if newLI != lm {
		t.Fatalf("last index should not have changed")
	}
	lm = newLI

	if af, err := s.Noop("don't care"); err != nil || af.Error() != nil {
		t.Fatalf("failed to execute Noop")
	}
	newLI, err = provider.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err.Error())
	}
	if newLI != lm {
		t.Fatalf("last index should not have changed")
	}
	lm = newLI

	// Right now any "execute" will be assumed to change the database. It's possible
	// that by checking the Error field of the response we could avoid this.
	er = executeRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`, // Constraint violation.
	}, false, false)
	_, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	newLI, err = provider.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err.Error())
	}
	if newLI == lm {
		t.Fatalf("last index should changed even with constraint violation")
	}
	lm = newLI

	// This should change the database.
	er = executeRequestFromStrings([]string{
		`INSERT INTO foo(id, name) VALUES(2, "fiona")`,
	}, false, false)
	_, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if _, err := s.WaitForAppliedFSM(2 * time.Second); err != nil {
		t.Fatalf("failed to wait for FSM to apply")
	}
	newLI, err = provider.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err.Error())
	}
	if newLI <= lm {
		t.Fatalf("last index should have changed")
	}
}

// Test_SingleNodeProvideLastIndex_Restart tests that the Provider correctly implements
// the LastIndex method after a Store restart.
func Test_SingleNodeProvideLastIndex_Restart(t *testing.T) {
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

	tmpFile := mustCreateTempFile()
	defer os.Remove(tmpFile)
	provider := NewProvider(s, false, false)
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	if _, err := s.Execute(er); err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	if _, err := s.WaitForAppliedFSM(2 * time.Second); err != nil {
		t.Fatalf("failed to wait for FSM to apply")
	}

	lm, err := provider.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err.Error())
	}
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close store: %s", err.Error())
	}

	// Restart the store and check the last index is the same.
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}
	provider = NewProvider(s, false, false)
	testPoll(t, func() bool {
		newLI, err := provider.LastIndex()
		if err != nil {
			t.Fatalf("failed to get last index: %s", err.Error())
		}
		return lm == newLI
	}, 100*time.Millisecond, 5*time.Second)
}
