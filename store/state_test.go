package store

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
)

func Test_IsStaleRead(t *testing.T) {
	tests := []struct {
		Name               string
		LeaderLastContact  time.Time
		LastFSMUpdateTime  time.Time
		LastAppendedAtTime time.Time
		FSMIndex           uint64
		CommitIndex        uint64
		Freshness          time.Duration
		Strict             bool
		Exp                bool
	}{
		{
			Name:      "no freshness set",
			Freshness: 0,
			Exp:       false,
		},
		{
			Name:              "no freshness set, but clearly unfresh connection",
			LeaderLastContact: time.Now().Add(-1000 * time.Hour),
			Freshness:         0,
			Exp:               false,
		},
		{
			Name:              "freshness set, but not exceeded",
			LeaderLastContact: time.Now().Add(10 * time.Second),
			Freshness:         time.Minute,
			Exp:               false,
		},
		{
			Name:              "freshness set and exceeded",
			LeaderLastContact: time.Now().Add(-10 * time.Second),
			Freshness:         time.Second,
			Exp:               true,
		},
		{
			Name:              "freshness set and ok, strict is set, but no appended time",
			LeaderLastContact: time.Now(),
			Freshness:         10 * time.Second,
			Strict:            true,
			Exp:               false,
		},
		{
			Name:               "freshness set, is ok, strict is set, appended time exceeds, but applied index is up-to-date",
			LeaderLastContact:  time.Now(),
			LastFSMUpdateTime:  time.Now(),
			LastAppendedAtTime: time.Now().Add(-30 * time.Second),
			FSMIndex:           10,
			CommitIndex:        10,
			Freshness:          10 * time.Second,
			Strict:             true,
			Exp:                false,
		},
		{
			Name:               "freshness set, is ok, strict is set, appended time exceeds, applied index behind",
			LeaderLastContact:  time.Now(),
			LastFSMUpdateTime:  time.Now(),
			LastAppendedAtTime: time.Now().Add(-15 * time.Second),
			FSMIndex:           9,
			CommitIndex:        10,
			Freshness:          10 * time.Second,
			Strict:             true,
			Exp:                true,
		},
		{
			Name:               "freshness set, is ok, strict is set, appended time does not exceed, applied index is behind",
			LeaderLastContact:  time.Now(),
			LastFSMUpdateTime:  time.Now(),
			LastAppendedAtTime: time.Now(),
			FSMIndex:           9,
			CommitIndex:        10,
			Freshness:          time.Minute,
			Strict:             true,
			Exp:                false,
		},
		{
			Name:               "freshness set, is ok, appended time exceeds, applied index is behind, but strict not set",
			LeaderLastContact:  time.Now(),
			LastFSMUpdateTime:  time.Now(),
			LastAppendedAtTime: time.Now().Add(-10 * time.Second),
			FSMIndex:           9,
			CommitIndex:        10,
			Freshness:          5 * time.Second,
			Exp:                false,
		},
	}

	for i, tt := range tests {
		if got, exp := IsStaleRead(
			tt.LeaderLastContact,
			tt.LastFSMUpdateTime,
			tt.LastAppendedAtTime,
			tt.FSMIndex,
			tt.CommitIndex,
			tt.Freshness.Nanoseconds(),
			tt.Strict), tt.Exp; got != exp {
			t.Fatalf("unexpected result for IsStaleRead test #%d, %s\nexp: %v\ngot: %v", i+1, tt.Name, exp, got)
		}
	}
}

func Test_Store_IsNewNode(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	if !IsNewNode(s.raftDir) {
		t.Fatalf("new store is not new")
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

	if IsNewNode(s.raftDir) {
		t.Fatalf("new store is new")
	}
}

func Test_Store_HasData(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	h, err := HasData(s.raftDir)
	if err != nil {
		t.Fatalf("failed to check for data: %s", err.Error())
	}
	if h {
		t.Fatalf("new store has data")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Write some data.
	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
	}, false, false)
	_, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}

	// Close the store to unblock the Bolt database.
	s.Close(true)

	h, err = HasData(s.raftDir)
	if err != nil {
		t.Fatalf("failed to check for data: %s", err.Error())
	}
	if !h {
		t.Fatalf("store does not have data")
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
		qr.Level = proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
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
