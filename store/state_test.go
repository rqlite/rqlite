package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

func Test_PragmaCheckRequest_Check(t *testing.T) {
	var p *PragmaCheckRequest
	if err := p.Check(); err != nil {
		t.Fatalf("expected nil error for nil PragmaCheckRequest")
	}

	tests := []struct {
		Name   string
		Stmts  []string
		ExpErr bool
	}{
		{
			Name:   "no statements",
			ExpErr: false,
		},
		{
			Name:   "SELECT",
			Stmts:  []string{"SELECT * FROM foo"},
			ExpErr: false,
		},
		{
			Name:   "non-breaking pragma and a SELECT",
			Stmts:  []string{"PRAGMA foo", "SELECT * FROM foo"},
			ExpErr: false,
		},
		{
			Name:   "Single disallowed pragma",
			Stmts:  []string{"PRAGMA wal_checkpoint(TRUNCATE)"},
			ExpErr: true,
		},
		{
			Name:   "Parenthesized assignment",
			Stmts:  []string{"PRAGMA wal_autocheckpoint(1)"},
			ExpErr: true,
		},
		{
			Name:   "Assignment after another SQL statement",
			Stmts:  []string{"SELECT 1; /* comment */ PRAGMA [wal_autocheckpoint](1)"},
			ExpErr: true,
		},
		{
			Name:   "Multiple statements including a trailing disallowed pragma",
			Stmts:  []string{"SELECT * FROM foo", "PRAGMA wal_checkpoint(TRUNCATE)"},
			ExpErr: true,
		},
		{
			Name:   "Multiple statements including a leading disallowed pragma",
			Stmts:  []string{"PRAGMA SYNCHRONOUS=NORMAL", "SELECT * FROM foo"},
			ExpErr: true,
		},
	}

	for i, tt := range tests {
		stmts := make([]*proto.Statement, len(tt.Stmts))
		for i, s := range tt.Stmts {
			stmts[i] = &proto.Statement{Sql: s}
		}
		p := &PragmaCheckRequest{Statements: stmts}

		err := p.Check()
		if tt.ExpErr {
			if err == nil {
				t.Fatalf("expected error for PragmaCheckRequest test #%d, %s", i+1, tt.Name)
			}
		} else {
			if err != nil {
				t.Fatalf("unexpected error for PragmaCheckRequest test #%d, %s: %s", i+1, tt.Name, err.Error())
			}
		}
	}
}

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
	_, _, err = s.Execute(context.Background(), er)
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
		qr := queryRequestFromString("SELECT * FROM foo", false, false, false)
		qr.Level = proto.ConsistencyLevel_NONE
		r, _, _, err := s.Query(context.Background(), qr)
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
	_, _, err := s.Execute(context.Background(), er)
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

	if fsutil.PathExists(peersPath) {
		t.Fatalf("Peers JSON exists at %s", peersPath)
	}
	if !fsutil.PathExists(peersInfo) {
		t.Fatalf("Peers info does not exist at %s", peersInfo)
	}
}

func Test_SingleNodeRecoverForeignKeysEnabled(t *testing.T) {
	s, ln := mustNewStoreFK(t)
	defer ln.Close()
	s.NoSnapshotOnClose = true // Keep the DELETE in the log for recovery to replay.
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err)
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("error waiting for leader: %s", err)
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY)`,
		`CREATE TABLE bar (fooid INTEGER NOT NULL PRIMARY KEY, FOREIGN KEY(fooid) REFERENCES foo(id))`,
		`INSERT INTO foo(id) VALUES(1)`,
		`INSERT INTO bar(fooid) VALUES(1)`,
	}, false, false)
	res, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to create foreign-key data: %s", err)
	}
	for _, r := range res {
		if r.GetError() != "" {
			t.Fatalf("failed to create foreign-key data: %s", r.GetError())
		}
	}
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err)
	}

	// Foreign-key enforcement rejects deleting the referenced parent.
	er = executeRequestFromString(`DELETE FROM foo WHERE id=1`, false, false)
	res, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute DELETE: %s", err)
	}
	if len(res) != 1 || res[0].GetError() != "FOREIGN KEY constraint failed" {
		t.Fatalf("unexpected DELETE results: %s", asJSON(res))
	}

	qr := queryRequestFromString(`SELECT (SELECT COUNT(*) FROM foo), (SELECT COUNT(*) FROM bar)`, false, false, false)
	qr.Level = proto.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(context.Background(), qr)
	if err != nil {
		t.Fatalf("failed to query before recovery: %s", err)
	}
	if len(r) != 1 || r[0].Error != "" {
		t.Fatalf("unexpected query results before recovery: %s", asJSON(r))
	}
	if exp, got := `[[1,1]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected parent/child counts before recovery: exp %s, got %s", exp, got)
	}
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err)
	}

	// Recover into a new Store with foreign-key enforcement still enabled.
	sR, lnR := mustNewStoreAtPathsLn(s.ID(), s.Path(), true)
	defer lnR.Close()
	peers := fmt.Sprintf(`[{"id": "%s", "address": "%s"}]`, sR.ID(), lnR.Addr().String())
	peersPath := filepath.Join(sR.Path(), "raft/peers.json")
	mustWriteFile(peersPath, peers)
	if err := sR.Open(); err != nil {
		t.Fatalf("failed to open recovered store: %s", err)
	}
	defer sR.Close(true)
	if _, err := sR.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("error waiting for leader on recovered node: %s", err)
	}

	r, _, _, err = sR.Query(context.Background(), qr)
	if err != nil {
		t.Fatalf("failed to query after recovery: %s", err)
	}
	if len(r) != 1 || r[0].Error != "" {
		t.Fatalf("unexpected query results after recovery: %s", asJSON(r))
	}
	if exp, got := `[[1,1]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected parent/child counts after recovery: exp %s, got %s", exp, got)
	}
	if fsutil.PathExists(peersPath) || !fsutil.PathExists(filepath.Join(sR.Path(), "raft/peers.info")) {
		t.Fatal("recovery did not rename peers.json to peers.info")
	}
}

func Test_SingleNodeRecoverForeignKeysDisabled(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	s.NoSnapshotOnClose = true // Keep the DELETE in the log for recovery to replay.
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err)
	}
	defer s.Close(true)
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("error waiting for leader: %s", err)
	}

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY)`,
		`CREATE TABLE bar (fooid INTEGER NOT NULL PRIMARY KEY, FOREIGN KEY(fooid) REFERENCES foo(id))`,
		`INSERT INTO foo(id) VALUES(1)`,
		`INSERT INTO bar(fooid) VALUES(1)`,
	}, false, false)
	res, _, err := s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to create foreign-key data: %s", err)
	}
	for _, r := range res {
		if r.GetError() != "" {
			t.Fatalf("failed to create foreign-key data: %s", r.GetError())
		}
	}
	if err := s.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err)
	}

	// With foreign keys disabled, deleting the parent leaves the child.
	er = executeRequestFromString(`DELETE FROM foo WHERE id=1`, false, false)
	res, _, err = s.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute DELETE: %s", err)
	}
	if len(res) != 1 || res[0].GetError() != "" {
		t.Fatalf("unexpected DELETE results: %s", asJSON(res))
	}

	qr := queryRequestFromString(`SELECT (SELECT COUNT(*) FROM foo), (SELECT COUNT(*) FROM bar)`, false, false, false)
	qr.Level = proto.ConsistencyLevel_STRONG
	r, _, _, err := s.Query(context.Background(), qr)
	if err != nil {
		t.Fatalf("failed to query before recovery: %s", err)
	}
	if len(r) != 1 || r[0].Error != "" {
		t.Fatalf("unexpected query results before recovery: %s", asJSON(r))
	}
	if exp, got := `[[0,1]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected parent/child counts before recovery: exp %s, got %s", exp, got)
	}
	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err)
	}

	// Recover into a new Store with foreign-key enforcement still disabled.
	sR, lnR := mustNewStoreAtPathsLn(s.ID(), s.Path(), false)
	defer lnR.Close()
	peers := fmt.Sprintf(`[{"id": "%s", "address": "%s"}]`, sR.ID(), lnR.Addr().String())
	peersPath := filepath.Join(sR.Path(), "raft/peers.json")
	mustWriteFile(peersPath, peers)
	if err := sR.Open(); err != nil {
		t.Fatalf("failed to open recovered store: %s", err)
	}
	defer sR.Close(true)
	if _, err := sR.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("error waiting for leader on recovered node: %s", err)
	}

	r, _, _, err = sR.Query(context.Background(), qr)
	if err != nil {
		t.Fatalf("failed to query after recovery: %s", err)
	}
	if len(r) != 1 || r[0].Error != "" {
		t.Fatalf("unexpected query results after recovery: %s", asJSON(r))
	}
	if exp, got := `[[0,1]]`, asJSON(r[0].Values); exp != got {
		t.Fatalf("unexpected parent/child counts after recovery: exp %s, got %s", exp, got)
	}
	if fsutil.PathExists(peersPath) || !fsutil.PathExists(filepath.Join(sR.Path(), "raft/peers.info")) {
		t.Fatal("recovery did not rename peers.json to peers.info")
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
		qr := queryRequestFromString("SELECT * FROM foo", false, false, false)
		qr.Level = proto.ConsistencyLevel_NONE
		r, _, _, err := s.Query(context.Background(), qr)
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
	_, _, err := s0.Execute(context.Background(), er)
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
	sR, srLn := mustNewStoreAtPathsLn(id, s0.Path(), true)

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

	if fsutil.PathExists(peersPath) {
		t.Fatalf("Peers JSON exists at %s", peersPath)
	}
	if !fsutil.PathExists(peersInfo) {
		t.Fatalf("Peers info does not exist at %s", peersInfo)
	}
}

// Test_SingleNodeRecoverNetworkChangeSnapshot tests a node recovery that
// involves a changed-network address, with snapshots underneath.
func Test_SingleNodeRecoverNetworkChangeSnapshot(t *testing.T) {
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

	queryTest := func(s *Store, c int) {
		t.Helper()
		qr := queryRequestFromString("SELECT COUNT(*) FROM foo", false, false, false)
		qr.Level = proto.ConsistencyLevel_STRONG
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

	er := executeRequestFromStrings([]string{
		`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`,
		`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
	}, false, false)
	_, _, err := s0.Execute(context.Background(), er)
	if err != nil {
		t.Fatalf("failed to execute on single node: %s", err.Error())
	}
	queryTest(s0, 1)

	for range 9 {
		er := executeRequestFromStrings([]string{
			`INSERT INTO foo(name) VALUES("fiona")`,
		}, false, false)
		if _, _, err := s0.Execute(context.Background(), er); err != nil {
			t.Fatalf("failed to execute on single node: %s", err.Error())
		}
	}
	queryTest(s0, 10)

	// Trigger a snapshot.
	if err := s0.Snapshot(0); err != nil {
		t.Fatalf("failed to snapshot single-node store: %s", err.Error())
	}

	id := s0.ID()
	if err := s0.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}

	// Create a new node, at the same path. Will presumably have a different
	// Raft network address, since they are randomly assigned.
	sR, srLn := mustNewStoreAtPathsLn(id, s0.Path(), true)

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

	if fsutil.PathExists(peersPath) {
		t.Fatalf("Peers JSON exists at %s", peersPath)
	}
	if !fsutil.PathExists(peersInfo) {
		t.Fatalf("Peers info does not exist at %s", peersInfo)
	}
}
