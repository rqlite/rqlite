package store

import (
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/rqlite/rqlite/v10/command"
	"github.com/rqlite/rqlite/v10/command/proto"
	sql "github.com/rqlite/rqlite/v10/db"
	sqlite3 "modernc.org/sqlite/lib"
)

func mustWrite(t *testing.T, s *Store, sqlText string) {
	t.Helper()
	if _, _, err := s.Execute(context.Background(), executeRequestFromString(sqlText, false, false)); err != nil {
		t.Fatalf("%s: %s", sqlText, err)
	}
}

// Test_Changeset_FollowerRefuses runs the Leader's side of a write on a
// follower, which Execute never lets happen, to reach the steps that fail when
// the node is not the Leader.
func Test_Changeset_FollowerRefuses(t *testing.T) {
	stores := mustNewCluster(t, 2, true)
	mustWrite(t, stores[0], `CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)`)
	waitConverged(t, stores)
	f := stores[1]
	ctx := context.Background()
	insert := executeRequestFromString(`INSERT INTO foo(v) VALUES('x')`, false, false).Request
	read := executeRequestFromString(`SELECT 1`, false, false).Request

	// The barrier is the first step, and it needs the Leader.
	if res, _, ok, err := f.changesetApply(ctx, read, false, false); err == nil || !ok || res != nil {
		t.Fatalf("expected the barrier error alone, got %v %v %v", res, ok, err)
	}
	if got := f.changesetBarrierAt.Load(); got != 0 {
		t.Fatalf("a failed barrier was recorded for term %d", got)
	}

	// Once the barrier is known to be done for this term, it is not taken again.
	f.changesetBarrierAt.Store(f.raft.CurrentTerm())
	if err := f.changesetCaughtUp(); err != nil {
		t.Fatalf("barrier taken again within the term: %s", err)
	}
	// A read replicates nothing, but it is confirmed by the leadership quorum
	// before local rows are returned: a follower must refuse it.
	if res, idx, ok, err := f.changesetApply(ctx, read, false, false); !errors.Is(err, ErrNotLeader) || !ok || idx != 0 || res != nil {
		t.Fatalf("a read on a follower must be refused, got res=%v idx=%d ok=%v err=%v", res, idx, ok, err)
	}

	// A write is recorded, then Raft refuses it.
	before := dumpStore(t, f)
	if _, _, _, err := f.changesetApply(ctx, insert, false, false); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
	if dumpStore(t, f) != before {
		t.Fatal("a refused write changed the follower")
	}

	// A new term takes the barrier again.
	f.changesetBarrierAt.Store(f.raft.CurrentTerm() + 1)
	if err := f.changesetCaughtUp(); err == nil {
		t.Fatal("expected the barrier to be taken for a new term")
	}
}

func Test_Changeset_LeaderFaults(t *testing.T) {
	s := mustNewCluster(t, 1, true)[0]
	mustWrite(t, s, `CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)`)
	ctx := context.Background()
	insert := executeRequestFromString(`INSERT INTO foo(v) VALUES('x')`, false, false).Request
	read := executeRequestFromString(`SELECT 1`, false, false).Request
	empty := dumpStore(t, s)
	boom := errors.New("boom")

	// Nothing changed: nothing goes through Raft.
	idx, n := s.raft.AppliedIndex(), s.numChangesets.Load()
	if _, got, ok, err := s.changesetApply(ctx, read, false, false); err != nil || !ok || got != idx {
		t.Fatalf("read: idx=%d (want %d) ok=%v err=%v", got, idx, ok, err)
	}
	if s.raft.AppliedIndex() != idx || s.numChangesets.Load() != n {
		t.Fatal("a read was replicated")
	}

	// A failed marshal replicates nothing.
	realCmd := commandMarshal
	defer func() { commandMarshal = realCmd }()
	commandMarshal = func(*proto.Command) ([]byte, error) { return nil, boom }
	if _, _, ok, err := s.changesetApply(ctx, insert, false, false); !errors.Is(err, boom) || !ok {
		t.Fatalf("expected the marshal error, got ok=%v %v", ok, err)
	}
	commandMarshal = realCmd
	if s.raft.AppliedIndex() != idx || dumpStore(t, s) != empty {
		t.Fatal("a failed marshal changed something")
	}

	// A conflict at apply time is fatal on the node (the FSM stops the process,
	// which a test cannot observe): Test_CommandProcessor_Changeset covers it.

	// A database that cannot be recorded against is reported, not skipped.
	s.db.Close()
	if res, got, ok, err := s.changesetApply(ctx, insert, false, false); err == nil || !ok || got != 0 || res != nil {
		t.Fatalf("expected the capture error alone, got %v %d %v %v", res, got, ok, err)
	}
}

func mustChangesetCommand(t *testing.T, cs []byte) []byte {
	t.Helper()
	data, err := command.Marshal(&proto.Command{Type: proto.Command_COMMAND_TYPE_CHANGESET, SubCommand: cs})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func Test_CommandProcessor_Changeset(t *testing.T) {
	processor := NewCommandProcessor(log.New(io.Discard, "", 0), nil)
	open := func(t *testing.T) *sql.SwappableDB {
		db, err := sql.OpenSwappable(filepath.Join(t.TempDir(), "test.db"), sql.NewConfig(sql.ConnConfig{}, true))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		if _, _, _, err := processor.Process(mustMarshalWriteCommand(t, proto.Command_COMMAND_TYPE_EXECUTE,
			"CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB)"), db); err != nil {
			t.Fatal(err)
		}
		return db
	}
	capture := func(t *testing.T, db *sql.SwappableDB) []byte {
		_, cs, err := db.ExecuteCapture(context.Background(),
			&proto.Request{Statements: []*proto.Statement{{Sql: "INSERT INTO data VALUES (2, zeroblob(1048576))"}}}, false)
		if err != nil || len(cs) == 0 {
			t.Fatalf("capture: %d bytes, %v", len(cs), err)
		}
		return cs
	}

	t.Run("applied then conflicting", func(t *testing.T) {
		db := open(t)
		data := mustChangesetCommand(t, capture(t, db))
		_, mutated, resp, err := processor.Process(data, db)
		if err != nil || !mutated || resp.(*fsmGenericResponse).error != nil {
			t.Fatalf("apply: mutated=%v %v %v", mutated, resp, err)
		}
		// The same changeset again conflicts: not fatal, nothing mutated.
		_, mutated, resp, err = processor.Process(data, db)
		var fatal *sql.FatalError
		if !errors.As(err, &fatal) || !errors.Is(err, sql.ErrChangesetConflict) || mutated || resp != nil {
			t.Fatalf("conflict: mutated=%v %v %v", mutated, resp, err)
		}
	})

	t.Run("fatal", func(t *testing.T) {
		db := open(t)
		data := mustChangesetCommand(t, capture(t, db))
		rows, err := db.QueryStringStmt("PRAGMA page_count")
		if err != nil || len(rows) != 1 || rows[0].Error != "" {
			t.Fatalf("page count: %v %v", rows, err)
		}
		if _, _, _, err := processor.Process(mustMarshalWriteCommand(t, proto.Command_COMMAND_TYPE_EXECUTE,
			"PRAGMA max_page_count="+strconv.FormatInt(rows[0].Values[0].Parameters[0].GetI(), 10)), db); err != nil {
			t.Fatal(err)
		}
		_, mutated, _, err := processor.Process(data, db)
		var fatal *sql.FatalError
		if se := sql.NewSQLiteErrorFromError(err); !errors.As(err, &fatal) || se == nil || se.Code != sqlite3.SQLITE_FULL || mutated {
			t.Fatalf("expected a fatal SQLITE_FULL, got %T %v", err, err)
		}
	})
}
