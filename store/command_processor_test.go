package store

import (
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"
	"github.com/rqlite/rqlite/v10/command"
	"github.com/rqlite/rqlite/v10/command/proto"
	sql "github.com/rqlite/rqlite/v10/db"
)

func Test_ExecuteQueryResponsesMutation(t *testing.T) {
	var e ExecuteQueryResponses
	if e.Mutation() {
		t.Fatalf("expected no mutations for empty ExecuteQueryResponses")
	}
}

func Test_ExecuteQueryResponsesMutation_Check(t *testing.T) {
	eqr0 := &proto.ExecuteQueryResponse{
		Mutated: true,
		Result: &proto.ExecuteQueryResponse_E{
			E: &proto.ExecuteResult{
				RowsAffected: 0,
			},
		},
	}
	eqr1 := &proto.ExecuteQueryResponse{
		Mutated: true,
		Result: &proto.ExecuteQueryResponse_E{
			E: &proto.ExecuteResult{
				RowsAffected: 1,
			},
		},
	}
	qqr := &proto.ExecuteQueryResponse{
		Result: &proto.ExecuteQueryResponse_Q{
			Q: &proto.QueryRows{
				Columns: []string{"foo"},
				Types:   []string{"text"},
			},
		},
	}

	e := ExecuteQueryResponses{eqr0}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteQueryResponses{eqr1}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteQueryResponses{eqr0, eqr1}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteQueryResponses{eqr0, eqr1, qqr}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteQueryResponses{eqr0, qqr}
	if !e.Mutation() {
		t.Fatalf("expected mutations")
	}
	e = ExecuteQueryResponses{qqr}
	if e.Mutation() {
		t.Fatalf("expected no mutations")
	}
	e = ExecuteQueryResponses{qqr, qqr}
	if e.Mutation() {
		t.Fatalf("expected no mutations")
	}
}

func Test_ExecuteQueryResponsesMutation_Returning(t *testing.T) {
	e := ExecuteQueryResponses{
		{Result: &proto.ExecuteQueryResponse_Q{Q: &proto.QueryRows{}}},
		{
			Mutated: true,
			Result:  &proto.ExecuteQueryResponse_Q{Q: &proto.QueryRows{}},
		},
	}
	if !e.Mutation() {
		t.Fatal("expected mutation for write returning rows")
	}
}

// Test_CommandProcessor_FatalError verifies that Execute and ExecuteQuery return
// SQLITE_FULL as a fatal application error. Successful writes and ordinary
// constraint violations must return responses without an application error.
func Test_CommandProcessor_FatalError(t *testing.T) {
	for _, cmdType := range []proto.Command_Type{
		proto.Command_COMMAND_TYPE_EXECUTE, proto.Command_COMMAND_TYPE_EXECUTE_QUERY,
	} {
		t.Run(cmdType.String(), func(t *testing.T) {
			db, err := sql.OpenSwappable(filepath.Join(t.TempDir(), "test.db"), nil, false, true, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			processor := NewCommandProcessor(log.New(io.Discard, "", 0), nil)
			process := func(statement string) *fsmExecuteQueryResponse {
				t.Helper()
				_, _, response, err := processor.Process(mustMarshalWriteCommand(t, cmdType, statement), db)
				if err != nil {
					t.Fatalf("unexpected application error: %v", err)
				}
				return response.(*fsmExecuteQueryResponse)
			}

			for _, statement := range []string{
				"CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB)",
				"INSERT INTO data VALUES (1, NULL)",
			} {
				response := process(statement)
				if response.error != nil || len(response.results) != 1 || response.results[0].GetError() != "" {
					t.Fatalf("setup failed: %+v", response)
				}
			}
			response := process("INSERT INTO data VALUES (1, NULL)")
			if response.error != nil || len(response.results) != 1 || response.results[0].GetError() == "" {
				t.Fatalf("expected ordinary constraint error: %+v", response)
			}

			// Force a real SQLite capacity failure without filling the host disk.
			rows, err := db.QueryStringStmt("PRAGMA page_count")
			if err != nil || len(rows) != 1 || rows[0].Error != "" {
				t.Fatalf("page count failed: %v, %v", rows, err)
			}
			pages := rows[0].Values[0].Parameters[0].GetI()
			response = process(fmt.Sprintf("PRAGMA max_page_count=%d", pages))
			if response.error != nil || len(response.results) != 1 || response.results[0].GetError() != "" {
				t.Fatalf("setting page limit failed: %+v", response)
			}
			_, _, _, err = processor.Process(mustMarshalWriteCommand(t, cmdType,
				"INSERT INTO data VALUES (2, zeroblob(1048576))"), db)
			var fatal *sql.FatalError
			var sqliteErr sqlite3.Error
			if !errors.As(err, &fatal) || !errors.As(err, &sqliteErr) || sqliteErr.Code != sqlite3.ErrFull {
				t.Fatalf("expected fatal SQLITE_FULL application error, got %T: %v", err, err)
			}
		})
	}
}

// Test_CommandProcessor_MalformedCommand verifies that command decoding failures
// return application errors, allowing the caller to decide how to terminate.
func Test_CommandProcessor_MalformedCommand(t *testing.T) {
	processor := NewCommandProcessor(log.New(io.Discard, "", 0), nil)
	invalid := []byte{0xff}
	if _, _, _, err := processor.Process(invalid, nil); err == nil {
		t.Fatal("expected malformed command error")
	}
	for _, cmdType := range []proto.Command_Type{
		proto.Command_COMMAND_TYPE_QUERY, proto.Command_COMMAND_TYPE_EXECUTE,
		proto.Command_COMMAND_TYPE_EXECUTE_QUERY, proto.Command_COMMAND_TYPE_LOAD,
		proto.Command_COMMAND_TYPE_LOAD_CHUNK,
	} {
		t.Run(cmdType.String(), func(t *testing.T) {
			data, err := command.Marshal(&proto.Command{Type: cmdType, SubCommand: invalid})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := processor.Process(data, nil); err == nil {
				t.Fatal("expected malformed subcommand error")
			}
		})
	}
}

// Test_RecoverNode_FatalCommand verifies that replay stops on a fatal database
// failure without snapshotting the incomplete state or compacting the log.
func Test_RecoverNode_FatalCommand(t *testing.T) {
	logs := raft.NewInmemStore()
	statements := []string{
		"CREATE TABLE data (value BLOB)",
		"PRAGMA max_page_count=2",
		"INSERT INTO data VALUES (zeroblob(1048576))",
	}
	for i, statement := range statements {
		if err := logs.StoreLog(&raft.Log{Index: uint64(i + 1), Term: 1, Type: raft.LogCommand,
			Data: mustMarshalWriteCommand(t, proto.Command_COMMAND_TYPE_EXECUTE, statement)}); err != nil {
			t.Fatal(err)
		}
	}
	snaps := raft.NewInmemSnapshotStore()
	_, transport := raft.NewInmemTransport("127.0.0.1:4002")
	defer transport.Close()

	err := RecoverNode(t.TempDir(), &DBConfig{}, log.New(io.Discard, "", 0), logs, nil, snaps, transport,
		raft.Configuration{Servers: []raft.Server{{ID: "node1", Address: "127.0.0.1:4002", Suffrage: raft.Voter}}})
	var fatal *sql.FatalError
	if !errors.As(err, &fatal) {
		t.Fatalf("expected fatal replay error, got %v", err)
	}
	if snapshots, err := snaps.List(); err != nil || len(snapshots) != 0 {
		t.Fatalf("unexpected recovery snapshot: %v, %v", snapshots, err)
	}
	if last, err := logs.LastIndex(); err != nil || last != uint64(len(statements)) {
		t.Fatalf("log was changed after failed recovery: last=%d, error=%v", last, err)
	}
}

func mustMarshalWriteCommand(t *testing.T, cmdType proto.Command_Type, statement string) []byte {
	t.Helper()
	req := &proto.Request{Statements: []*proto.Statement{{Sql: statement}}}
	var request command.Requester = &proto.ExecuteRequest{Request: req}
	if cmdType == proto.Command_COMMAND_TYPE_EXECUTE_QUERY {
		request = &proto.ExecuteQueryRequest{Request: req}
	}
	sub, compressed, err := command.NewRequestMarshaler().Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	data, err := command.Marshal(&proto.Command{Type: cmdType, SubCommand: sub, Compressed: compressed})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
