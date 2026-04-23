package cluster

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/cluster/proto"
	"github.com/rqlite/rqlite/v10/cluster/servicetest"
	command "github.com/rqlite/rqlite/v10/command/proto"
	pb "google.golang.org/protobuf/proto"
)

func Test_NewClient(t *testing.T) {
	c := NewClient(nil, 0)
	if c == nil {
		t.Fatal("expected client, got nil")
	}
}

func Test_ClientGetNodeMeta(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_GET_NODE_META {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		p, err = pb.Marshal(&proto.NodeMeta{
			Url:     "http://localhost:1234",
			Version: "1.0.0",
		})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	c.SetLocalVersion("1.0.0")
	meta, err := c.GetNodeMeta(context.Background(), srv.Addr(), noRetries, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := "http://localhost:1234", meta.Url; exp != got {
		t.Fatalf("unexpected addr, got %s, exp: %s", got, exp)
	}
	if exp, got := "1.0.0", meta.Version; exp != got {
		t.Fatalf("unexpected version, got %s, exp: %s", got, exp)
	}
}

func Test_ClientGetCommitIndex(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_GET_NODE_META {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		p, err = pb.Marshal(&proto.NodeMeta{
			CommitIndex: 5678,
		})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	idx, err := c.GetCommitIndex(context.Background(), srv.Addr(), noRetries, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	exp, got := uint64(5678), idx
	if exp != got {
		t.Fatalf("unexpected addr, got %d, exp: %d", got, exp)
	}
}

func Test_ClientExecute(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_EXECUTE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		er := c.GetExecuteRequest()
		if er == nil {
			t.Fatal("expected execute request, got nil")
		}
		if er.Request.Statements[0].Sql != "INSERT INTO foo (id) VALUES (1)" {
			t.Fatalf("unexpected statement, got %s", er.Request.Statements[0])
		}

		p, err = pb.Marshal(&proto.CommandExecuteResponse{RaftIndex: 1234})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, idx, err := c.Execute(context.Background(), executeRequestFromString("INSERT INTO foo (id) VALUES (1)"),
		srv.Addr(), nil, time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 1234 {
		t.Fatalf("unexpected raft index, got %d, exp: %d", idx, 1234)
	}
}

func Test_ClientQuery(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_QUERY {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		qr := c.GetQueryRequest()
		if qr == nil {
			t.Fatal("expected query request, got nil")
		}
		if qr.Request.Statements[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("unexpected statement, got %s", qr.Request.Statements[0])
		}

		p, err = pb.Marshal(&proto.CommandQueryResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, noRetries)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ClientRequest(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_REQUEST {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		er := c.GetExecuteQueryRequest()
		if er == nil {
			t.Fatal("expected execute query request, got nil")
		}
		if er.Request.Statements[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("unexpected statement, got %s", er.Request.Statements[0])
		}

		p, err = pb.Marshal(&proto.CommandRequestResponse{RaftIndex: 1234})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, idx, err := c.Request(context.Background(), executeQueryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 1234 {
		t.Fatalf("unexpected raft index, got %d, exp: %d", idx, 1234)
	}
}

func Test_ClientRemoveNode(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_REMOVE_NODE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		rnr := c.GetRemoveNodeRequest()
		if rnr == nil {
			t.Fatal("expected remove node request, got nil")
		}
		if rnr.Id != "node1" {
			t.Fatalf("unexpected node id, got %s", rnr.Id)
		}

		p, err = pb.Marshal(&proto.CommandRemoveNodeResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.RemoveNodeRequest{
		Id: "node1",
	}
	err := c.RemoveNode(context.Background(), req, srv.Addr(), nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ClientRemoveNodeTimeout(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_REMOVE_NODE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		rnr := c.GetRemoveNodeRequest()
		if rnr == nil {
			t.Fatal("expected remove node request, got nil")
		}
		if rnr.Id != "node1" {
			t.Fatalf("unexpected node id, got %s", rnr.Id)
		}

		// Don't write anything, so a timeout occurs.
		time.Sleep(5 * time.Second)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.RemoveNodeRequest{
		Id: "node1",
	}
	err := c.RemoveNode(context.Background(), req, srv.Addr(), nil, time.Second)
	if err == nil || !strings.Contains(err.Error(), "i/o timeout") {
		t.Fatalf("failed to receive expected error, got: %T %s", err, err)
	}
}

func Test_ClientJoinNode(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Connection error handling
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_JOIN {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		jnr := c.GetJoinRequest()
		if jnr == nil {
			t.Fatal("expected join node request, got nil")
		}
		if jnr.Address != "test-node-addr" {
			t.Fatalf("unexpected node address, got %s", jnr.Address)
		}

		p, err = pb.Marshal(&proto.CommandJoinResponse{})
		if err != nil {
			conn.Close()
			return
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.JoinRequest{
		Address: "test-node-addr",
	}
	err := c.Join(context.Background(), req, srv.Addr(), nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ClientJoinNodeRedirect(t *testing.T) {
	srv2 := servicetest.NewService()
	srv2.PersistentHandler = func(conn net.Conn) {
		defer conn.Close()
		for {
			c := readCommand(conn)
			if c == nil {
				return
			}
			if c.Type != proto.Command_COMMAND_TYPE_JOIN {
				t.Errorf("unexpected command type: %d", c.Type)
				return
			}

			p, err := pb.Marshal(&proto.CommandJoinResponse{})
			if err != nil {
				t.Errorf("failed to marshal join response: %s", err)
				return
			}
			if err := writeBytesWithLength(conn, p); err != nil {
				return
			}
		}
	}
	srv2.Start()
	defer srv2.Close()

	srv1 := servicetest.NewService()
	srv1.PersistentHandler = func(conn net.Conn) {
		defer conn.Close()
		for {
			c := readCommand(conn)
			if c == nil {
				return
			}
			if c.Type != proto.Command_COMMAND_TYPE_JOIN {
				t.Errorf("unexpected command type: %d", c.Type)
				return
			}

			p, err := pb.Marshal(&proto.CommandJoinResponse{
				Error:  "not leader",
				Leader: srv2.Addr(),
			})
			if err != nil {
				t.Errorf("failed to marshal join response: %s", err)
				return
			}
			if err := writeBytesWithLength(conn, p); err != nil {
				return
			}
		}
	}
	srv1.Start()
	defer srv1.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.JoinRequest{
		Address: "test-node-addr",
	}
	err := c.Join(context.Background(), req, srv1.Addr(), nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ClientJoinNodeTooManyRedirects(t *testing.T) {
	srv1 := servicetest.NewService()
	srv2 := servicetest.NewService()

	srv1.PersistentHandler = func(conn net.Conn) {
		defer conn.Close()
		for {
			c := readCommand(conn)
			if c == nil {
				return
			}
			if c.Type != proto.Command_COMMAND_TYPE_JOIN {
				t.Errorf("unexpected command type: %d", c.Type)
				return
			}

			p, err := pb.Marshal(&proto.CommandJoinResponse{
				Error:  "not leader",
				Leader: srv2.Addr(),
			})
			if err != nil {
				t.Errorf("failed to marshal join response: %s", err)
				return
			}
			if err := writeBytesWithLength(conn, p); err != nil {
				return
			}
		}
	}

	srv2.PersistentHandler = func(conn net.Conn) {
		defer conn.Close()
		for {
			c := readCommand(conn)
			if c == nil {
				return
			}
			if c.Type != proto.Command_COMMAND_TYPE_JOIN {
				t.Errorf("unexpected command type: %d", c.Type)
				return
			}

			p, err := pb.Marshal(&proto.CommandJoinResponse{
				Error:  "not leader",
				Leader: srv1.Addr(),
			})
			if err != nil {
				t.Errorf("failed to marshal join response: %s", err)
				return
			}
			if err := writeBytesWithLength(conn, p); err != nil {
				return
			}
		}
	}

	srv1.Start()
	defer srv1.Close()
	srv2.Start()
	defer srv2.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.JoinRequest{
		Address: "test-node-addr",
	}
	err := c.Join(context.Background(), req, srv1.Addr(), nil, time.Second)
	if err != ErrTooManyJoinRedirects {
		t.Fatalf("expected %v, got %v", ErrTooManyJoinRedirects, err)
	}
}

func Test_ClientRetry_SuccessFirstAttempt(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		c := readCommand(conn)
		if c == nil {
			return
		}
		p, err := pb.Marshal(&proto.CommandQueryResponse{})
		if err != nil {
			conn.Close()
			return
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, 0)
	if err != nil {
		t.Fatalf("expected success on first attempt, got: %s", err)
	}
	if c.numForcedNewConns.Load() != 0 {
		t.Fatalf("expected no forced new connections, got %d", c.numForcedNewConns.Load())
	}
}

func Test_ClientRetry_FailAllPoolThenFreshSucceeds(t *testing.T) {
	// Simulate a node that restarted: the first connection (from pool) fails,
	// but a fresh connection succeeds.
	var connCount int
	var mu sync.Mutex
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		mu.Lock()
		connCount++
		n := connCount
		mu.Unlock()

		c := readCommand(conn)
		if c == nil {
			return
		}

		// First connection fails (simulating stale pool connection reading response),
		// second succeeds.
		if n == 1 {
			conn.Close()
			return
		}
		p, err := pb.Marshal(&proto.CommandQueryResponse{})
		if err != nil {
			conn.Close()
			return
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, 0)
	if err != nil {
		t.Fatalf("expected fresh connection to succeed, got: %s", err)
	}
	if exp, got := int32(1), c.numForcedNewConns.Load(); exp != got {
		t.Fatalf("expected %d forced new connection, got %d", exp, got)
	}
}

func Test_ClientRetry_AllAttemptsFail(t *testing.T) {
	// Server immediately closes every connection — both pool and fresh should fail.
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		readCommand(conn)
		conn.Close()
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, 0)
	if err == nil {
		t.Fatal("expected error when all attempts fail")
	}
	if exp, got := int32(1), c.numForcedNewConns.Load(); exp != got {
		t.Fatalf("expected %d forced new connection, got %d", exp, got)
	}
}

func Test_ClientRetry_SuccessMidPool(t *testing.T) {
	// With maxRetries=3, fail the first 2 pool attempts, succeed on the 3rd.
	// The fresh dial should never be needed.
	var connCount int
	var mu sync.Mutex
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		mu.Lock()
		connCount++
		n := connCount
		mu.Unlock()

		c := readCommand(conn)
		if c == nil {
			return
		}
		if n <= 2 {
			conn.Close()
			return
		}
		p, err := pb.Marshal(&proto.CommandQueryResponse{})
		if err != nil {
			conn.Close()
			return
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, 3)
	if err != nil {
		t.Fatalf("expected success on 3rd pool attempt, got: %s", err)
	}
	if exp, got := int32(0), c.numForcedNewConns.Load(); exp != got {
		t.Fatalf("expected %d forced new connection, got %d", exp, got)
	}
}

func Test_ClientRetry_ZeroMaxRetries_NoInfiniteLoop(t *testing.T) {
	// Regression test: with maxRetries=0, a failing server must not cause
	// an infinite retry loop. The function must return an error promptly.
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		readCommand(conn)
		conn.Close()
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := c.Query(ctx, queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, 0)
	if err == nil {
		t.Fatal("expected error with maxRetries=0 and failing server")
	}
	if ctx.Err() != nil {
		t.Fatal("retry loop did not terminate before context timeout — possible infinite loop")
	}
	if exp, got := int32(1), c.numForcedNewConns.Load(); exp != got {
		t.Fatalf("expected %d forced new connection, got %d", exp, got)
	}
}

func Test_ClientRetry_ContextCanceled(t *testing.T) {
	// Server is slow, context is canceled before retry completes.
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		readCommand(conn)
		time.Sleep(5 * time.Second)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, _, err := c.Query(ctx, queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, 3)
	if err == nil {
		t.Fatal("expected error when context is canceled")
	}
}

func readCommand(conn net.Conn) *proto.Command {
	b := make([]byte, protoBufferLengthSize)
	_, err := io.ReadFull(conn, b)
	if err != nil {
		return nil
	}
	sz := binary.LittleEndian.Uint64(b[0:])
	p := make([]byte, sz)
	_, err = io.ReadFull(conn, p)
	if err != nil {
		return nil
	}
	c := &proto.Command{}
	err = pb.Unmarshal(p, c)
	if err != nil {
		return nil
	}
	return c
}

type simpleDialer struct {
}

func (s *simpleDialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func Test_ClientBroadcast(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_HIGHWATER_MARK_UPDATE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		br := c.GetHighwaterMarkUpdateRequest()
		if br == nil {
			t.Fatal("expected highwater mark update request, got nil")
		}
		if br.NodeId != "node1" {
			t.Fatalf("unexpected node_id, got %s", br.NodeId)
		}
		if br.HighwaterMark != 12345 {
			t.Fatalf("unexpected highwater_mark, got %d", br.HighwaterMark)
		}

		p, err = pb.Marshal(&proto.HighwaterMarkUpdateResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	c.SetLocal("node1", nil) // Set local node address to match test expectation
	responses, err := c.BroadcastHWM(context.Background(), 12345, 0, time.Second, srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	resp, exists := responses[srv.Addr()]
	if !exists {
		t.Fatalf("response for %s not found", srv.Addr())
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error in response: %s", resp.Error)
	}
}

func Test_ClientBroadcast_MultipleNodes(t *testing.T) {
	// Create multiple test services
	srv1 := servicetest.NewService()
	srv2 := servicetest.NewService()

	handler := func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_HIGHWATER_MARK_UPDATE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}

		p, err = pb.Marshal(&proto.HighwaterMarkUpdateResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}

	srv1.Handler = handler
	srv2.Handler = handler

	srv1.Start()
	srv2.Start()
	defer srv1.Close()
	defer srv2.Close()

	c := NewClient(&simpleDialer{}, 0)
	c.SetLocal("test-node", nil) // Set local node address to match test expectation
	responses, err := c.BroadcastHWM(context.Background(), 999, 0, time.Second, srv1.Addr(), srv2.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	for addr, resp := range responses {
		if resp.Error != "" {
			t.Fatalf("unexpected error in response from %s: %s", addr, resp.Error)
		}
	}
}

func Test_ClientBroadcast_EmptyNodeList(t *testing.T) {
	c := NewClient(&simpleDialer{}, 0)
	responses, err := c.BroadcastHWM(context.Background(), 1, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 0 {
		t.Fatalf("expected 0 responses, got %d", len(responses))
	}
}

func Test_ClientBroadcast_WithError(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_HIGHWATER_MARK_UPDATE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}

		p, err = pb.Marshal(&proto.HighwaterMarkUpdateResponse{Error: "test error"})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	c.SetLocal("node1", nil) // Set local node address to match test expectation
	responses, err := c.BroadcastHWM(context.Background(), 12345, 0, time.Second, srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	resp, exists := responses[srv.Addr()]
	if !exists {
		t.Fatalf("response for %s not found", srv.Addr())
	}
	if resp.Error != "test error" {
		t.Fatalf("expected 'test error', got '%s'", resp.Error)
	}
}
