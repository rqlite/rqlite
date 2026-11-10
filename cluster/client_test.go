package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
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

func Test_ClientDemoteNode(t *testing.T) {
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
		if c.Type != proto.Command_COMMAND_TYPE_DEMOTE_NODE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		dnr := c.GetDemoteNodeRequest()
		if dnr == nil {
			t.Fatal("expected demote node request, got nil")
		}
		if dnr.Id != "node1" {
			t.Fatalf("unexpected node id, got %s", dnr.Id)
		}

		p, err = pb.Marshal(&proto.CommandDemoteNodeResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.DemoteNodeRequest{
		Id: "node1",
	}
	err := c.DemoteNode(context.Background(), req, srv.Addr(), nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ClientDemoteNodeTimeout(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_DEMOTE_NODE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		dnr := c.GetDemoteNodeRequest()
		if dnr == nil {
			t.Fatal("expected demote node request, got nil")
		}
		if dnr.Id != "node1" {
			t.Fatalf("unexpected node id, got %s", dnr.Id)
		}

		// Don't write anything, so a timeout occurs.
		time.Sleep(5 * time.Second)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.DemoteNodeRequest{
		Id: "node1",
	}
	err := c.DemoteNode(context.Background(), req, srv.Addr(), nil, time.Second)
	if err == nil || !strings.Contains(err.Error(), "i/o timeout") {
		t.Fatalf("failed to receive expected error, got: %T %s", err, err)
	}
}

func Test_ClientPromoteNode(t *testing.T) {
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
		if c.Type != proto.Command_COMMAND_TYPE_PROMOTE_NODE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		pnr := c.GetPromoteNodeRequest()
		if pnr == nil {
			t.Fatal("expected promote node request, got nil")
		}
		if pnr.Id != "node1" {
			t.Fatalf("unexpected node id, got %s", pnr.Id)
		}

		p, err = pb.Marshal(&proto.CommandPromoteNodeResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.PromoteNodeRequest{
		Id: "node1",
	}
	err := c.PromoteNode(context.Background(), req, srv.Addr(), nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ClientPromoteNodeTimeout(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_PROMOTE_NODE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		pnr := c.GetPromoteNodeRequest()
		if pnr == nil {
			t.Fatal("expected promote node request, got nil")
		}
		if pnr.Id != "node1" {
			t.Fatalf("unexpected node id, got %s", pnr.Id)
		}

		// Don't write anything, so a timeout occurs.
		time.Sleep(5 * time.Second)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.PromoteNodeRequest{
		Id: "node1",
	}
	err := c.PromoteNode(context.Background(), req, srv.Addr(), nil, time.Second)
	if err == nil || !strings.Contains(err.Error(), "i/o timeout") {
		t.Fatalf("failed to receive expected error, got: %T %s", err, err)
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

func Test_ClientJoinNode_MaxRedirectsExceeded(t *testing.T) {
	// Simulate a cluster where every node always redirects to another node,
	// creating an infinite redirect loop. The client should stop after
	// maxRedirects and return an error.
	redirects := make(chan struct{}, maxRedirects)

	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		c := readCommand(conn)
		if c == nil {
			return
		}
		redirects <- struct{}{}
		if c.Type != proto.Command_COMMAND_TYPE_JOIN {
			t.Fatalf("unexpected command type: %d", c.Type)
		}

		// Always respond with "not leader" and a leader address to redirect to.
		p, err := pb.Marshal(&proto.CommandJoinResponse{
			Error:  "not leader",
			Leader: srv.Addr(), // redirect back to same server to simulate loop
		})
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
	if err == nil {
		t.Fatal("expected error when max redirects exceeded")
	}
	if !strings.Contains(err.Error(), "max redirects exceeded") {
		t.Fatalf("expected 'max redirects exceeded' error, got: %s", err)
	}

	close(redirects)
	count := 0
	for range redirects {
		count++
	}
	if count != maxRedirects {
		t.Fatalf("expected %d redirect attempts, got %d", maxRedirects, count)
	}
}

func Test_ClientJoinNode_ContextCanceledDuringRedirect(t *testing.T) {
	// Simulate a redirect loop and cancel the context partway through.
	// The context check happens at the top of each loop iteration, before
	// dialing. Because the local test server responds instantly, we need
	// to cancel the context before calling Join to guarantee the
	// cancellation is observed before the loop exhausts maxRedirects.
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		c := readCommand(conn)
		if c == nil {
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_JOIN {
			t.Fatalf("unexpected command type: %d", c.Type)
		}

		p, err := pb.Marshal(&proto.CommandJoinResponse{
			Error:  "not leader",
			Leader: srv.Addr(),
		})
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

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so the first iteration sees it

	err := c.Join(ctx, req, srv.Addr(), nil, time.Second)
	if err == nil {
		t.Fatal("expected error when context is canceled")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected context canceled error, got: %s", err)
	}
}

func Test_ClientJoinNode_SuccessAfterRedirect(t *testing.T) {
	// First response redirects, second response succeeds.
	attempts := make(chan struct{}, 2)

	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		c := readCommand(conn)
		if c == nil {
			return
		}
		attempts <- struct{}{}
		if c.Type != proto.Command_COMMAND_TYPE_JOIN {
			t.Fatalf("unexpected command type: %d", c.Type)
		}

		var p []byte
		var err error
		if len(attempts) == 1 {
			p, err = pb.Marshal(&proto.CommandJoinResponse{
				Error:  "not leader",
				Leader: srv.Addr(),
			})
		} else {
			p, err = pb.Marshal(&proto.CommandJoinResponse{})
		}
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
		t.Fatalf("expected success after redirect, got: %s", err)
	}

	close(attempts)
	count := 0
	for range attempts {
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 attempts, got %d", count)
	}
}

// countingHandler wraps a handler so a test can see how many commands the
// server read.
type countingHandler struct {
	commands atomic.Int32
}

// handle returns a servicetest handler that reads a command, counts it, and
// then hands the connection and command to f. f is not called if no command
// arrived.
func (h *countingHandler) handle(f func(conn net.Conn, c *proto.Command)) func(net.Conn) {
	return func(conn net.Conn) {
		c := readCommand(conn)
		if c == nil {
			return
		}
		h.commands.Add(1)
		f(conn, c)
	}
}

func mustWriteQueryResponse(conn net.Conn) {
	p, err := pb.Marshal(&proto.CommandQueryResponse{})
	if err != nil {
		conn.Close()
		return
	}
	writeBytesWithLength(conn, p)
}

func Test_ClientRetry_SuccessFirstAttempt(t *testing.T) {
	h := &countingHandler{}
	srv := servicetest.NewService()
	srv.Handler = h.handle(func(conn net.Conn, _ *proto.Command) {
		mustWriteQueryResponse(conn)
	})
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, 0)
	if err != nil {
		t.Fatalf("expected success on first attempt, got: %s", err)
	}
	if exp, got := int32(1), h.commands.Load(); exp != got {
		t.Fatalf("expected command to be sent %d time, got %d", exp, got)
	}
}

func Test_ClientRetry_ZeroRetries_SingleAttempt(t *testing.T) {
	// With zero retries, or a nonsensical negative count, a failing server
	// is tried exactly once.
	for _, retries := range []int{0, -1} {
		t.Run(fmt.Sprint(retries), func(t *testing.T) {
			h := &countingHandler{}
			srv := servicetest.NewService()
			srv.Handler = h.handle(func(conn net.Conn, _ *proto.Command) {
				conn.Close()
			})
			srv.Start()
			defer srv.Close()

			c := NewClient(&simpleDialer{}, 0)
			_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
				srv.Addr(), nil, time.Second, retries)
			if err == nil {
				t.Fatal("expected error with no retries and a failing server")
			}
			if exp, got := int32(1), h.commands.Load(); exp != got {
				t.Fatalf("expected command to be sent %d time, got %d", exp, got)
			}
		})
	}
}

func Test_ClientRetry_FailThenSucceed(t *testing.T) {
	// The first attempt fails, the one retry asked for succeeds.
	h := &countingHandler{}
	srv := servicetest.NewService()
	srv.Handler = h.handle(func(conn net.Conn, _ *proto.Command) {
		if h.commands.Load() == 1 {
			conn.Close()
			return
		}
		mustWriteQueryResponse(conn)
	})
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, 1)
	if err != nil {
		t.Fatalf("expected retry to succeed, got: %s", err)
	}
	if exp, got := int32(2), h.commands.Load(); exp != got {
		t.Fatalf("expected command to be sent %d times, got %d", exp, got)
	}
}

func Test_ClientRetry_AllAttemptsFail(t *testing.T) {
	// Server closes every connection: the attempt and its one retry both fail.
	h := &countingHandler{}
	srv := servicetest.NewService()
	srv.Handler = h.handle(func(conn net.Conn, _ *proto.Command) {
		conn.Close()
	})
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, 1)
	if err == nil {
		t.Fatal("expected error when all attempts fail")
	}
	if exp, got := int32(2), h.commands.Load(); exp != got {
		t.Fatalf("expected command to be sent %d times, got %d", exp, got)
	}
}

func Test_ClientRetry_SuccessOnThirdAttempt(t *testing.T) {
	// With 3 retries, fail the first 2 attempts and succeed on the 3rd.
	h := &countingHandler{}
	srv := servicetest.NewService()
	srv.Handler = h.handle(func(conn net.Conn, _ *proto.Command) {
		if h.commands.Load() <= 2 {
			conn.Close()
			return
		}
		mustWriteQueryResponse(conn)
	})
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, 3)
	if err != nil {
		t.Fatalf("expected success on 3rd attempt, got: %s", err)
	}
	if exp, got := int32(3), h.commands.Load(); exp != got {
		t.Fatalf("expected command to be sent %d times, got %d", exp, got)
	}
}

// Test_ClientWrite_ZeroRetries_NotResentAfterTimeout is the regression test
// for issue 3000: a write forwarded to a node that does not reply in time
// must not be sent again when no retries were asked for, since the node may
// have applied it. Every kind of command that changes the database is covered.
func Test_ClientWrite_ZeroRetries_NotResentAfterTimeout(t *testing.T) {
	const timeout = 100 * time.Millisecond
	for _, tc := range []struct {
		name string
		typ  proto.Command_Type
		send func(c *Client, addr string) error
	}{
		{"execute", proto.Command_COMMAND_TYPE_EXECUTE, func(c *Client, addr string) error {
			_, _, err := c.Execute(context.Background(), executeRequestFromString("INSERT INTO foo(name) VALUES('fiona')"),
				addr, nil, timeout, 0)
			return err
		}},
		{"request", proto.Command_COMMAND_TYPE_REQUEST, func(c *Client, addr string) error {
			_, _, _, err := c.Request(context.Background(), executeQueryRequestFromString("INSERT INTO foo(name) VALUES('fiona')"),
				addr, nil, timeout, 0)
			return err
		}},
		{"load", proto.Command_COMMAND_TYPE_LOAD, func(c *Client, addr string) error {
			return c.Load(context.Background(), &command.LoadRequest{Data: []byte("SQLite format 3")},
				addr, nil, timeout, 0)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })

			h := &countingHandler{}
			srv := servicetest.NewService()
			srv.Handler = h.handle(func(conn net.Conn, c *proto.Command) {
				if c.Type != tc.typ {
					t.Errorf("unexpected command type: %d", c.Type)
				}
				<-release // The slow node: never replies in time.
			})
			srv.Start()
			defer srv.Close()

			err := tc.send(NewClient(&simpleDialer{}, 0), srv.Addr())
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("expected a timeout error, got: %v", err)
			}
			if exp, got := int32(1), h.commands.Load(); exp != got {
				t.Fatalf("expected write to be sent %d time, got %d", exp, got)
			}
		})
	}
}

// Test_ClientExecute_Retry_ResentAfterTimeout shows what asking for a retry
// means: a write whose reply did not arrive in time is sent again.
func Test_ClientExecute_Retry_ResentAfterTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	h := &countingHandler{}
	srv := servicetest.NewService()
	srv.Handler = h.handle(func(conn net.Conn, _ *proto.Command) {
		if h.commands.Load() == 1 {
			<-release // The first attempt never gets its reply.
			return
		}
		p, err := pb.Marshal(&proto.CommandExecuteResponse{})
		if err != nil {
			conn.Close()
			return
		}
		writeBytesWithLength(conn, p)
	})
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, _, err := c.Execute(context.Background(), executeRequestFromString("INSERT INTO foo(name) VALUES('fiona')"),
		srv.Addr(), nil, 100*time.Millisecond, 1)
	if err != nil {
		t.Fatalf("expected retry to succeed, got: %s", err)
	}
	if exp, got := int32(2), h.commands.Load(); exp != got {
		t.Fatalf("expected write to be sent %d times, got %d", exp, got)
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

// Test_ClientRejectedCommandDropsConnection checks that a connection on which
// the service sets "close connection", or a failing backup, results in the
// connection being closed
func Test_ClientRejectedCommandDropsConnection(t *testing.T) {
	query := func(c *Client, addr string) error {
		_, _, err := c.Query(context.Background(), queryRequestFromString("SELECT * FROM foo"),
			addr, nil, time.Second, 0)
		return err
	}
	backup := func(c *Client, addr string) error {
		_, err := c.Backup(context.Background(), &command.BackupRequest{}, addr, nil, time.Second, io.Discard)
		return err
	}
	for _, tc := range []struct {
		name     string
		send     func(c *Client, addr string) error
		errStr   string
		setClose bool
		wantIdle int
	}{
		{"generic set close", query, unsupportedError, true, 0},
		{"other_error", query, "no such table", false, 1},
		{"backup_error", backup, "BackupRequest is nil", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := servicetest.NewService()
			srv.Handler = func(conn net.Conn) {
				c := readCommand(conn)
				if c == nil {
					return
				}
				p, err := errorResponse(c.Type, tc.errStr, tc.setClose)
				if err != nil {
					t.Errorf("failed to build error response: %s", err)
					conn.Close()
					return
				}
				writeBytesWithLength(conn, p)
				if tc.wantIdle == 0 {
					conn.Close() // As the real service does after rejecting a command.
				}
			}
			srv.Start()
			defer srv.Close()

			c := NewClient(&simpleDialer{}, 0)
			err := tc.send(c, srv.Addr())
			if err == nil || err.Error() != tc.errStr {
				t.Fatalf("expected error %q, got %v", tc.errStr, err)
			}

			c.poolMu.RLock()
			pl := c.pools[srv.Addr()]
			c.poolMu.RUnlock()
			if got := pl.Len(); got != tc.wantIdle {
				t.Fatalf("expected %d idle connection(s) in pool, got %d", tc.wantIdle, got)
			}
		})
	}
}

// Test_ClientRetry_FailureBeforeReply checks that a failure to dial or to
// write keeps to the retry budget just as a failure to read does, and that a
// cancelled context ends the attempts at once, with no retry counted that
// never happened.
func Test_ClientRetry_FailureBeforeReply(t *testing.T) {
	for _, tc := range []struct {
		name         string
		retries      int
		cancel       bool // Cancel the context during the first dial.
		writeFails   bool // Dial succeeds but every write fails.
		wantAttempts int
		wantRetries  int
	}{
		{"dial_fails_no_retries", 0, false, false, 1, 0},
		{"dial_fails_two_retries", 2, false, false, 3, 2},
		{"write_fails_no_retries", 0, false, true, 1, 0},
		{"write_fails_two_retries", 2, false, true, 3, 2},
		{"cancelled", 2, true, false, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			attempts := 0
			dialer := funcDialer(func(string, time.Duration) (net.Conn, error) {
				attempts++
				if tc.cancel {
					cancel()
				}
				if tc.writeFails {
					a, b := net.Pipe()
					t.Cleanup(func() { b.Close() })
					return writeFailConn{a}, nil
				}
				return nil, errors.New("dial failed")
			})

			c := NewClient(dialer, 0)
			cmd := &proto.Command{Type: proto.Command_COMMAND_TYPE_QUERY}
			nRetries, err := c.retry(ctx, cmd, "unused", time.Second, tc.retries, &proto.CommandQueryResponse{})
			if err == nil {
				t.Fatal("expected an error")
			}
			if attempts != tc.wantAttempts {
				t.Fatalf("expected %d attempt(s), got %d", tc.wantAttempts, attempts)
			}
			if nRetries != tc.wantRetries {
				t.Fatalf("expected %d retries reported, got %d", tc.wantRetries, nRetries)
			}
		})
	}
}

// funcDialer is a Dialer backed by a function.
type funcDialer func(address string, timeout time.Duration) (net.Conn, error)

func (d funcDialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return d(address, timeout)
}

// writeFailConn is a connection on which every write fails.
type writeFailConn struct{ net.Conn }

func (writeFailConn) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

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
	responses, err := c.BroadcastHWM(context.Background(), 12345, nil, 0, time.Second, srv.Addr())
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
	responses, err := c.BroadcastHWM(context.Background(), 999, nil, 0, time.Second, srv1.Addr(), srv2.Addr())
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
	responses, err := c.BroadcastHWM(context.Background(), 1, nil, 0, time.Second)
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
	responses, err := c.BroadcastHWM(context.Background(), 12345, nil, 0, time.Second, srv.Addr())
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

func Test_ClientBroadcast_WithCreds(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			return
		}
		if c.Credentials == nil {
			t.Fatal("got nil credentials")
		}
		if c.Credentials.Username != "bob" || c.Credentials.Password != "passwd" {
			t.Fatal("got wrong credentials")
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
	creds := &proto.Credentials{Username: "bob", Password: "passwd"}
	responses, err := c.BroadcastHWM(context.Background(), 12345, creds, 0, time.Second, srv.Addr())
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
