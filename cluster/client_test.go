package cluster

import (
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/cluster/proto"
	"github.com/rqlite/rqlite/v8/cluster/servicetest"
	command "github.com/rqlite/rqlite/v8/command/proto"
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
	meta, err := c.GetNodeMeta(srv.Addr(), noRetries, time.Second)
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
	idx, err := c.GetCommitIndex(srv.Addr(), noRetries, time.Second)
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

		p, err = pb.Marshal(&proto.CommandExecuteResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, err := c.Execute(executeRequestFromString("INSERT INTO foo (id) VALUES (1)"),
		srv.Addr(), nil, time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal(err)
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
	_, err := c.Query(queryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second)
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

		p, err = pb.Marshal(&proto.CommandRequestResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, err := c.Request(executeQueryRequestFromString("SELECT * FROM foo"),
		srv.Addr(), nil, time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal(err)
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
	err := c.RemoveNode(req, srv.Addr(), nil, time.Second)
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
	err := c.RemoveNode(req, srv.Addr(), nil, time.Second)
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
	err := c.Join(req, srv.Addr(), nil, time.Second)
	if err != nil {
		t.Fatal(err)
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
