package cluster

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/rqlite/rqlite/cluster/servicetest"
	"github.com/rqlite/rqlite/command"
	"google.golang.org/protobuf/proto"
)

func Test_NewClient(t *testing.T) {
	c := NewClient(nil, 0)
	if c == nil {
		t.Fatal("expected client, got nil")
	}
}

func Test_ClientGetNodeAPIAddr(t *testing.T) {
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
		if c.Type != Command_COMMAND_TYPE_GET_NODE_API_URL {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		p, err = proto.Marshal(&Address{
			Url: "http://localhost:1234",
		})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	addr, err := c.GetNodeAPIAddr(srv.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	exp, got := "http://localhost:1234", addr
	if exp != got {
		t.Fatalf("unexpected addr, got %s, exp: %s", got, exp)
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
		if c.Type != Command_COMMAND_TYPE_EXECUTE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		er := c.GetExecuteRequest()
		if er == nil {
			t.Fatal("expected execute request, got nil")
		}
		if er.Request.Statements[0].Sql != "INSERT INTO foo (id) VALUES (1)" {
			t.Fatalf("unexpected statement, got %s", er.Request.Statements[0])
		}

		p, err = proto.Marshal(&CommandExecuteResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	_, err := c.Execute(executeRequestFromString("INSERT INTO foo (id) VALUES (1)"),
		srv.Addr(), nil, time.Second)
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
		if c.Type != Command_COMMAND_TYPE_QUERY {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		qr := c.GetQueryRequest()
		if qr == nil {
			t.Fatal("expected query request, got nil")
		}
		if qr.Request.Statements[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("unexpected statement, got %s", qr.Request.Statements[0])
		}

		p, err = proto.Marshal(&CommandQueryResponse{})
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
		if c.Type != Command_COMMAND_TYPE_REMOVE_NODE {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		rnr := c.GetRemoveNodeRequest()
		if rnr == nil {
			t.Fatal("expected remove node request, got nil")
		}
		if rnr.Id != "node1" {
			t.Fatalf("unexpected node id, got %s", rnr.Id)
		}

		p, err = proto.Marshal(&CommandRemoveNodeResponse{})
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

func Test_ClientNotify(t *testing.T) {
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
		if c.Type != Command_COMMAND_TYPE_NOTIFY {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		nr := c.GetNotifyRequest()
		if nr == nil {
			t.Fatal("expected notify node request, got nil")
		}
		if nr.Id != "node1" {
			t.Fatalf("unexpected node id, got %s", nr.Id)
		}
		if nr.Address != "localhost:1234" {
			t.Fatalf("unexpected node address, got %s", nr.Address)
		}

		p, err = proto.Marshal(&CommandNotifyResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.NotifyRequest{
		Id:      "node1",
		Address: "localhost:1234",
	}
	err := c.Notify(req, srv.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ClientJoin(t *testing.T) {
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
		if c.Type != Command_COMMAND_TYPE_JOIN {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		nr := c.GetJoinRequest()
		if nr == nil {
			t.Fatal("expected notify node request, got nil")
		}
		if nr.Id != "node1" {
			t.Fatalf("unexpected node id, got %s", nr.Id)
		}
		if nr.Address != "localhost:1234" {
			t.Fatalf("unexpected address, got %s", nr.Address)
		}
		if !nr.Voter {
			t.Fatalf("unexpected voter, got %v", nr.Voter)
		}

		p, err = proto.Marshal(&CommandJoinResponse{})
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	req := &command.JoinRequest{
		Id:      "node1",
		Address: "localhost:1234",
		Voter:   true,
	}
	err := c.Join(req, srv.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func readCommand(conn net.Conn) *Command {
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
	c := &Command{}
	err = proto.Unmarshal(p, c)
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
