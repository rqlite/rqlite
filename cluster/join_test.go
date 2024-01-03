package cluster

import (
	"net"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/cluster/proto"
	"github.com/rqlite/rqlite/v8/cluster/servicetest"
	pb "google.golang.org/protobuf/proto"
)

const numAttempts int = 3
const attemptInterval = 1 * time.Second

func Test_SingleJoinOK(t *testing.T) {
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
		if c.Type != proto.Command_COMMAND_TYPE_JOIN {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		jr := c.GetJoinRequest()
		if jr == nil {
			t.Fatal("join request is nil")
		}
		if exp, got := "id0", jr.Id; exp != got {
			t.Fatalf("unexpected id, got %s, exp: %s", got, exp)
		}
		if exp, got := "1.2.3.4", jr.Address; exp != got {
			t.Fatalf("unexpected addr, got %s, exp: %s", got, exp)
		}

		resp := &proto.CommandJoinResponse{}
		p, err = pb.Marshal(resp)
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	joiner := NewJoiner(c, numAttempts, attemptInterval)
	addr, err := joiner.Do([]string{srv.Addr()}, "id0", "1.2.3.4", Voter)
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := srv.Addr(), addr; exp != got {
		t.Fatalf("unexpected addr, got %s, exp: %s", got, exp)
	}
}

func Test_SingleJoinZeroAttempts(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		t.Fatalf("handler should not have been called")
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	joiner := NewJoiner(c, 0, attemptInterval)
	_, err := joiner.Do([]string{srv.Addr()}, "id0", "1.2.3.4", Voter)
	if err != ErrJoinFailed {
		t.Fatalf("Incorrect error returned when zero attempts specified")
	}
}

func Test_SingleJoinFail(t *testing.T) {
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
		resp := &proto.CommandJoinResponse{
			Error: "bad request",
		}
		p, err = pb.Marshal(resp)
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	c := NewClient(&simpleDialer{}, 0)
	joiner := NewJoiner(c, numAttempts, attemptInterval)
	_, err := joiner.Do([]string{srv.Addr()}, "id0", "1.2.3.4", Voter)
	if err == nil {
		t.Fatalf("expected error when joining bad node")
	}
}

func Test_DoubleJoinOKSecondNode(t *testing.T) {
	srv1 := servicetest.NewService()
	srv1.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		resp := &proto.CommandJoinResponse{
			Error: "bad request",
		}
		p, err = pb.Marshal(resp)
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv1.Start()
	defer srv1.Close()

	srv2 := servicetest.NewService()
	srv2.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Error on connection, so give up, as normal
			// test exit can cause that too.
			return
		}
		if c.Type != proto.Command_COMMAND_TYPE_JOIN {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		resp := &proto.CommandJoinResponse{}
		p, err = pb.Marshal(resp)
		if err != nil {
			conn.Close()
		}
		writeBytesWithLength(conn, p)
	}
	srv2.Start()
	defer srv2.Close()

	c := NewClient(&simpleDialer{}, 0)
	joiner := NewJoiner(c, numAttempts, attemptInterval)
	addr, err := joiner.Do([]string{srv1.Addr(), srv2.Addr()}, "id0", "1.2.3.4", Voter)
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := srv2.Addr(), addr; exp != got {
		t.Fatalf("unexpected addr, got %s, exp: %s", got, exp)
	}
}
