package cluster

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/rqlite/rqlite/cluster/servicetest"
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
		c := readCommand(t, conn)
		if c.Type != Command_COMMAND_TYPE_GET_NODE_API_URL {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		p, err = proto.Marshal(&Address{
			Url: "http://localhost:8080",
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
	exp, got := "http://localhost:8080", addr
	if exp != got {
		t.Fatalf("unexpected addr, got %s, exp: %s", got, exp)
	}
}

func readCommand(t *testing.T, conn net.Conn) *Command {
	b := make([]byte, protoBufferLengthSize)
	_, err := io.ReadFull(conn, b)
	if err != nil {
		t.Fatalf("failed to command size: %s", err)
	}
	sz := binary.LittleEndian.Uint64(b[0:])
	p := make([]byte, sz)
	_, err = io.ReadFull(conn, p)
	if err != nil {
		t.Fatalf("failed to read command bytes: %s", err)
	}
	c := &Command{}
	err = proto.Unmarshal(p, c)
	if err != nil {
		t.Fatalf("failed to unmarshal command: %s", err)
	}
	return c
}

type simpleDialer struct {
}

func (s *simpleDialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return net.Dial("tcp", address)
}
