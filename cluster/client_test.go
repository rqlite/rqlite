package cluster

import (
	"encoding/binary"
	"fmt"
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
	fmt.Println(">>>>Test_ClientGetNodeAPIAddr called")
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			t.Fatal("expected command, got nil")
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
	c.SetInitialPoolSize(1)
	addr, err := c.GetNodeAPIAddr(srv.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	exp, got := "http://localhost:1234", addr
	if exp != got {
		t.Fatalf("unexpected addr, got %s, exp: %s", got, exp)
	}
	fmt.Println(">>>>Test_ClientGetNodeAPIAddr finished")
}

func readCommand(conn net.Conn) *Command {
	fmt.Println(">>>>readCommmand called")
	b := make([]byte, protoBufferLengthSize)
	_, err := io.ReadFull(conn, b)
	if err != nil {
		fmt.Println(">>>>>> readCommand1: ", err)
		return nil
	}
	sz := binary.LittleEndian.Uint64(b[0:])
	p := make([]byte, sz)
	_, err = io.ReadFull(conn, p)
	if err != nil {
		fmt.Println(">>>>>> readCommand2: ", err)
		return nil
	}
	c := &Command{}
	err = proto.Unmarshal(p, c)
	if err != nil {
		fmt.Println(">>>>>> readCommand3: ", err)
		return nil
	}
	return c
}

type simpleDialer struct {
}

func (s *simpleDialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println(">>>>>> simpleDialer.Dial: ", err)
		return nil, err
	}
	fmt.Println(">>>>>> simpleDialer.Dial OK, conn local address ", conn.LocalAddr())
	return conn, nil
}
