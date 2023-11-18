package cluster

import (
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/rqlite/rqlite/cluster/servicetest"
	"github.com/rqlite/rqlite/command"
	"google.golang.org/protobuf/proto"
)

func Test_AddressProviderString(t *testing.T) {
	a := []string{"a", "b", "c"}
	p := NewAddressProviderString(a)
	b, err := p.Lookup()
	if err != nil {
		t.Fatalf("failed to lookup addresses: %s", err.Error())
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("failed to get correct addresses")
	}
}

func Test_NewBootstrapper(t *testing.T) {
	bs := NewBootstrapper(nil, nil)
	if bs == nil {
		t.Fatalf("failed to create a simple Bootstrapper")
	}
	if exp, got := BootUnknown, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootDoneImmediately(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		t.Fatalf("client made request")
	}
	srv.Start()
	defer srv.Close()

	done := func() bool {
		return true
	}
	p := NewAddressProviderString([]string{srv.Addr()})
	bs := NewBootstrapper(p, nil)
	if err := bs.Boot("node1", "192.168.1.1:1234", done, 10*time.Second); err != nil {
		t.Fatalf("failed to boot: %s", err)
	}
	if exp, got := BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootTimeout(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
	}
	srv.Start()
	defer srv.Close()

	done := func() bool {
		return false
	}
	p := NewAddressProviderString([]string{srv.Addr()})
	bs := NewBootstrapper(p, NewClient(&simpleDialer{}, 0))
	bs.Interval = time.Second
	err := bs.Boot("node1", "192.168.1.1:1234", done, 5*time.Second)
	if err == nil {
		t.Fatalf("no error returned from timed-out boot")
	}
	if !errors.Is(err, ErrBootTimeout) {
		t.Fatalf("wrong error returned")
	}
	if exp, got := BootTimeout, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootSingleJoin(t *testing.T) {
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Connection error handling
			return
		}
		if c.Type != Command_COMMAND_TYPE_JOIN {
			t.Fatalf("unexpected command type: %d", c.Type)
		}
		jnr := c.GetJoinRequest()
		if jnr == nil {
			t.Fatal("expected join node request, got nil")
		}
		if jnr.Address != "192.168.1.1:1234" {
			t.Fatalf("unexpected node address, got %s", jnr.Address)
		}

		p, err = proto.Marshal(&CommandJoinResponse{})
		if err != nil {
			conn.Close()
			return
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	done := func() bool {
		return false
	}

	p := NewAddressProviderString([]string{srv.Addr()})
	bs := NewBootstrapper(p, NewClient(&simpleDialer{}, 0))
	bs.Interval = time.Second

	err := bs.Boot("node1", "192.168.1.1:1234", done, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}
	if exp, got := BootJoin, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootSingleNotify(t *testing.T) {
	var gotNR *command.NotifyRequest
	srv := servicetest.NewService()
	srv.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Connection error handling
			return
		}

		if c.Type != Command_COMMAND_TYPE_NOTIFY {
			return
		}
		gotNR = c.GetNotifyRequest()

		p, err = proto.Marshal(&CommandNotifyResponse{})
		if err != nil {
			conn.Close()
			return
		}
		writeBytesWithLength(conn, p)
	}
	srv.Start()
	defer srv.Close()

	n := -1
	done := func() bool {
		n++
		return n == 5
	}

	p := NewAddressProviderString([]string{srv.Addr()})
	bs := NewBootstrapper(p, NewClient(&simpleDialer{}, 0))
	bs.Interval = time.Second

	err := bs.Boot("node1", "192.168.1.1:1234", done, 60*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}

	if got, exp := gotNR.Id, "node1"; got != exp {
		t.Fatalf("wrong node ID supplied, exp %s, got %s", exp, got)
	}
	if got, exp := gotNR.Address, "192.168.1.1:1234"; got != exp {
		t.Fatalf("wrong address supplied, exp %s, got %s", exp, got)
	}
	if exp, got := BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootMultiJoinNotify(t *testing.T) {
	srv1Join := false
	srv1Notified := false
	srv1 := servicetest.NewService()
	srv1.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Connection error handling
			return
		}

		if c.Type == Command_COMMAND_TYPE_JOIN {
			srv1Join = true
		}

		if c.Type != Command_COMMAND_TYPE_NOTIFY {
			return
		}
		srv1Notified = true

		p, err = proto.Marshal(&CommandNotifyResponse{})
		if err != nil {
			conn.Close()
			return
		}
		writeBytesWithLength(conn, p)
	}
	srv1.Start()
	defer srv1.Close()

	srv2Join := false
	srv2Notified := false
	srv2 := servicetest.NewService()
	srv2.Handler = func(conn net.Conn) {
		var p []byte
		var err error
		c := readCommand(conn)
		if c == nil {
			// Connection error handling
			return
		}

		if c.Type == Command_COMMAND_TYPE_JOIN {
			srv2Join = true
		}

		if c.Type != Command_COMMAND_TYPE_NOTIFY {
			return
		}
		srv2Notified = true

		p, err = proto.Marshal(&CommandNotifyResponse{})
		if err != nil {
			conn.Close()
			return
		}
		writeBytesWithLength(conn, p)
	}
	srv2.Start()
	defer srv2.Close()

	n := -1
	done := func() bool {
		n++
		return n == 5
	}

	p := NewAddressProviderString([]string{srv1.Addr(), srv2.Addr()})
	bs := NewBootstrapper(p, NewClient(&simpleDialer{}, 0))
	bs.Interval = time.Second

	err := bs.Boot("node1", "192.168.1.1:1234", done, 60*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}

	if srv1Join != true || srv2Join != true {
		t.Fatalf("all join targets not contacted")
	}
	if srv1Notified != true || srv2Notified != true {
		t.Fatalf("all notify targets not contacted")
	}
	if exp, got := BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}
