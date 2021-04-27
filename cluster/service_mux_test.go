package cluster

import (
	"fmt"
	"net"
	"testing"

	"github.com/rqlite/rqlite/tcp"
)

func Test_NewServiceSetGetNodeAPIAddrMuxed(t *testing.T) {
	ln, mux := mustNewMux()
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.

	s := NewService(tn)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	addr, err := s.GetNodeAPIAddr(s.Addr())
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if addr != "foo" {
		t.Fatalf("failed to get correct node API address")
	}

	if err := ln.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func mustNewMux() (net.Listener, *tcp.Mux) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create mock listener")
	}

	mux, err := tcp.NewMux(ln, nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create mux: %s", err))
	}

	return ln, mux
}
