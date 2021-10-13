package cluster

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/tcp"
	"github.com/rqlite/rqlite/testdata/x509"
)

func Test_NewServiceSetGetNodeAPIAddrMuxed(t *testing.T) {
	ln, mux := mustNewMux(context.TODO())
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.

	s := New(context.TODO(), tn, mustNewMockDatabase())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	c := NewClient(mustNewDialer(1, false, false))

	addr, err := c.GetNodeAPIAddr(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if addr != "http://foo" {
		t.Fatalf("failed to get correct node API address")
	}

	if err := ln.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetNodeAPIAddrMuxedTLS(t *testing.T) {
	ln, mux := mustNewTLSMux(context.TODO())
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.

	s := New(context.TODO(), tn, mustNewMockDatabase())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	c := NewClient(mustNewDialer(1, true, true))

	addr, err := c.GetNodeAPIAddr(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if addr != "http://foo" {
		t.Fatalf("failed to get correct node API address")
	}

	if err := ln.Close(); err != nil {
		t.Fatalf("failed to close Mux's listener: %s", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func mustNewMux(ctx context.Context) (net.Listener, *tcp.Mux) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create mock listener")
	}

	mux, err := tcp.NewMux(ctx, ln, nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create mux: %s", err))
	}

	return ln, mux
}

func mustNewTLSMux(ctx context.Context) (net.Listener, *tcp.Mux) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create mock listener")
	}

	cert := x509.CertFile("")
	defer os.Remove(cert)
	key := x509.KeyFile("")
	defer os.Remove(key)

	mux, err := tcp.NewTLSMux(ctx, ln, nil, cert, key, "")
	if err != nil {
		panic(fmt.Sprintf("failed to create TLS mux: %s", err))
	}
	mux.InsecureSkipVerify = true

	return ln, mux
}

func mustNewDialer(header byte, remoteEncrypted, skipVerify bool) *tcp.Dialer {
	return tcp.NewDialer(header, remoteEncrypted, skipVerify)
}
