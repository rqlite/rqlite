package cluster

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/rtls"
	"github.com/rqlite/rqlite/v8/tcp"
	"github.com/rqlite/rqlite/v8/testdata/x509"
)

func Test_NewServiceSetGetNodeAPIAddrMuxed(t *testing.T) {
	ln, mux := mustNewMux()
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.

	s := New(tn, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	c := NewClient(mustNewDialer(1, false, false), 30*time.Second)

	meta, err := c.GetNodeMeta(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "http://foo" {
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
	ln, mux := mustNewTLSMux()
	go mux.Serve()
	tn := mux.Listen(1) // Could be any byte value.

	s := New(tn, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	c := NewClient(mustNewDialer(1, true, true), 30*time.Second)

	meta, err := c.GetNodeMeta(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "http://foo" {
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

func mustNewTLSMux() (net.Listener, *tcp.Mux) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create mock listener")
	}

	cert := x509.CertExampleDotComFile("")
	defer os.Remove(cert)
	key := x509.KeyExampleDotComFile("")
	defer os.Remove(key)

	mux, err := tcp.NewTLSMux(ln, nil, cert, key, "", true, false)
	if err != nil {
		panic(fmt.Sprintf("failed to create TLS mux: %s", err))
	}

	return ln, mux
}

func mustNewDialer(header byte, remoteEncrypted, skipVerify bool) *tcp.Dialer {
	var tlsConfig *tls.Config
	var err error
	if remoteEncrypted {
		tlsConfig, err = rtls.CreateClientConfig("", "", rtls.NoCACert, rtls.NoServerName, skipVerify)
		if err != nil {
			panic(fmt.Sprintf("failed to create client TLS config: %s", err))
		}
	}
	return tcp.NewDialer(header, tlsConfig)
}
