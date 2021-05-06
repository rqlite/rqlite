package cluster

import (
	"crypto/tls"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v6/testdata/x509"
)

func Test_NewServiceOpenClose(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	if ml.Addr().String() != s.Addr() {
		t.Fatalf("service returned incorrect address")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetAPIAddr(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")
	if exp, got := "foo", s.GetAPIAddr(); exp != got {
		t.Fatalf("got incorrect API address, exp %s, got %s", exp, got)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetNodeAPIAddr(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Test by connecting to itself.
	addr, err := s.GetNodeAPIAddr(s.Addr())
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if addr != "http://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "http://foo", addr)
	}

	s.EnableHTTPS(true)
	addr, err = s.GetNodeAPIAddr(s.Addr())
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if addr != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", addr)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetNodeAPIAddrTLS(t *testing.T) {
	ml := mustNewMockTLSTransport()
	s := New(ml)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Test by connecting to itself.
	addr, err := s.GetNodeAPIAddr(s.Addr())
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	exp := "http://foo"
	if addr != exp {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", exp, addr)
	}

	s.EnableHTTPS(true)
	addr, err = s.GetNodeAPIAddr(s.Addr())
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if addr != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", addr)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

type mockTransport struct {
	tn              net.Listener
	remoteEncrypted bool
}

func mustNewMockTransport() *mockTransport {
	tn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create mock listener")
	}
	return &mockTransport{
		tn: tn,
	}
}

func mustNewMockTLSTransport() *mockTransport {
	tn := mustNewMockTransport()
	return &mockTransport{
		tn:              tls.NewListener(tn, mustCreateTLSConfig()),
		remoteEncrypted: true,
	}
}

func (ml *mockTransport) Accept() (c net.Conn, err error) {
	return ml.tn.Accept()
}

func (ml *mockTransport) Addr() net.Addr {
	return ml.tn.Addr()
}

func (ml *mockTransport) Close() (err error) {
	return ml.tn.Close()
}

func (ml *mockTransport) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	var err error
	var conn net.Conn
	if ml.remoteEncrypted {
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, conf)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}

	return conn, err
}

func mustCreateTLSConfig() *tls.Config {
	var err error

	certFile := x509.CertFile("")
	defer os.Remove(certFile)
	keyFile := x509.KeyFile("")
	defer os.Remove(keyFile)

	config := &tls.Config{
		InsecureSkipVerify: true,
	}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		panic("failed to create TLS config")
	}

	return config
}
