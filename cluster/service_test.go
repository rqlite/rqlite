package cluster

import (
	"crypto/tls"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/testdata/x509"
)

func Test_NewServiceOpenClose(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase())
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
	s := New(ml, mustNewMockDatabase())
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
	s := New(ml, mustNewMockDatabase())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Test by connecting to itself.
	c := NewClient(ml)
	addr, err := c.GetNodeAPIAddr(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if addr != "http://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "http://foo", addr)
	}

	s.EnableHTTPS(true)

	// Test fetch via network.
	addr, err = c.GetNodeAPIAddr(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if addr != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", addr)
	}

	// Test fetch via local call.
	addr = s.GetNodeAPIURL()
	if addr != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", addr)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetNodeAPIAddrLocal(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Check stats to confirm no local request yet.
	if stats.Get(numGetNodeAPIRequestLocal).String() != "0" {
		t.Fatalf("failed to confirm request served locally")
	}

	// Test by enabling local answering
	c := NewClient(ml)
	if err := c.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	addr, err := c.GetNodeAPIAddr(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address locally: %s", err)
	}
	if addr != "http://foo" {
		t.Fatalf("failed to get correct node API address locally, exp %s, got %s", "http://foo", addr)
	}

	// Check stats to confirm local response.
	if stats.Get(numGetNodeAPIRequestLocal).String() != "1" {
		t.Fatalf("failed to confirm request served locally")
	}

	// Ensure it still works when hostname passed in.
	_, port, err := net.SplitHostPort(s.Addr())
	if err != nil {
		t.Fatalf("failed to split host and port: %s", err)
	}
	if err := c.SetLocal(net.JoinHostPort("localhost", port), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	addr, err = c.GetNodeAPIAddr(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address locally: %s", err)
	}
	if addr != "http://foo" {
		t.Fatalf("failed to get correct node API address locally, exp %s, got %s", "http://foo", addr)
	}

	// Check stats to confirm local response.
	if stats.Get(numGetNodeAPIRequestLocal).String() != "2" {
		t.Fatalf("failed to confirm request served locally")
	}

}

func Test_NewServiceSetGetNodeAPIAddrTLS(t *testing.T) {
	ml := mustNewMockTLSTransport()
	s := New(ml, mustNewMockDatabase())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Test by connecting to itself.
	c := NewClient(ml)
	addr, err := c.GetNodeAPIAddr(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	exp := "http://foo"
	if addr != exp {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", exp, addr)
	}

	s.EnableHTTPS(true)
	addr, err = c.GetNodeAPIAddr(s.Addr(), 5*time.Second)
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

type mockDatabase struct {
	executeFn func(er *command.ExecuteRequest) ([]*command.ExecuteResult, error)
	queryFn   func(qr *command.QueryRequest) ([]*command.QueryRows, error)
}

func (m *mockDatabase) Execute(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
	return m.executeFn(er)
}

func (m *mockDatabase) Query(qr *command.QueryRequest) ([]*command.QueryRows, error) {
	return m.queryFn(qr)
}

func mustNewMockDatabase() *mockDatabase {
	return &mockDatabase{}
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
