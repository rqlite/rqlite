package cluster

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/testdata/x509"
)

func Test_NewServiceOpenClose(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
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
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
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
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
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
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
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
	c := NewClient(ml, 30*time.Second)
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
}

func Test_NewServiceSetGetNodeAPIAddrTLS(t *testing.T) {
	ml := mustNewMockTLSTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
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

func Test_NewServiceTestExecuteQueryAuthNoCredentials(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	// Test that for a cluster with no credential store configed
	// all users are authed for both operations
	var c CredentialStore = nil
	c = nil
	s := New(ml, db, clstr, c)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &command.ExecuteRequest{}
	_, err := cl.Execute(er, s.Addr(), nil, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	qr := &command.QueryRequest{}
	_, err = cl.Query(qr, s.Addr(), nil, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

// Test_NewServiceTestExecuteQueryAuth tests that for a cluster with a credential
// store, configured users with execute permissions can execute and users with
// query permissions can query, and can't if they don't have those permissions.
func Test_NewServiceTestExecuteQueryAuth(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	f := func(username string, password string, perm string) bool {
		if username == "alice" && password == "secret1" && perm == "execute" {
			return true
		} else if username == "bob" && password == "secret1" && perm == "query" {
			return true
		}
		return false
	}
	c := &mockCredentialStore{aaFunc: f}

	s := New(ml, db, clstr, c)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &command.ExecuteRequest{}
	_, err := cl.Execute(er, s.Addr(), makeCredentials("alice", "secret1"), 5*time.Second)
	if err != nil {
		t.Fatal("alice improperly unauthorized to execute")
	}
	_, err = cl.Execute(er, s.Addr(), makeCredentials("bob", "secret1"), 5*time.Second)
	if err == nil {
		t.Fatal("bob improperly authorized to execute")
	}
	qr := &command.QueryRequest{}
	_, err = cl.Query(qr, s.Addr(), makeCredentials("bob", "secret1"), 5*time.Second)
	if err != nil && err.Error() != "unauthorized" {
		fmt.Println(err)
		t.Fatal("bob improperly unauthorized to query")
	}
	_, err = cl.Query(qr, s.Addr(), makeCredentials("alice", "secret1"), 5*time.Second)
	if err != nil && err.Error() != "unauthorized" {
		t.Fatal("alice improperly authorized to query")
	}
}

func Test_NewServiceNotify(t *testing.T) {
	ml := mustNewMockTransport()
	mm := mustNewMockManager()
	mm.notifyFn = func(n *command.NotifyRequest) error {
		if n.Id != "foo" {
			t.Fatalf("failed to get correct node ID, exp %s, got %s", "foo", n.Id)
		}
		if n.Address != "localhost" {
			t.Fatalf("failed to get correct node address, exp %s, got %s", "localhost", n.Address)
		}
		return nil
	}

	s := New(ml, mustNewMockDatabase(), mm, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	// Create a notify request.
	nr := &command.NotifyRequest{
		Id:      "foo",
		Address: "localhost",
	}

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	err := c.Notify(nr, s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to notify node: %s", err)
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
	backupFn  func(br *command.BackupRequest, dst io.Writer) error
	loadFn    func(lr *command.LoadRequest) error
}

func (m *mockDatabase) Execute(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
	return m.executeFn(er)
}

func (m *mockDatabase) Query(qr *command.QueryRequest) ([]*command.QueryRows, error) {
	return m.queryFn(qr)
}

func (m *mockDatabase) Backup(br *command.BackupRequest, dst io.Writer) error {
	if m.backupFn == nil {
		return nil
	}
	return m.backupFn(br, dst)
}

func (m *mockDatabase) Load(lr *command.LoadRequest) error {
	if m.loadFn == nil {
		return nil
	}
	return m.loadFn(lr)
}

func mustNewMockDatabase() *mockDatabase {
	e := func(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
		return []*command.ExecuteResult{}, nil
	}
	q := func(er *command.QueryRequest) ([]*command.QueryRows, error) {
		return []*command.QueryRows{}, nil
	}
	return &mockDatabase{executeFn: e, queryFn: q}
}

type MockManager struct {
	removeNodeFn func(rn *command.RemoveNodeRequest) error
	notifyFn     func(n *command.NotifyRequest) error
}

func (m *MockManager) Remove(rn *command.RemoveNodeRequest) error {
	if m.removeNodeFn == nil {
		return nil
	}
	return m.removeNodeFn(rn)
}

func (m *MockManager) Notify(n *command.NotifyRequest) error {
	if m.notifyFn == nil {
		return nil
	}
	return m.notifyFn(n)
}

func mustNewMockManager() *MockManager {
	return &MockManager{}
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

type mockCredentialStore struct {
	HasPermOK bool
	aaFunc    func(username, password, perm string) bool
}

func (m *mockCredentialStore) AA(username, password, perm string) bool {
	if m == nil {
		return true
	}

	if m.aaFunc != nil {
		return m.aaFunc(username, password, perm)
	}
	return m.HasPermOK
}

func mustNewMockCredentialStore() *mockCredentialStore {
	return &mockCredentialStore{HasPermOK: true}
}

func makeCredentials(username, password string) *Credentials {
	return &Credentials{
		Username: username,
		Password: password,
	}
}
