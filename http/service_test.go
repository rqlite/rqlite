package http

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/rqlite/rqlite/command"
	sql "github.com/rqlite/rqlite/db"
	"github.com/rqlite/rqlite/store"
	"github.com/rqlite/rqlite/testdata/x509"
)

func Test_NormalizeAddr(t *testing.T) {
	tests := []struct {
		orig string
		norm string
	}{
		{
			orig: "http://localhost:4001",
			norm: "http://localhost:4001",
		},
		{
			orig: "https://localhost:4001",
			norm: "https://localhost:4001",
		},
		{
			orig: "https://localhost:4001/foo",
			norm: "https://localhost:4001/foo",
		},
		{
			orig: "localhost:4001",
			norm: "http://localhost:4001",
		},
		{
			orig: "localhost",
			norm: "http://localhost",
		},
		{
			orig: ":4001",
			norm: "http://:4001",
		},
	}

	for _, tt := range tests {
		if NormalizeAddr(tt.orig) != tt.norm {
			t.Fatalf("%s not normalized correctly, got: %s", tt.orig, tt.norm)
		}
	}
}

func Test_EnsureHTTPS(t *testing.T) {
	tests := []struct {
		orig    string
		ensured string
	}{
		{
			orig:    "http://localhost:4001",
			ensured: "https://localhost:4001",
		},
		{
			orig:    "https://localhost:4001",
			ensured: "https://localhost:4001",
		},
		{
			orig:    "https://localhost:4001/foo",
			ensured: "https://localhost:4001/foo",
		},
		{
			orig:    "localhost:4001",
			ensured: "https://localhost:4001",
		},
	}

	for _, tt := range tests {
		if e := EnsureHTTPS(tt.orig); e != tt.ensured {
			t.Fatalf("%s not HTTPS ensured correctly, exp %s, got %s", tt.orig, tt.ensured, e)
		}
	}
}

func Test_NewService(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if s == nil {
		t.Fatalf("failed to create new service")
	}
}

func Test_HasVersionHeader(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	s.BuildInfo = map[string]interface{}{
		"version": "the version",
	}
	url := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("failed to make request")
	}

	if resp.Header.Get("X-RQLITE-VERSION") != "the version" {
		t.Fatalf("incorrect build version present in HTTP response header")
	}
}

func Test_HasContentTypeJSON(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	client := &http.Client{}
	resp, err := client.Get(fmt.Sprintf("http://%s/status", s.Addr().String()))
	if err != nil {
		t.Fatalf("failed to make request")
	}

	h := resp.Header.Get("Content-Type")
	if h != "application/json; charset=utf-8" {
		t.Fatalf("incorrect Content-type in HTTP response: %s", h)
	}
}

func Test_HasContentTypeOctetStream(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	client := &http.Client{}
	resp, err := client.Get(fmt.Sprintf("http://%s/db/backup", s.Addr().String()))
	if err != nil {
		t.Fatalf("failed to make request")
	}

	h := resp.Header.Get("Content-Type")
	if h != "application/octet-stream" {
		t.Fatalf("incorrect Content-type in HTTP response: %s", h)
	}
}

func Test_HasVersionHeaderUnknown(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	url := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("failed to make request")
	}

	if resp.Header.Get("X-RQLITE-VERSION") != "unknown" {
		t.Fatalf("incorrect build version present in HTTP response header")
	}
}

func Test_404Routes(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	resp, err := client.Get(host + "/db/xxx")
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 404 {
		t.Fatalf("failed to get expected 404, got %d", resp.StatusCode)
	}

	resp, err = client.Post(host+"/xxx", "", nil)
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 404 {
		t.Fatalf("failed to get expected 404, got %d", resp.StatusCode)
	}
}

func Test_404Routes_ExpvarPprofDisabled(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	for _, path := range []string{
		"/debug/vars",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
	} {
		req, err := http.NewRequest("GET", host+path, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("failed to make request: %s", err.Error())
		}
		if resp.StatusCode != 404 {
			t.Fatalf("failed to get expected 404 for path %s, got %d", path, resp.StatusCode)
		}
	}
}

func Test_405Routes(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	resp, err := client.Get(host + "/db/execute")
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 405 {
		t.Fatalf("failed to get expected 405, got %d", resp.StatusCode)
	}

	resp, err = client.Get(host + "/remove")
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 405 {
		t.Fatalf("failed to get expected 405, got %d", resp.StatusCode)
	}

	resp, err = client.Post(host+"/remove", "", nil)
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 405 {
		t.Fatalf("failed to get expected 405, got %d", resp.StatusCode)
	}

	resp, err = client.Get(host + "/join")
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 405 {
		t.Fatalf("failed to get expected 405, got %d", resp.StatusCode)
	}

	resp, err = client.Post(host+"/db/backup", "", nil)
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 405 {
		t.Fatalf("failed to get expected 405, got %d", resp.StatusCode)
	}

	resp, err = client.Post(host+"/status", "", nil)
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 405 {
		t.Fatalf("failed to get expected 405, got %d", resp.StatusCode)
	}
}

func Test_400Routes(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	resp, err := client.Get(host + "/db/query?q=")
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 400 {
		t.Fatalf("failed to get expected 400, got %d", resp.StatusCode)
	}
}

func Test_401Routes_NoBasicAuth(t *testing.T) {
	c := &mockCredentialStore{CheckOK: false, HasPermOK: false}

	m := &MockStore{}
	s := New("127.0.0.1:0", m, c)
	s.Expvar = true
	s.Pprof = true
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	for _, path := range []string{
		"/db/execute",
		"/db/query",
		"/db/backup",
		"/db/load",
		"/join",
		"/delete",
		"/status",
		"/debug/vars",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
	} {
		resp, err := client.Get(host + path)
		if err != nil {
			t.Fatalf("failed to make request")
		}
		if resp.StatusCode != 401 {
			t.Fatalf("failed to get expected 401 for path %s, got %d", path, resp.StatusCode)
		}
	}
}

func Test_401Routes_BasicAuthBadPassword(t *testing.T) {
	c := &mockCredentialStore{CheckOK: false, HasPermOK: false}

	m := &MockStore{}
	s := New("127.0.0.1:0", m, c)
	s.Expvar = true
	s.Pprof = true
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	for _, path := range []string{
		"/db/execute",
		"/db/query",
		"/db/backup",
		"/db/load",
		"/join",
		"/status",
		"/debug/vars",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
	} {
		req, err := http.NewRequest("GET", host+path, nil)
		if err != nil {
			t.Fatalf("failed to create request: %s", err.Error())
		}
		req.SetBasicAuth("username1", "password1")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("failed to make request: %s", err.Error())
		}
		if resp.StatusCode != 401 {
			t.Fatalf("failed to get expected 401 for path %s, got %d", path, resp.StatusCode)
		}
	}
}

func Test_401Routes_BasicAuthBadPerm(t *testing.T) {
	c := &mockCredentialStore{CheckOK: true, HasPermOK: false}

	m := &MockStore{}
	s := New("127.0.0.1:0", m, c)
	s.Expvar = true
	s.Pprof = true
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	for _, path := range []string{
		"/db/execute",
		"/db/query",
		"/db/backup",
		"/db/load",
		"/join",
		"/status",
		"/debug/vars",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile",
		"/debug/pprof/symbol",
	} {
		req, err := http.NewRequest("GET", host+path, nil)
		if err != nil {
			t.Fatalf("failed to create request: %s", err.Error())
		}
		req.SetBasicAuth("username1", "password1")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("failed to make request: %s", err.Error())
		}
		if resp.StatusCode != 401 {
			t.Fatalf("failed to get expected 401 for path %s, got %d", path, resp.StatusCode)
		}
	}
}

func Test_BackupOK(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	m.backupFn = func(leader bool, f store.BackupFormat, dst io.Writer) error {
		return nil
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup")
	if err != nil {
		t.Fatalf("failed to make backup request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for backup, got %d", resp.StatusCode)
	}
}

func Test_BackupFlagsNoLeader(t *testing.T) {
	m := &MockStore{}
	m.metadata = map[string]string{
		"api_addr":  "1.2.3.4:999",
		"api_proto": "http",
	}

	s := New("127.0.0.1:0", m, nil)

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	m.backupFn = func(leader bool, f store.BackupFormat, dst io.Writer) error {
		return store.ErrNotLeader
	}

	client := &http.Client{}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup")
	if err != nil {
		t.Fatalf("failed to make backup request: %s", err.Error())
	}
	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("failed to get expected StatusServiceUnavailable for backup, got %d", resp.StatusCode)
	}
}

func Test_BackupFlagsNoLeaderOK(t *testing.T) {
	m := &MockStore{}

	s := New("127.0.0.1:0", m, nil)

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	m.backupFn = func(leader bool, f store.BackupFormat, dst io.Writer) error {
		if !leader {
			return nil
		}
		return store.ErrNotLeader
	}

	client := &http.Client{}
	host := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := client.Get(host + "/db/backup?noleader")
	if err != nil {
		t.Fatalf("failed to make backup request")
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get expected StatusOK for backup, got %d", resp.StatusCode)
	}
}

func Test_RegisterStatus(t *testing.T) {
	var stats *mockStatuser
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)

	if err := s.RegisterStatus("foo", stats); err != nil {
		t.Fatalf("failed to register statuser: %s", err.Error())
	}

	if err := s.RegisterStatus("foo", stats); err == nil {
		t.Fatal("successfully re-registered statuser")
	}
}

func Test_LeaderAPIAddrProto(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)

	m.metadata = map[string]string{
		"api_addr":  "1.2.3.4:999",
		"api_proto": "http",
	}
	if addr := s.LeaderAPIAddr(); addr != "1.2.3.4:999" {
		t.Fatalf("incorrect Leader API addresss, got %s", addr)
	}
	if proto := s.LeaderAPIProto(); proto != "http" {
		t.Fatalf("incorrect Leader API proto, got %s", proto)
	}

	m.metadata = map[string]string{
		"api_addr": "1.2.3.4:999",
	}
	if addr := s.LeaderAPIAddr(); addr != "1.2.3.4:999" {
		t.Fatalf("incorrect Leader API addresss, got %s", addr)
	}
	if proto := s.LeaderAPIProto(); proto != "http" {
		t.Fatalf("incorrect Leader API proto, got %s", proto)
	}

	m.metadata = map[string]string{
		"api_addr":  "1.2.3.4:999",
		"api_proto": "https",
	}
	if addr := s.LeaderAPIAddr(); addr != "1.2.3.4:999" {
		t.Fatalf("incorrect Leader API addresss, got %s", addr)
	}
	if proto := s.LeaderAPIProto(); proto != "https" {
		t.Fatalf("incorrect Leader API proto, got %s", proto)
	}
}

func Test_FormRedirect(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	req := mustNewHTTPRequest("http://qux:4001")

	if rd := s.FormRedirect(req, "http", "foo:4001"); rd != "http://foo:4001" {
		t.Fatal("failed to form redirect for simple URL")
	}
	if rd := s.FormRedirect(req, "http", "bar:4002"); rd != "http://bar:4002" {
		t.Fatal("failed to form redirect for simple URL with new host")
	}
}

func Test_FormRedirectParam(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	req := mustNewHTTPRequest("http://qux:4001/db/query?x=y")

	if rd := s.FormRedirect(req, "http", "foo:4001"); rd != "http://foo:4001/db/query?x=y" {
		t.Fatal("failed to form redirect for URL")
	}
	if rd := s.FormRedirect(req, "http", "bar:4003"); rd != "http://bar:4003/db/query?x=y" {
		t.Fatal("failed to form redirect for URL with new host")
	}
}

func Test_FormRedirectHTTPS(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	req := mustNewHTTPRequest("http://qux:4001")

	if rd := s.FormRedirect(req, "https", "foo:4001"); rd != "https://foo:4001" {
		t.Fatal("failed to form redirect for simple URL")
	}
	if rd := s.FormRedirect(req, "https", "bar:4002"); rd != "https://bar:4002" {
		t.Fatal("failed to form redirect for simple URL with new host")
	}
}

func Test_TLSServce(t *testing.T) {
	m := &MockStore{}
	var s *Service
	tempDir := mustTempDir()

	s = New("127.0.0.1:0", m, nil)
	s.CertFile = x509.CertFile(tempDir)
	s.KeyFile = x509.KeyFile(tempDir)
	s.BuildInfo = map[string]interface{}{
		"version": "the version",
	}
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	url := fmt.Sprintf("https://%s", s.Addr().String())

	tn := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tn}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("failed to make request")
	}

	if v := resp.Header.Get("X-RQLITE-VERSION"); v != "the version" {
		t.Fatalf("incorrect build version present in HTTP response header, got: %s", v)
	}
}

type MockStore struct {
	executeFn func(queries []string, tx bool) ([]*sql.Result, error)
	queryFn   func(queries []string, tx, leader, verify bool) ([]*sql.Rows, error)
	backupFn  func(leader bool, f store.BackupFormat, dst io.Writer) error
	leaderID  string
	metadata  map[string]string
}

func (m *MockStore) Execute(er *command.ExecuteRequest) ([]*sql.Result, error) {
	if m.executeFn == nil {
		return nil, nil
	}
	return nil, nil
}

func (m *MockStore) ExecuteOrAbort(er *command.ExecuteRequest) ([]*sql.Result, error) {
	return nil, nil
}

func (m *MockStore) Query(qr *command.QueryRequest) ([]*sql.Rows, error) {
	if m.queryFn == nil {
		return nil, nil
	}
	return nil, nil
}

func (m *MockStore) Join(id, addr string, voter bool, metadata map[string]string) error {
	return nil
}

func (m *MockStore) Remove(id string) error {
	return nil
}

func (m *MockStore) LeaderID() (string, error) {
	return m.leaderID, nil
}

func (m *MockStore) Metadata(id, key string) string {
	// If the preset leaderID is not used in the call, return nothing.
	if id != m.leaderID {
		return ""
	}
	return m.metadata[key]
}

func (m *MockStore) Stats() (map[string]interface{}, error) {
	return nil, nil
}

func (m *MockStore) Backup(leader bool, f store.BackupFormat, w io.Writer) error {
	if m.backupFn == nil {
		return nil
	}
	return m.backupFn(leader, f, w)
}

type mockCredentialStore struct {
	CheckOK   bool
	HasPermOK bool
}

func (m *mockCredentialStore) Check(username, password string) bool {
	return m.CheckOK
}

func (m *mockCredentialStore) HasPerm(username, perm string) bool {
	return m.HasPermOK
}

func (m *mockCredentialStore) HasAnyPerm(username string, perm ...string) bool {
	return m.HasPermOK
}

type mockStatuser struct {
}

func (m *mockStatuser) Stats() (interface{}, error) {
	return nil, nil
}

func mustNewHTTPRequest(url string) *http.Request {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic("failed to create HTTP request for testing")
	}
	return req
}

func mustTempDir() string {
	var err error
	path, err := ioutil.TempDir("", "rqlilte-system-test-")
	if err != nil {
		panic("failed to create temp dir")
	}
	return path
}
