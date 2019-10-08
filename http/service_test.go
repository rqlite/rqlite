package http

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/lkm1321/rqlite/store"
)

func Test_NormalizeAddr(t *testing.T) {
	t.Parallel()

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

func Test_NewService(t *testing.T) {
	t.Parallel()

	m := NewMockStore()
	s := New("127.0.0.1:0", m, nil)
	if s == nil {
		t.Fatalf("failed to create new service")
	}
}

func Test_HasVersionHeader(t *testing.T) {
	t.Parallel()

	m := NewMockStore()
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
	t.Parallel()

	m := NewMockStore()
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
	t.Parallel()

	m := NewMockStore()
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
	t.Parallel()

	m := NewMockStore()
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

func Test_CreateConnection(t *testing.T) {
	t.Parallel()

	m := NewMockStore()
	s := New("127.0.0.1:0", m, nil)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start HTTP server: %s", err.Error())
	}
	defer s.Close()
	r, err := http.Post(fmt.Sprintf("http://%s/db/connections", s.Addr().String()), "", nil)
	if err != nil {
		t.Fatalf("failed to make connections request: %s", err.Error())
	}
	if r.StatusCode != http.StatusCreated {
		t.Fatalf("incorrect status code received: %s", r.Status)
	}
	loc, err := r.Location()
	if err != nil {
		t.Fatalf("failed to get Location header value: %s", err.Error())
	}

	re := regexp.MustCompile(fmt.Sprintf("^http://%s/db/connections/[0-9]+$", s.Addr().String()))
	if !re.Match([]byte(loc.String())) {
		t.Fatalf("connection URL is incorrect format: %s", loc.String())
	}
}

func Test_404Routes(t *testing.T) {
	t.Parallel()

	m := NewMockStore()
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
	t.Parallel()

	m := NewMockStore()
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
	t.Parallel()

	m := NewMockStore()
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
	t.Parallel()

	m := NewMockStore()
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
	t.Parallel()

	c := &mockCredentialStore{CheckOK: false, HasPermOK: false}

	m := NewMockStore()
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
		"/db/connections",
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
	t.Parallel()

	c := &mockCredentialStore{CheckOK: false, HasPermOK: false}

	m := NewMockStore()
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
		"/db/connections",
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
	t.Parallel()

	c := &mockCredentialStore{CheckOK: true, HasPermOK: false}

	m := NewMockStore()
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
		"/db/connections",
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

func Test_RegisterStatus(t *testing.T) {
	t.Parallel()

	var stats *mockStatuser
	m := NewMockStore()
	s := New("127.0.0.1:0", m, nil)

	if err := s.RegisterStatus("foo", stats); err != nil {
		t.Fatalf("failed to register statuser: %s", err.Error())
	}

	if err := s.RegisterStatus("foo", stats); err == nil {
		t.Fatal("successfully re-registered statuser")
	}
}

func Test_FormRedirect(t *testing.T) {
	t.Parallel()

	m := NewMockStore()
	s := New("127.0.0.1:0", m, nil)
	req := mustNewHTTPRequest("http://foo:4001")

	if rd := s.FormRedirect(req, "foo:4001"); rd != "http://foo:4001" {
		t.Fatal("failed to form redirect for simple URL")
	}
	if rd := s.FormRedirect(req, "bar:4002"); rd != "http://bar:4002" {
		t.Fatal("failed to form redirect for simple URL with new host")
	}
}

func Test_FormRedirectParam(t *testing.T) {
	t.Parallel()

	m := NewMockStore()
	s := New("127.0.0.1:0", m, nil)
	req := mustNewHTTPRequest("http://foo:4001/db/query?x=y")

	if rd := s.FormRedirect(req, "foo:4001"); rd != "http://foo:4001/db/query?x=y" {
		t.Fatal("failed to form redirect for URL")
	}
	if rd := s.FormRedirect(req, "bar:4003"); rd != "http://bar:4003/db/query?x=y" {
		t.Fatal("failed to form redirect for URL with new host")
	}
}

func Test_FormConnectionURL(t *testing.T) {
	t.Parallel()

	m := NewMockStore()
	s := New("127.0.0.1:0", m, nil)

	req := mustNewHTTPRequest("http://foo:4001/db/connections")
	if got, exp := s.FormConnectionURL(req, 1234), "http://foo:4001/db/connections/1234"; got != exp {
		t.Fatalf("failed to form redirect for URL:\ngot %s\nexp %s\n", got, exp)
	}
	req = mustNewHTTPRequest("http://foo/db/connections")
	if got, exp := s.FormConnectionURL(req, 1234), "http://foo/db/connections/1234"; got != exp {
		t.Fatalf("failed to form redirect for URL:\ngot %s\nexp %s\n", got, exp)
	}
	req = mustNewHTTPRequest("http://foo/db/connections?w=x&y=z")
	if got, exp := s.FormConnectionURL(req, 1234), "http://foo/db/connections/1234"; got != exp {
		t.Fatalf("failed to form redirect for URL:\ngot %s\nexp %s\n", got, exp)
	}
}

func Test_ConnectionTimingParams(t *testing.T) {
	t.Parallel()

	var d time.Duration
	var b bool
	var err error
	var req *http.Request

	req, _ = http.NewRequest(http.MethodPost, "http://foo?tx_timeout=1 0s", nil)
	_, _, err = txTimeout(req)
	if err == nil {
		t.Fatal("failed")
	}

	req, _ = http.NewRequest(http.MethodPost, "http://foo?tx_timeout=10s", nil)
	d, b, err = txTimeout(req)
	if d != 10*time.Second || !b || err != nil {
		t.Fatal("failed")
	}

	req, _ = http.NewRequest(http.MethodPost, "http://foo?tx_timeout=0s", nil)
	d, b, err = txTimeout(req)
	if d != 0 || !b || err != nil {
		t.Fatal("failed")
	}

	req, _ = http.NewRequest(http.MethodPost, "http://foo", nil)
	_, b, err = txTimeout(req)
	if b || err != nil {
		t.Fatal("failed")
	}

	req, _ = http.NewRequest(http.MethodPost, "http://foo?idle_timeout=1 0s", nil)
	_, _, err = idleTimeout(req)
	if err == nil {
		t.Fatal("failed")
	}

	req, _ = http.NewRequest(http.MethodPost, "http://foo?idle_timeout=10s", nil)
	d, b, err = idleTimeout(req)
	if d != 10*time.Second || !b || err != nil {
		t.Fatal("failed")
	}

	req, _ = http.NewRequest(http.MethodPost, "http://foo?idle_timeout=0s", nil)
	d, b, err = idleTimeout(req)
	if d != 0 || !b || err != nil {
		t.Fatal("failed")
	}

	req, _ = http.NewRequest(http.MethodPost, "http://foo", nil)
	_, b, err = idleTimeout(req)
	if b || err != nil {
		t.Fatal("failed")
	}

}

type MockStore struct {
	executeFn func(queries []string, tx bool) (*store.ExecuteResponse, error)
	queryFn   func(queries []string, tx, leader, verify bool) (*store.QueryResponse, error)
	conns     map[uint64]*store.Connection
}

func NewMockStore() *MockStore {
	return &MockStore{
		conns: make(map[uint64]*store.Connection),
	}
}

func (m *MockStore) Execute(er *store.ExecuteRequest) (*store.ExecuteResponse, error) {
	if m.executeFn == nil {
		return nil, nil
	}
	return nil, nil
}

func (m *MockStore) ExecuteOrAbort(er *store.ExecuteRequest) (*store.ExecuteResponse, error) {
	return nil, nil
}

func (m *MockStore) Query(qr *store.QueryRequest) (*store.QueryResponse, error) {
	if m.queryFn == nil {
		return nil, nil
	}
	return nil, nil
}

func (m *MockStore) Connect(opt *store.ConnectionOptions) (*store.Connection, error) {
	id := rand.Uint64()
	m.conns[id] = store.NewConnection(nil, nil, id, 0, 0)
	return m.conns[id], nil
}

func (m *MockStore) Connection(id uint64) (*store.Connection, bool) {
	c, ok := m.conns[id]
	return c, ok
}

func (m *MockStore) Join(id, addr string, metadata map[string]string) error {
	return nil
}

func (m *MockStore) Remove(id string) error {
	return nil
}

func (m *MockStore) LeaderID() (string, error) {
	return "", nil
}

func (m *MockStore) Metadata(id, key string) string {
	return ""
}

func (m *MockStore) Stats() (map[string]interface{}, error) {
	return nil, nil
}

func (m *MockStore) Backup(leader bool, f store.BackupFormat, w io.Writer) error {
	return nil
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
