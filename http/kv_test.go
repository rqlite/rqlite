package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v10/auth"
	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/proxy"
	"github.com/rqlite/rqlite/v10/store"
)

func newKVTestService(t *testing.T, st *MockStore, cred *mockCredentialStore) *Service {
	t.Helper()
	cl := &mockClusterService{apiAddr: "http://leader-host:4001"}
	if cred == nil {
		cred = &mockCredentialStore{HasPermOK: true}
	}
	return New("127.0.0.1:4001", st, cl, proxy.New(st, cl), cred)
}

func doKVRequest(t *testing.T, s *Service, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	var req *http.Request
	var err error
	if rdr != nil {
		req, err = http.NewRequest(method, path, rdr)
	} else {
		req, err = http.NewRequest(method, path, nil)
	}
	if err != nil {
		t.Fatalf("failed to build %s %s: %s", method, path, err)
	}
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	return rr
}

func Test_KV_HTTP_Put_OK(t *testing.T) {
	var (
		gotKey string
		gotVal []byte
		called bool
	)
	st := &MockStore{
		setKeyFn: func(key string, value []byte) (uint64, error) {
			called = true
			gotKey = key
			gotVal = append([]byte(nil), value...)
			return 42, nil
		},
	}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodPut, "/kv/alpha", []byte("hello"))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if !called {
		t.Fatal("SetKey was not called")
	}
	if gotKey != "alpha" {
		t.Fatalf("key passed to store: %q, want %q", gotKey, "alpha")
	}
	if !bytes.Equal(gotVal, []byte("hello")) {
		t.Fatalf("value passed to store: %q, want %q", gotVal, "hello")
	}
}

func Test_KV_HTTP_Put_URLEncodedKey(t *testing.T) {
	var gotKey string
	st := &MockStore{
		setKeyFn: func(key string, value []byte) (uint64, error) {
			gotKey = key
			return 1, nil
		},
	}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodPut, "/kv/a%2Fb%20c", []byte("v"))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if gotKey != "a/b c" {
		t.Fatalf("key decoded as %q, want %q", gotKey, "a/b c")
	}
}

func Test_KV_HTTP_Put_BinaryBody(t *testing.T) {
	var gotVal []byte
	st := &MockStore{
		setKeyFn: func(key string, value []byte) (uint64, error) {
			gotVal = append([]byte(nil), value...)
			return 1, nil
		},
	}
	s := newKVTestService(t, st, nil)

	val := make([]byte, 256)
	for i := range val {
		val[i] = byte(i)
	}
	rr := doKVRequest(t, s, http.MethodPut, "/kv/bin", val)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if !bytes.Equal(gotVal, val) {
		t.Fatalf("binary body round-trip mismatch")
	}
}

func Test_KV_HTTP_Put_EmptyKey(t *testing.T) {
	st := &MockStore{
		setKeyFn: func(key string, value []byte) (uint64, error) {
			t.Fatal("SetKey should not be called for empty key")
			return 0, nil
		},
	}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodPut, "/kv/", []byte("v"))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func Test_KV_HTTP_Put_NotLeader_Redirect(t *testing.T) {
	st := &MockStore{
		leaderAddr: "127.0.0.1:9001",
		setKeyFn: func(key string, value []byte) (uint64, error) {
			return 0, store.ErrNotLeader
		},
	}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodPut, "/kv/foo", []byte("v"))
	if rr.Code != http.StatusMovedPermanently {
		t.Fatalf("expected 301, got %d", rr.Code)
	}
	loc := rr.Header().Get("Location")
	if !strings.Contains(loc, "leader-host:4001") {
		t.Fatalf("Location header %q does not point at leader", loc)
	}
	if !strings.Contains(loc, "/kv/foo") {
		t.Fatalf("Location header %q missing kv path", loc)
	}
}

func Test_KV_HTTP_Put_NotLeader_NoLeader(t *testing.T) {
	st := &MockStore{
		setKeyFn: func(key string, value []byte) (uint64, error) {
			return 0, store.ErrNotLeader
		},
	}
	cl := &mockClusterService{apiAddr: ""} // no leader URL
	cred := &mockCredentialStore{HasPermOK: true}
	s := New("127.0.0.1:4001", st, cl, proxy.New(st, cl), cred)

	rr := doKVRequest(t, s, http.MethodPut, "/kv/foo", []byte("v"))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}

func Test_KV_HTTP_Get_OK(t *testing.T) {
	st := &MockStore{
		getKeyFn: func(key string, level command.ConsistencyLevel, _ int64, _ bool, _ int64) ([]byte, uint64, error) {
			if key != "alpha" {
				t.Fatalf("expected key alpha, got %q", key)
			}
			return []byte("world"), 7, nil
		},
	}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodGet, "/kv/alpha", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if got := rr.Body.String(); got != "world" {
		t.Fatalf("body %q, want %q", got, "world")
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("Content-Type %q, want application/octet-stream", ct)
	}
}

func Test_KV_HTTP_Get_Missing(t *testing.T) {
	st := &MockStore{
		getKeyFn: func(key string, _ command.ConsistencyLevel, _ int64, _ bool, _ int64) ([]byte, uint64, error) {
			return nil, 0, store.ErrKVKeyNotFound
		},
	}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodGet, "/kv/nope", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func Test_KV_HTTP_Get_LevelStrong(t *testing.T) {
	var gotLevel command.ConsistencyLevel
	st := &MockStore{
		getKeyFn: func(_ string, level command.ConsistencyLevel, _ int64, _ bool, _ int64) ([]byte, uint64, error) {
			gotLevel = level
			return []byte("v"), 1, nil
		},
	}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodGet, "/kv/k?level=strong", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if gotLevel != command.ConsistencyLevel_STRONG {
		t.Fatalf("level passed to store: %v, want STRONG", gotLevel)
	}
}

func Test_KV_HTTP_Get_RaftIndexHeader(t *testing.T) {
	st := &MockStore{
		getKeyFn: func(_ string, _ command.ConsistencyLevel, _ int64, _ bool, _ int64) ([]byte, uint64, error) {
			return []byte("v"), 99, nil
		},
	}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodGet, "/kv/k?raft_index", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if got := rr.Header().Get("X-RQLITE-RAFT-INDEX"); got != "99" {
		t.Fatalf("X-RQLITE-RAFT-INDEX %q, want 99", got)
	}
}

func Test_KV_HTTP_Delete_OK(t *testing.T) {
	var gotKey string
	st := &MockStore{
		deleteKeyFn: func(key string) (uint64, error) {
			gotKey = key
			return 5, nil
		},
	}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodDelete, "/kv/alpha", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if gotKey != "alpha" {
		t.Fatalf("key passed to store: %q, want alpha", gotKey)
	}
}

func Test_KV_HTTP_Delete_Missing(t *testing.T) {
	st := &MockStore{
		deleteKeyFn: func(key string) (uint64, error) {
			return 0, store.ErrKVKeyNotFound
		},
	}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodDelete, "/kv/nope", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func Test_KV_HTTP_MethodNotAllowed(t *testing.T) {
	st := &MockStore{}
	s := newKVTestService(t, st, nil)

	rr := doKVRequest(t, s, http.MethodPatch, "/kv/k", []byte("v"))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
	if allow := rr.Header().Get("Allow"); !strings.Contains(allow, "PUT") {
		t.Fatalf("Allow header %q missing PUT", allow)
	}
}

func Test_KV_HTTP_Unauthorized(t *testing.T) {
	calls := 0
	st := &MockStore{
		setKeyFn: func(key string, value []byte) (uint64, error) {
			calls++
			return 1, nil
		},
		getKeyFn: func(key string, _ command.ConsistencyLevel, _ int64, _ bool, _ int64) ([]byte, uint64, error) {
			calls++
			return []byte("v"), 1, nil
		},
		deleteKeyFn: func(key string) (uint64, error) {
			calls++
			return 1, nil
		},
	}
	cred := &mockCredentialStore{
		aaFunc: func(username, password, perm string) bool {
			return perm != auth.PermKV
		},
	}
	s := newKVTestService(t, st, cred)

	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		var body []byte
		if m == http.MethodPut {
			body = []byte("v")
		}
		rr := doKVRequest(t, s, m, "/kv/foo", body)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("%s without PermKV: expected 401, got %d", m, rr.Code)
		}
	}
	if calls != 0 {
		t.Fatalf("store was invoked %d times despite missing PermKV", calls)
	}
}

func Test_KV_HTTP_Stats(t *testing.T) {
	ResetStats()
	st := &MockStore{
		setKeyFn: func(key string, value []byte) (uint64, error) { return 1, nil },
		getKeyFn: func(_ string, _ command.ConsistencyLevel, _ int64, _ bool, _ int64) ([]byte, uint64, error) {
			return []byte("v"), 1, nil
		},
		deleteKeyFn: func(key string) (uint64, error) { return 1, nil },
	}
	s := newKVTestService(t, st, nil)

	doKVRequest(t, s, http.MethodPut, "/kv/a", []byte("v"))
	doKVRequest(t, s, http.MethodGet, "/kv/a", nil)
	doKVRequest(t, s, http.MethodDelete, "/kv/a", nil)

	if got := stats.Get(numKVRequests).String(); got != "3" {
		t.Fatalf("numKVRequests = %s, want 3", got)
	}
	if got := stats.Get(numKVPuts).String(); got != "1" {
		t.Fatalf("numKVPuts = %s, want 1", got)
	}
	if got := stats.Get(numKVGets).String(); got != "1" {
		t.Fatalf("numKVGets = %s, want 1", got)
	}
	if got := stats.Get(numKVDeletes).String(); got != "1" {
		t.Fatalf("numKVDeletes = %s, want 1", got)
	}
}

func Test_KV_HTTP_NotFoundIncrementsCounter(t *testing.T) {
	ResetStats()
	st := &MockStore{
		getKeyFn: func(_ string, _ command.ConsistencyLevel, _ int64, _ bool, _ int64) ([]byte, uint64, error) {
			return nil, 0, store.ErrKVKeyNotFound
		},
		deleteKeyFn: func(_ string) (uint64, error) { return 0, store.ErrKVKeyNotFound },
	}
	s := newKVTestService(t, st, nil)

	doKVRequest(t, s, http.MethodGet, "/kv/missing", nil)
	doKVRequest(t, s, http.MethodDelete, "/kv/missing", nil)
	if got := stats.Get(numKVNotFound).String(); got != "2" {
		t.Fatalf("numKVNotFound = %s, want 2", got)
	}
}

func Test_KV_HTTP_Put_TooLarge(t *testing.T) {
	st := &MockStore{
		setKeyFn: func(_ string, _ []byte) (uint64, error) {
			t.Fatal("SetKey should not be called for oversized body")
			return 0, nil
		},
	}
	s := newKVTestService(t, st, nil)

	big := make([]byte, MaxKVValueBytes+1)
	rr := doKVRequest(t, s, http.MethodPut, "/kv/big", big)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}
}

// ensure newKVTestService compiles against the proxy package shape.
var _ = context.Background
