package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v10/proxy"
)

func Test_LeaderPOSTRejectsTrailingJSON(t *testing.T) {
	for _, body := range []string{`{"id":"node1"} {"id":"node2"}`, `{"id":"node1"} junk`, `{} null`} {
		t.Run(body, func(t *testing.T) {
			called := false
			m := &MockStore{stepdownFn: func(bool, string) error { called = true; return nil }}
			c := &mockClusterService{}
			s := New("", m, c, proxy.New(m, c), nil)
			rr := httptest.NewRecorder()
			s.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/leader", strings.NewReader(body)))
			if rr.Code != http.StatusBadRequest || called {
				t.Fatalf("status=%d, stepdown called=%v; want 400 without stepdown", rr.Code, called)
			}
		})
	}
}

func Test_LeaderPOSTAllowsTrailingWhitespace(t *testing.T) {
	called := false
	m := &MockStore{stepdownFn: func(_ bool, id string) error {
		called = true
		if id != "node1" {
			t.Fatalf("unexpected target %q", id)
		}
		return nil
	}}
	c := &mockClusterService{}
	s := New("", m, c, proxy.New(m, c), nil)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/leader", strings.NewReader("{\"id\":\"node1\"}\n\t ")))
	if rr.Code != http.StatusOK || !called {
		t.Fatalf("status=%d, stepdown called=%v; want 200 with stepdown", rr.Code, called)
	}
}
