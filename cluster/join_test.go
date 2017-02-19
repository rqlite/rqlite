package cluster

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func Test_SingleJoinOK(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	_, err := Join([]string{ts.URL}, "127.0.0.1:9090", true)
	if err != nil {
		t.Fatalf("failed to join a single node: %s", err.Error())
	}
}
