package cluster

import (
	"fmt"
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

	j, err := Join([]string{ts.URL}, "127.0.0.1:9090", true)
	if err != nil {
		t.Fatalf("failed to join a single node: %s", err.Error())
	}
	if j != ts.URL+"/join" {
		t.Fatalf("node joined using wrong endpoint, exp: %s, got: %s", j, ts.URL)
	}
}

func Test_SingleJoinFail(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()

	_, err := Join([]string{ts.URL}, "127.0.0.1:9090", true)
	if err == nil {
		t.Fatalf("expected error when joining bad node")
	}
}

func Test_DoubleJoinOK(t *testing.T) {
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer ts1.Close()
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer ts2.Close()

	j, err := Join([]string{ts1.URL, ts2.URL}, "127.0.0.1:9090", true)
	if err != nil {
		t.Fatalf("failed to join a single node: %s", err.Error())
	}
	if j != ts1.URL+"/join" {
		t.Fatalf("node joined using wrong endpoint, exp: %s, got: %s", j, ts1.URL)
	}
}

func Test_DoubleJoinOKSecondNode(t *testing.T) {
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts1.Close()
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer ts2.Close()

	j, err := Join([]string{ts1.URL, ts2.URL}, "127.0.0.1:9090", true)
	if err != nil {
		t.Fatalf("failed to join a single node: %s", err.Error())
	}
	if j != ts2.URL+"/join" {
		t.Fatalf("node joined using wrong endpoint, exp: %s, got: %s", j, ts2.URL)
	}
}

func Test_DoubleJoinOKSecondNodeRedirect(t *testing.T) {
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer ts1.Close()
	joinAddr := fmt.Sprintf("%s%s", ts1.URL, "/join")

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, joinAddr, http.StatusMovedPermanently)
	}))
	defer ts2.Close()

	j, err := Join([]string{ts2.URL, ts1.URL}, "127.0.0.1:9090", true)
	if err != nil {
		t.Fatalf("failed to join a single node: %s", err.Error())
	}
	if j != joinAddr {
		t.Fatalf("node joined using wrong endpoint, exp: %s, got: %s", joinAddr, j)
	}
}
