package cluster

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rqlite/rqlite/rtls"
)

const numAttempts int = 3
const attemptInterval = 1 * time.Second

func Test_SingleJoinOK(t *testing.T) {
	var body map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		w.WriteHeader(http.StatusOK)

		if r.Header["Content-Type"][0] != "application/json" {
			t.Fatalf("incorrect Content-Type set")
		}

		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if err := json.Unmarshal(b, &body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}))
	defer ts.Close()

	joiner := NewJoiner("127.0.0.1", numAttempts, attemptInterval, nil)

	j, err := joiner.Do([]string{ts.URL}, "id0", "127.0.0.1:9090", false)
	if err != nil {
		t.Fatalf("failed to join a single node: %s", err.Error())
	}
	if j != ts.URL+"/join" {
		t.Fatalf("node joined using wrong endpoint, exp: %s, got: %s", j, ts.URL)
	}

	if got, exp := body["id"].(string), "id0"; got != exp {
		t.Fatalf("wrong node ID supplied, exp %s, got %s", exp, got)
	}
	if got, exp := body["addr"].(string), "127.0.0.1:9090"; got != exp {
		t.Fatalf("wrong address supplied, exp %s, got %s", exp, got)
	}
	if got, exp := body["voter"].(bool), false; got != exp {
		t.Fatalf("wrong voter state supplied, exp %v, got %v", exp, got)
	}
}

func Test_SingleJoinHTTPSOK(t *testing.T) {
	var body map[string]interface{}
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		w.WriteHeader(http.StatusOK)

		if r.Header["Content-Type"][0] != "application/json" {
			t.Fatalf("incorrect Content-Type set")
		}

		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if err := json.Unmarshal(b, &body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}))
	defer ts.Close()
	ts.TLS = &tls.Config{NextProtos: []string{"h2", "http/1.1"}}
	ts.StartTLS()

	tlsConfig, err := rtls.CreateClientConfig("", "", "", true, false)
	if err != nil {
		t.Fatalf("failed to create TLS config: %s", err.Error())
	}
	joiner := NewJoiner("127.0.0.1", numAttempts, attemptInterval, tlsConfig)

	j, err := joiner.Do([]string{ts.URL}, "id0", "127.0.0.1:9090", false)
	if err != nil {
		t.Fatalf("failed to join a single node: %s", err.Error())
	}
	if j != ts.URL+"/join" {
		t.Fatalf("node joined using wrong endpoint, exp: %s, got: %s", j, ts.URL)
	}

	if got, exp := body["id"].(string), "id0"; got != exp {
		t.Fatalf("wrong node ID supplied, exp %s, got %s", exp, got)
	}
	if got, exp := body["addr"].(string), "127.0.0.1:9090"; got != exp {
		t.Fatalf("wrong address supplied, exp %s, got %s", exp, got)
	}
	if got, exp := body["voter"].(bool), false; got != exp {
		t.Fatalf("wrong voter state supplied, exp %v, got %v", exp, got)
	}
}

func Test_SingleJoinOKBasicAuth(t *testing.T) {
	var body map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		w.WriteHeader(http.StatusOK)

		username, password, ok := r.BasicAuth()
		if !ok {
			t.Fatalf("request did not have Basic Auth credentials")
		}
		if username != "user1" || password != "password1" {
			t.Fatalf("bad Basic Auth credentials received (%s, %s", username, password)
		}

		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if err := json.Unmarshal(b, &body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}))
	defer ts.Close()

	joiner := NewJoiner("127.0.0.1", numAttempts, attemptInterval, nil)
	joiner.SetBasicAuth("user1", "password1")

	j, err := joiner.Do([]string{ts.URL}, "id0", "127.0.0.1:9090", false)
	if err != nil {
		t.Fatalf("failed to join a single node: %s", err.Error())
	}
	if j != ts.URL+"/join" {
		t.Fatalf("node joined using wrong endpoint, exp: %s, got: %s", j, ts.URL)
	}

	if got, exp := body["id"].(string), "id0"; got != exp {
		t.Fatalf("wrong node ID supplied, exp %s, got %s", exp, got)
	}
	if got, exp := body["addr"].(string), "127.0.0.1:9090"; got != exp {
		t.Fatalf("wrong address supplied, exp %s, got %s", exp, got)
	}
	if got, exp := body["voter"].(bool), false; got != exp {
		t.Fatalf("wrong voter state supplied, exp %v, got %v", exp, got)
	}
}

func Test_SingleJoinZeroAttempts(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("handler should not have been called")
	}))

	joiner := NewJoiner("127.0.0.1", 0, attemptInterval, nil)
	_, err := joiner.Do([]string{ts.URL}, "id0", "127.0.0.1:9090", false)
	if err != ErrJoinFailed {
		t.Fatalf("Incorrect error returned when zero attempts specified")
	}
}

func Test_SingleJoinFail(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()

	joiner := NewJoiner("", 0, attemptInterval, nil)
	_, err := joiner.Do([]string{ts.URL}, "id0", "127.0.0.1:9090", true)
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

	joiner := NewJoiner("127.0.0.1", numAttempts, attemptInterval, nil)

	j, err := joiner.Do([]string{ts1.URL, ts2.URL}, "id0", "127.0.0.1:9090", true)
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

	joiner := NewJoiner("", numAttempts, attemptInterval, nil)

	j, err := joiner.Do([]string{ts1.URL, ts2.URL}, "id0", "127.0.0.1:9090", true)
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
	redirectAddr := fmt.Sprintf("%s%s", ts1.URL, "/join")

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectAddr, http.StatusMovedPermanently)
	}))
	defer ts2.Close()

	joiner := NewJoiner("127.0.0.1", numAttempts, attemptInterval, nil)

	j, err := joiner.Do([]string{ts2.URL}, "id0", "127.0.0.1:9090", true)
	if err != nil {
		t.Fatalf("failed to join a single node: %s", err.Error())
	}
	if j != redirectAddr {
		t.Fatalf("node joined using wrong endpoint, exp: %s, got: %s", redirectAddr, j)
	}
}
