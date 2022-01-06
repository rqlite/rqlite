package cluster

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const numAttempts int = 3
const attemptInterval = 5 * time.Second

func Test_AddUserInfo(t *testing.T) {
	var u string
	var err error

	u, err = AddUserInfo("http://example.com", "user1", "pass1")
	if err != nil {
		t.Fatalf("failed to add user info: %s", err.Error())
	}
	if exp, got := "http://user1:pass1@example.com", u; exp != got {
		t.Fatalf("wrong URL created, exp %s, got %s", exp, got)
	}

	u, err = AddUserInfo("http://example.com", "user1", "")
	if err != nil {
		t.Fatalf("failed to add user info: %s", err.Error())
	}
	if exp, got := "http://user1:@example.com", u; exp != got {
		t.Fatalf("wrong URL created, exp %s, got %s", exp, got)
	}

	u, err = AddUserInfo("http://example.com", "", "pass1")
	if err != nil {
		t.Fatalf("failed to add user info: %s", err.Error())
	}
	if exp, got := "http://example.com", u; exp != got {
		t.Fatalf("wrong URL created, exp %s, got %s", exp, got)
	}

	u, err = AddUserInfo("http://user1:pass1@example.com", "user2", "pass2")
	if err == nil {
		t.Fatalf("failed to get expected error when UserInfo exists")
	}
}

func Test_RemoveUserInfo(t *testing.T) {
	if exp, got := "http://example.com", RemoveUserInfo("http://user1:pass1@example.com"); exp != got {
		t.Fatalf("expected %s, got %s", exp, got)
	}
	if exp, got := "http://example.com", RemoveUserInfo("http://example.com"); exp != got {
		t.Fatalf("expected %s, got %s", exp, got)
	}
	if exp, got := "nonsense", RemoveUserInfo("nonsense"); exp != got {
		t.Fatalf("expected %s, got %s", exp, got)
	}
}

func Test_SingleJoinOK(t *testing.T) {
	var body map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		w.WriteHeader(http.StatusOK)

		b, err := ioutil.ReadAll(r.Body)
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

	j, err := Join("127.0.0.1", []string{ts.URL}, "id0", "127.0.0.1:9090", false,
		numAttempts, attemptInterval, nil)
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

	_, err := Join("127.0.0.1", []string{ts.URL}, "id0", "127.0.0.1:9090", false, 0, attemptInterval, nil)
	if err != ErrJoinFailed {
		t.Fatalf("Incorrect error returned when zero attempts specified")
	}
}

func Test_SingleJoinFail(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()

	_, err := Join("", []string{ts.URL}, "id0", "127.0.0.1:9090", true,
		numAttempts, attemptInterval, nil)
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

	j, err := Join("127.0.0.1", []string{ts1.URL, ts2.URL}, "id0", "127.0.0.1:9090", true,
		numAttempts, attemptInterval, nil)
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

	j, err := Join("", []string{ts1.URL, ts2.URL}, "id0", "127.0.0.1:9090", true,
		numAttempts, attemptInterval, nil)
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

	j, err := Join("127.0.0.1", []string{ts2.URL}, "id0", "127.0.0.1:9090", true,
		numAttempts, attemptInterval, nil)
	if err != nil {
		t.Fatalf("failed to join a single node: %s", err.Error())
	}
	if j != redirectAddr {
		t.Fatalf("node joined using wrong endpoint, exp: %s, got: %s", redirectAddr, j)
	}
}
