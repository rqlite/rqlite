package disco

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
)

func Test_NewClient(t *testing.T) {
	c := New("https://discovery.rqlite.com")
	if c == nil {
		t.Fatal("failed to create new disco client")
	}

	if c.URL() != "https://discovery.rqlite.com" {
		t.Fatal("configured address of disco service is incorrect")
	}
}

func Test_ClientRegisterBadRequest(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()

	c := New(ts.URL)
	_, err := c.Register("http://127.0.0.1")
	if err == nil {
		t.Fatalf("failed to receive error on 400 from server")
	}
}

func Test_ClientRegisterNotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	c := New(ts.URL)
	_, err := c.Register("http://127.0.0.1")
	if err == nil {
		t.Fatalf("failed to receive error on 404 from server")
	}
}

func Test_ClientRegisterForbidden(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ts.Close()

	c := New(ts.URL)
	_, err := c.Register("http://127.0.0.1")
	if err == nil {
		t.Fatalf("failed to receive error on 403 from server")
	}
}

func Test_ClientRegisterRequestOK(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request from client: %s", err.Error())
		}

		m := map[string]string{}
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("failed to unmarshal request from client: %s", err.Error())
		}

		if m["addr"] != "http://127.0.0.1" {
			t.Fatalf("incorrect join address supplied by client: %s", m["addr"])
		}
		if m["GOOS"] != runtime.GOOS {
			t.Fatalf("incorrect GOOS supplied by client: %s", m["GOOS"])
		}
		if m["GOARCH"] != runtime.GOARCH {
			t.Fatalf("incorrect GOOS supplied by client: %s", m["GOARCH"])
		}

		fmt.Fprintln(w, `[]`)
	}))
	defer ts.Close()

	c := New(ts.URL)
	nodes, err := c.Register("http://127.0.0.1")
	if err != nil {
		t.Fatalf("failed to register: %s", err.Error())
	}
	if len(nodes) != 0 {
		t.Fatalf("failed to receive empty list of nodes, got %v", nodes)
	}
}

func Test_ClientRegisterLeaderOK(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		fmt.Fprintln(w, `[]`)
	}))
	defer ts.Close()

	c := New(ts.URL)
	nodes, err := c.Register("http://127.0.0.1")
	if err != nil {
		t.Fatalf("failed to register: %s", err.Error())
	}
	if len(nodes) != 0 {
		t.Fatalf("failed to receive empty list of nodes, got %v", nodes)
	}
}

func Test_ClientRegisterFollowerOK(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		fmt.Fprintln(w, `["http://1.1.1.1"]`)
	}))
	defer ts.Close()

	c := New(ts.URL)
	nodes, err := c.Register("http://2.2.2.2")
	if err != nil {
		t.Fatalf("failed to register: %s", err.Error())
	}
	if len(nodes) != 1 {
		t.Fatalf("failed to receive non-empty list of nodes")
	}
	if nodes[0] != `http://1.1.1.1` {
		t.Fatalf("got incorrect node, got %v", nodes[0])
	}
}

func Test_ClientRegisterFollowerMultiOK(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		fmt.Fprintln(w, `["http://1.1.1.1", "http://2.2.2.2"]`)
	}))
	defer ts.Close()

	c := New(ts.URL)
	nodes, err := c.Register("http://3.3.3.3")
	if err != nil {
		t.Fatalf("failed to register: %s", err.Error())
	}
	if len(nodes) != 2 {
		t.Fatalf("failed to receive non-empty list of nodes")
	}
	if nodes[0] != `http://1.1.1.1` {
		t.Fatalf("got incorrect first node, got %v", nodes[0])
	}
	if nodes[1] != `http://2.2.2.2` {
		t.Fatalf("got incorrect second node, got %v", nodes[1])
	}
}
