package cluster

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/rqlite/rqlite/rtls"
)

func Test_AddressProviderString(t *testing.T) {
	a := []string{"a", "b", "c"}
	p := NewAddressProviderString(a)
	b, err := p.Lookup()
	if err != nil {
		t.Fatalf("failed to lookup addresses: %s", err.Error())
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("failed to get correct addresses")
	}
}

func Test_NewBootstrapper(t *testing.T) {
	bs := NewBootstrapper(nil, nil)
	if bs == nil {
		t.Fatalf("failed to create a simple Bootstrapper")
	}
	if exp, got := BootUnknown, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootDoneImmediately(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("client made HTTP request")
	}))

	done := func() bool {
		return true
	}
	p := NewAddressProviderString([]string{ts.URL})
	bs := NewBootstrapper(p, nil)
	if err := bs.Boot("node1", "192.168.1.1:1234", done, 10*time.Second); err != nil {
		t.Fatalf("failed to boot: %s", err)
	}
	if exp, got := BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	done := func() bool {
		return false
	}
	p := NewAddressProviderString([]string{ts.URL})
	bs := NewBootstrapper(p, nil)
	bs.Interval = time.Second
	err := bs.Boot("node1", "192.168.1.1:1234", done, 5*time.Second)
	if err == nil {
		t.Fatalf("no error returned from timed-out boot")
	}
	if !errors.Is(err, ErrBootTimeout) {
		t.Fatalf("wrong error returned")
	}
	if exp, got := BootTimeout, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootSingleJoin(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/join" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
	}))

	done := func() bool {
		return false
	}

	p := NewAddressProviderString([]string{ts.URL})
	bs := NewBootstrapper(p, nil)
	bs.Interval = time.Second

	err := bs.Boot("node1", "192.168.1.1:1234", done, 60*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}
	if exp, got := BootJoin, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootSingleNotify(t *testing.T) {
	tsNotified := false
	var body map[string]string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/join" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		tsNotified = true
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

	n := -1
	done := func() bool {
		n++
		return n == 5
	}

	p := NewAddressProviderString([]string{ts.URL})
	bs := NewBootstrapper(p, nil)
	bs.Interval = time.Second

	err := bs.Boot("node1", "192.168.1.1:1234", done, 60*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}

	if tsNotified != true {
		t.Fatalf("notify target not contacted")
	}

	if got, exp := body["id"], "node1"; got != exp {
		t.Fatalf("wrong node ID supplied, exp %s, got %s", exp, got)
	}
	if got, exp := body["addr"], "192.168.1.1:1234"; got != exp {
		t.Fatalf("wrong address supplied, exp %s, got %s", exp, got)
	}

	if exp, got := BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootSingleNotifyHTTPS(t *testing.T) {
	tsNotified := false
	var body map[string]string
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/join" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		if r.URL.Path != "/notify" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		tsNotified = true
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

	n := -1
	done := func() bool {
		n++
		return n == 5
	}

	tlsConfig, err := rtls.CreateClientConfig("", "", "", true)
	if err != nil {
		t.Fatalf("failed to create TLS config: %s", err)
	}

	p := NewAddressProviderString([]string{ts.URL})
	bs := NewBootstrapper(p, tlsConfig)
	bs.Interval = time.Second

	err = bs.Boot("node1", "192.168.1.1:1234", done, 60*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}

	if tsNotified != true {
		t.Fatalf("notify target not contacted")
	}

	if got, exp := body["id"], "node1"; got != exp {
		t.Fatalf("wrong node ID supplied, exp %s, got %s", exp, got)
	}
	if got, exp := body["addr"], "192.168.1.1:1234"; got != exp {
		t.Fatalf("wrong address supplied, exp %s, got %s", exp, got)
	}

	if exp, got := BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootSingleNotifyAuth(t *testing.T) {
	tsNotified := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			t.Fatalf("request did not have Basic Auth credentials")
		}
		if username != "username1" || password != "password1" {
			t.Fatalf("bad Basic Auth credentials received (%s, %s", username, password)
		}

		if r.URL.Path == "/join" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		tsNotified = true
	}))

	n := -1
	done := func() bool {
		n++
		return n == 5
	}

	p := NewAddressProviderString([]string{ts.URL})
	bs := NewBootstrapper(p, nil)
	bs.SetBasicAuth("username1", "password1")
	bs.Interval = time.Second

	err := bs.Boot("node1", "192.168.1.1:1234", done, 60*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}

	if tsNotified != true {
		t.Fatalf("notify target not contacted")
	}
	if exp, got := BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootMultiNotify(t *testing.T) {
	ts1Join := false
	ts1Notified := false
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/join" {
			ts1Join = true
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		ts1Notified = true
	}))

	ts2Join := false
	ts2Notified := false
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/join" {
			ts2Join = true
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		ts2Notified = true
	}))

	n := -1
	done := func() bool {
		n++
		return n == 5
	}

	p := NewAddressProviderString([]string{ts1.URL, ts2.URL})
	bs := NewBootstrapper(p, nil)
	bs.Interval = time.Second

	err := bs.Boot("node1", "192.168.1.1:1234", done, 60*time.Second)
	if err != nil {
		t.Fatalf("failed to boot: %s", err)
	}

	if ts1Join != true || ts2Join != true {
		t.Fatalf("all join targets not contacted")
	}
	if ts1Notified != true || ts2Notified != true {
		t.Fatalf("all notify targets not contacted")
	}
	if exp, got := BootDone, bs.Status(); exp != got {
		t.Fatalf("wrong status, exp %s, got %s", exp, got)
	}
}
