package cluster

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
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
	bs := NewBootstrapper(nil, 1, 1, 0, nil)
	if bs == nil {
		t.Fatalf("failed to create a simple Bootstrapper")
	}
}

func Test_BootstrapperBootSingleNotify(t *testing.T) {
	var body map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Fatalf("Client did not use POST")
		}
		w.WriteHeader(http.StatusOK)

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

	p := NewAddressProviderString([]string{ts.URL})
	bs := NewBootstrapper(p, 1, 1, 0, nil)
	if err := bs.Boot("node1", "192.168.1.1:1234"); err != nil {
		t.Fatalf("failed to boot: %s", err)
	}

	if got, exp := body["id"].(string), "node1"; got != exp {
		t.Fatalf("wrong node ID supplied, exp %s, got %s", exp, got)
	}
	if got, exp := body["addr"].(string), "192.168.1.1:1234"; got != exp {
		t.Fatalf("wrong address supplied, exp %s, got %s", exp, got)
	}
}

func Test_BootstrapperBootMultiNotify(t *testing.T) {
	ts1Notified := false
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts1Notified = true
	}))

	ts2Notified := false
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts2Notified = true
	}))

	p := NewAddressProviderString([]string{ts1.URL, ts2.URL})
	bs := NewBootstrapper(p, 1, 1, 0, nil)
	if err := bs.Boot("node1", "192.168.1.1:1234"); err != nil {
		t.Fatalf("failed to boot: %s", err)
	}

	if ts1Notified != true || ts2Notified != true {
		t.Fatalf("all notify targets not accessed")
	}
}
