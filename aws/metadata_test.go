package aws

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func Test_NewMetadataClient(t *testing.T) {
	t.Parallel()

	c := NewMetadataClient()
	if c == nil {
		t.Fatalf("failed to create new Metadata client")
	}
}

func Test_MetadataClient_LocalIPv4(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Fatalf("Client did not use GET")
		}
		if r.URL.String() != "/latest/meta-data/local-ipv4" {
			t.Fatalf("Request URL is wrong, got: %s", r.URL.String())
		}
		fmt.Fprint(w, "172.31.34.179")
	}))
	defer ts.Close()

	c := NewMetadataClient()
	c.URL = ts.URL
	addr, err := c.LocalIPv4()
	if err != nil {
		t.Fatalf("failed to get local IPv4 address: %s", err.Error())
	}
	if addr != "172.31.34.179" {
		t.Fatalf("got incorrect local IPv4 address: %s", addr)
	}
}

func Test_MetadataClient_LocalIPv4Fail(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()

	c := NewMetadataClient()
	c.URL = ts.URL
	_, err := c.LocalIPv4()
	if err == nil {
		t.Fatalf("failed to get error when server returned 400")
	}
}

func Test_MetadataClient_PublicIPv4(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Fatalf("Client did not use GET")
		}
		if r.URL.String() != "/latest/meta-data/public-ipv4" {
			t.Fatalf("Request URL is wrong, got: %s", r.URL.String())
		}
		fmt.Fprint(w, "52.38.41.98")
	}))
	defer ts.Close()

	c := NewMetadataClient()
	c.URL = ts.URL
	addr, err := c.PublicIPv4()
	if err != nil {
		t.Fatalf("failed to get local IPv4 address: %s", err.Error())
	}
	if addr != "52.38.41.98" {
		t.Fatalf("got incorrect local IPv4 address: %s", addr)
	}
}

func Test_MetadataClient_PublicIPv4Fail(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()

	c := NewMetadataClient()
	c.URL = ts.URL
	_, err := c.PublicIPv4()
	if err == nil {
		t.Fatalf("failed to get error when server returned 400")
	}
}
