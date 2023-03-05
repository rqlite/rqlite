package http

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"testing"

	"github.com/rqlite/rqlite/testdata/x509"
	"golang.org/x/net/http2"
)

func Test_TLSServce(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	var s *Service
	tempDir := t.TempDir()

	s = New("127.0.0.1:0", m, c, nil)
	s.CertFile = x509.CertFile(tempDir)
	s.KeyFile = x509.KeyFile(tempDir)
	s.BuildInfo = map[string]interface{}{
		"version": "the version",
	}
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	if !s.HTTPS() {
		t.Fatalf("expected service to report HTTPS")
	}
	defer s.Close()

	url := fmt.Sprintf("https://%s", s.Addr().String())

	// Test connecting with an HTTP client.
	tn := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tn}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("failed to make HTTP request: %s", err)
	}

	if v := resp.Header.Get("X-RQLITE-VERSION"); v != "the version" {
		t.Fatalf("incorrect build version present in HTTP response header, got: %s", v)
	}

	// Test connecting with an HTTP/2 client.
	client = &http.Client{
		Transport: &http2.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err = client.Get(url)
	if err != nil {
		t.Fatalf("failed to make HTTP/2 request: %s", err)
	}

	if v := resp.Header.Get("X-RQLITE-VERSION"); v != "the version" {
		t.Fatalf("incorrect build version present in HTTP/2 response header, got: %s", v)
	}
}
