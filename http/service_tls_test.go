package http

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	rtls "github.com/rqlite/rqlite/tls"
	"golang.org/x/net/http2"
)

func Test_TLSServiceInsecure(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)

	cert, key, err := rtls.GenerateSelfSignedCert(pkix.Name{CommonName: "rqlite"}, time.Hour, 2048)
	if err != nil {
		t.Fatalf("failed to generate self-signed cert: %s", err)
	}
	s.CertFile = mustWriteTempFile(cert)
	defer os.Remove(s.CertFile)
	s.KeyFile = mustWriteTempFile(key)
	defer os.Remove(s.KeyFile)

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

func Test_TLSServiceSecure(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)

	cert, key, err := rtls.GenerateSelfSignedCertIPSAN(pkix.Name{CommonName: "rqlite.io"}, time.Hour, 2048, net.ParseIP("127.0.0.1"))
	if err != nil {
		t.Fatalf("failed to generate self-signed cert: %s", err)
	}
	s.CertFile = mustWriteTempFile(cert)
	defer os.Remove(s.CertFile)
	s.KeyFile = mustWriteTempFile(key)
	defer os.Remove(s.KeyFile)

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

	// Create a TLS Config which verfies server cert, and trusts the CA cert.
	tlsConfig := &tls.Config{InsecureSkipVerify: false}
	tlsConfig.RootCAs = x509.NewCertPool()
	ok := tlsConfig.RootCAs.AppendCertsFromPEM(cert)
	if !ok {
		t.Fatalf("failed to parse CA certificate(s) for client verification in %q", cert)
	}

	// Test connecting with an HTTP1.1 client.
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: tlsConfig,
	}}
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
			TLSClientConfig: tlsConfig,
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

// mustWriteTempFile writes the given bytes to a temporary file, and returns the
// path to the file. If there is an error, it panics.
func mustWriteTempFile(b []byte) string {
	f, err := ioutil.TempFile("", "rqlite-test")
	if err != nil {
		panic("failed to create temp file")
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		panic("failed to write to temp file")
	}
	return f.Name()
}
