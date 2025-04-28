package http

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/rtls"
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
	s.CertFile = mustWriteTempFile(t, cert)
	s.KeyFile = mustWriteTempFile(t, key)

	s.BuildInfo = map[string]any{
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
	if resp.TLS.PeerCertificates[0].Subject.CommonName != "rqlite" {
		t.Fatalf("incorrect common name in server certificate, got: %s", resp.TLS.PeerCertificates[0].Subject.CommonName)
	}

	// Check cert reloading by changing the cert and key files, waiting, creating a new
	// client, and making a new request.
	cert2, key2, err := rtls.GenerateSelfSignedCert(pkix.Name{CommonName: "rqlite2"}, time.Hour, 2048)
	if err != nil {
		t.Fatalf("failed to generate self-signed cert: %s", err)
	}
	cert2Path := mustWriteTempFile(t, cert2)
	key2Path := mustWriteTempFile(t, key2)
	mustRename(key2Path, s.KeyFile)
	mustRename(cert2Path, s.CertFile)
	time.Sleep(2 * time.Second) // Wait for the cert to be reloaded.

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
	if resp.TLS.PeerCertificates[0].Subject.CommonName != "rqlite2" {
		t.Fatalf("incorrect common name in server certificate after reload, got: %s", resp.TLS.PeerCertificates[0].Subject.CommonName)
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
	s.CertFile = mustWriteTempFile(t, cert)
	s.KeyFile = mustWriteTempFile(t, key)

	s.BuildInfo = map[string]any{
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

	// Create a TLS Config which verifies server cert, and trusts the CA cert.
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

func Test_TLSServiceSecureMutual(t *testing.T) {
	// Generate a CA cert and key.
	caCertPEM, caKeyPEM, err := rtls.GenerateCACert(pkix.Name{CommonName: "ca.rqlite.io"}, time.Hour, 2048)
	if err != nil {
		t.Fatalf("failed to generate CA cert: %s", err)
	}

	caCert, _ := pem.Decode(caCertPEM)
	if caCert == nil {
		t.Fatal("failed to decode certificate")
	}

	caKey, _ := pem.Decode(caKeyPEM)
	if caKey == nil {
		t.Fatal("failed to decode key")
	}

	// parse the certificate and private key
	parsedCACert, err := x509.ParseCertificate(caCert.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	parsedCAKey, err := x509.ParsePKCS1PrivateKey(caKey.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	// Create a cert signed by the CA for the server
	certServer, keyServer, err := rtls.GenerateCertIPSAN(pkix.Name{CommonName: "server.rqlite.io"}, time.Hour, 2048, parsedCACert, parsedCAKey, net.ParseIP("127.0.0.1"))
	if err != nil {
		t.Fatalf("failed to generate server cert: %s", err)
	}

	// Create a cert signed by the CA for the client
	certClient, keyClient, err := rtls.GenerateCertIPSAN(pkix.Name{CommonName: "client.rqlite.io"}, time.Hour, 2048, parsedCACert, parsedCAKey, net.ParseIP("127.0.0.1"))
	if err != nil {
		t.Fatalf("failed to generate client cert: %s", err)
	}

	// Create and start the HTTP service.
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, nil)
	s.CertFile = mustWriteTempFile(t, certServer)
	s.KeyFile = mustWriteTempFile(t, keyServer)
	s.CACertFile = mustWriteTempFile(t, caCertPEM) // Enables client verification by HTTP server
	s.BuildInfo = map[string]any{
		"version": "the version",
	}
	s.ClientVerify = true
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()

	url := fmt.Sprintf("https://%s", s.Addr().String())

	// Create a TLS Config which will require verification of the server cert, and trusts the CA cert.
	tlsConfig := &tls.Config{InsecureSkipVerify: false}
	tlsConfig.RootCAs = x509.NewCertPool()
	ok := tlsConfig.RootCAs.AppendCertsFromPEM(caCertPEM)
	if !ok {
		t.Fatalf("failed to parse CA certificate(s) for client verification in %q", caCertPEM)
	}

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: tlsConfig,
	}}

	_, err = client.Get(url)
	if err == nil {
		t.Fatalf("made successful HTTP request by untrusted client")
	}

	// Now set the client cert, which as also been signed by the CA. This should
	// mean the HTTP server trusts the client.
	tlsConfig.Certificates = make([]tls.Certificate, 1)
	tlsConfig.Certificates[0], err = tls.X509KeyPair(certClient, keyClient)
	if err != nil {
		t.Fatalf("failed to set X509 key pair %s", err)
	}
	client = &http.Client{Transport: &http.Transport{
		TLSClientConfig: tlsConfig,
	}}

	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("trusted client failed to make HTTP request: %s", err)
	}

	if v := resp.Header.Get("X-RQLITE-VERSION"); v != "the version" {
		t.Fatalf("incorrect build version present in HTTP response header, got: %s", v)
	}
}

// mustWriteTempFile writes the given bytes to a temporary file, and returns the
// path to the file. If there is an error, it panics. The file will be automatically
// deleted when the test ends.
func mustWriteTempFile(t *testing.T, b []byte) string {
	f, err := os.CreateTemp(t.TempDir(), "rqlite-test")
	if err != nil {
		panic("failed to create temp file")
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		panic("failed to write to temp file")
	}
	return f.Name()
}

func mustRename(new, old string) {
	if err := os.Rename(new, old); err != nil {
		panic(fmt.Sprintf("failed to rename %s to %s: %s", new, old, err))
	}
}
