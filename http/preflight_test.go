package http

import (
	"crypto/tls"
	"crypto/x509/pkix"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rqlite/rqlite/rtls"
)

// Test_IsServingHTTP_HTTPServerOnly tests only HTTP server running.
func Test_IsServingHTTP_HTTPServer(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer httpServer.Close()

	addr := httpServer.Listener.Addr().String()
	if !IsServingHTTP(addr) {
		t.Errorf("Expected true for HTTP server running on %s", addr)
	}
}

// Test_IsServingHTTP_HTTPSServerOnly tests only HTTPS server running.
func Test_IsServingHTTP_HTTPSServer(t *testing.T) {
	httpsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer httpsServer.Close()

	addr := httpsServer.Listener.Addr().String()
	if !IsServingHTTP(addr) {
		t.Error("Expected true for HTTPS server running")
	}
}

// Test_IsServingHTTP_NoServersRunning tests no servers running.
func Test_IsServingHTTP_NoServersRunning(t *testing.T) {
	addr := "127.0.0.1:9999" // Assume this address is not used
	if IsServingHTTP(addr) {
		t.Error("Expected false for no servers running")
	}
}

// Test_IsServingHTTP_InvalidAddress tests invalid address format.
func Test_IsServingHTTP_InvalidAddress(t *testing.T) {
	addr := "invalid-address"
	if IsServingHTTP(addr) {
		t.Error("Expected false for invalid address")
	}
}

// // Test_IsServingHTTP_Timeout is not straightforward to simulate in a unit test without external dependencies.
// // Skipping this test case.

// Test_IsServingHTTP_HTTPErrorStatusCode tests HTTP server returning error status code.
func Test_IsServingHTTP_HTTPErrorStatusCode(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer httpServer.Close()

	addr := httpServer.Listener.Addr().String()
	if !IsServingHTTP(addr) {
		t.Error("Expected true for HTTP server running, even with error status code")
	}
}

// Test_IsServingHTTP_HTTPSSuccessStatusCode tests HTTPS server running with success status code.
func Test_IsServingHTTP_HTTPSSuccessStatusCode(t *testing.T) {
	httpsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer httpsServer.Close()

	addr := httpsServer.Listener.Addr().String()
	if !IsServingHTTP(addr) {
		t.Error("Expected true for HTTPS server running with success status code")
	}
}

func Test_IsServingHTTP_OpenPort(t *testing.T) {
	// Create a TCP listener on a random port
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	if IsServingHTTP(addr) {
		t.Error("Expected false for open port")
	}
}

func Test_IsServingHTTP_OpenPortTLS(t *testing.T) {
	cert, key, err := rtls.GenerateSelfSignedCert(pkix.Name{CommonName: "rqlite"}, time.Hour, 2048)
	if err != nil {
		t.Fatalf("failed to generate self-signed cert: %s", err)
	}
	certFile := mustWriteTempFile(t, cert)
	keyFile := mustWriteTempFile(t, key)
	tlsConfig, err := rtls.CreateServerConfig(certFile, keyFile, "", false)
	if err != nil {
		t.Fatalf("failed to create TLS config: %s", err)
	}

	ln, err := tls.Listen("tcp", ":0", tlsConfig)
	if err != nil {
		t.Fatalf("failed to create TLS listener: %s", err)
	}

	addr := ln.Addr().String()
	if IsServingHTTP(addr) {
		t.Error("Expected false for open TLS port")
	}
}
