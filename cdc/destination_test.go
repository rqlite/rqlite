package cdc

import (
	"bytes"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestStdoutSink_Write(t *testing.T) {
	dest := NewStdoutSink()

	// Redirect stdout to capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	testData := []byte(`{"test": "data"}`)
	_, err := dest.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Restore stdout and read captured output
	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	r.Close()

	output := strings.TrimSpace(buf.String())
	expected := string(testData)
	if output != expected {
		t.Errorf("Expected output %q, got %q", expected, output)
	}
}

func TestStdoutSink_Close(t *testing.T) {
	dest := NewStdoutSink()
	err := dest.Close()
	if err != nil {
		t.Errorf("Close should not return error, got: %v", err)
	}
}

func TestStdoutSink_String(t *testing.T) {
	dest := NewStdoutSink()
	expected := "stdout"
	if got := dest.String(); got != expected {
		t.Errorf("Expected %q, got %q", expected, got)
	}
}

func TestHTTPSink_Write_Success(t *testing.T) {
	// Create test server
	var receivedData []byte
	var receivedContentType string
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		receivedData = body
		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	dest := NewHTTPSink(testSrv.URL, nil, 5*time.Second)

	testData := []byte(`{"test": "data"}`)
	_, err := dest.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if !bytes.Equal(receivedData, testData) {
		t.Errorf("Expected data %s, got %s", testData, receivedData)
	}

	if receivedContentType != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got %q", receivedContentType)
	}
}

func TestHTTPSink_Write_Accepted(t *testing.T) {
	// Test that 202 Accepted is also considered success
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer testSrv.Close()

	dest := NewHTTPSink(testSrv.URL, nil, 5*time.Second)

	_, err := dest.Write([]byte(`{"test": "data"}`))
	if err != nil {
		t.Errorf("Write should succeed with 202 status, got error: %v", err)
	}
}

func TestHTTPSink_Write_HTTPError(t *testing.T) {
	// Test HTTP error response
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer testSrv.Close()

	dest := NewHTTPSink(testSrv.URL, nil, 5*time.Second)

	_, err := dest.Write([]byte(`{"test": "data"}`))
	if err == nil {
		t.Error("Write should fail with 500 status")
	}

	expectedError := "HTTP request failed with status 500"
	if !strings.Contains(err.Error(), expectedError) {
		t.Errorf("Expected error to contain %q, got: %v", expectedError, err)
	}
}

func TestHTTPSink_Write_NetworkError(t *testing.T) {
	// Test network error (invalid URL)
	dest := NewHTTPSink("http://invalid-url-that-does-not-exist:9999", nil, 1*time.Second)

	_, err := dest.Write([]byte(`{"test": "data"}`))
	if err == nil {
		t.Error("Write should fail with network error")
	}

	if !strings.Contains(err.Error(), "error sending HTTP request") {
		t.Errorf("Expected network error message, got: %v", err)
	}
}

func TestHTTPSink_WithTLS(t *testing.T) {
	// Create HTTPS test server
	testSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	// Use insecure TLS config for testing
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	dest := NewHTTPSink(testSrv.URL, tlsConfig, 5*time.Second)

	_, err := dest.Write([]byte(`{"test": "data"}`))
	if err != nil {
		t.Errorf("Write with TLS should succeed, got error: %v", err)
	}
}

func TestHTTPSink_Close(t *testing.T) {
	dest := NewHTTPSink("http://example.com", nil, 5*time.Second)
	err := dest.Close()
	if err != nil {
		t.Errorf("Close should not return error, got: %v", err)
	}
}

func TestHTTPSink_String(t *testing.T) {
	endpoint := "https://webhook.example.com/cdc"
	dest := NewHTTPSink(endpoint, nil, 5*time.Second)
	if got := dest.String(); got != endpoint {
		t.Errorf("Expected %q, got %q", endpoint, got)
	}
}

func TestNewSink_Stdout(t *testing.T) {
	cfg := SinkConfig{
		Endpoint: "stdout",
	}

	dest, err := NewSink(cfg)
	if err != nil {
		t.Fatalf("NewSink failed: %v", err)
	}

	if _, ok := dest.(*StdoutSink); !ok {
		t.Errorf("Expected StdoutSink, got %T", dest)
	}
}

func TestNewSink_HTTP(t *testing.T) {
	cfg := SinkConfig{
		Endpoint:        "http://example.com",
		TransmitTimeout: 10 * time.Second,
	}

	dest, err := NewSink(cfg)
	if err != nil {
		t.Fatalf("NewSink failed: %v", err)
	}

	if _, ok := dest.(*HTTPSink); !ok {
		t.Errorf("Expected HTTPSink, got %T", dest)
	}
}

func TestNewSink_HTTPS(t *testing.T) {
	cfg := SinkConfig{
		Endpoint:        "https://example.com",
		TransmitTimeout: 10 * time.Second,
	}

	dest, err := NewSink(cfg)
	if err != nil {
		t.Fatalf("NewSink failed: %v", err)
	}

	if _, ok := dest.(*HTTPSink); !ok {
		t.Errorf("Expected HTTPSink, got %T", dest)
	}
}

func TestNewSink_UnsupportedScheme(t *testing.T) {
	cfg := SinkConfig{
		Endpoint: "ftp://example.com",
	}

	_, err := NewSink(cfg)
	if err == nil {
		t.Error("NewSink should fail with unsupported scheme")
	}

	expectedError := `cdc: unsupported scheme "ftp"`
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestNewSink_InvalidURL(t *testing.T) {
	cfg := SinkConfig{
		Endpoint: "://invalid-url",
	}

	_, err := NewSink(cfg)
	if err == nil {
		t.Error("NewSink should fail with invalid URL")
	}

	if !strings.Contains(err.Error(), "failed to parse endpoint URL") {
		t.Errorf("Expected URL parse error, got: %v", err)
	}
}
