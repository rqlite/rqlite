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

func TestStdoutDestination_SendBatch(t *testing.T) {
	dest := NewStdoutDestination()

	// Redirect stdout to capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	testData := []byte(`{"test": "data"}`)
	err := dest.SendBatch(testData)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
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

func TestStdoutDestination_Close(t *testing.T) {
	dest := NewStdoutDestination()
	err := dest.Close()
	if err != nil {
		t.Errorf("Close should not return error, got: %v", err)
	}
}

func TestHTTPDestination_SendBatch_Success(t *testing.T) {
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

	dest := NewHTTPDestination(testSrv.URL, nil, 5*time.Second)

	testData := []byte(`{"test": "data"}`)
	err := dest.SendBatch(testData)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	if !bytes.Equal(receivedData, testData) {
		t.Errorf("Expected data %s, got %s", testData, receivedData)
	}

	if receivedContentType != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got %q", receivedContentType)
	}
}

func TestHTTPDestination_SendBatch_Accepted(t *testing.T) {
	// Test that 202 Accepted is also considered success
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer testSrv.Close()

	dest := NewHTTPDestination(testSrv.URL, nil, 5*time.Second)

	err := dest.SendBatch([]byte(`{"test": "data"}`))
	if err != nil {
		t.Errorf("SendBatch should succeed with 202 status, got error: %v", err)
	}
}

func TestHTTPDestination_SendBatch_HTTPError(t *testing.T) {
	// Test HTTP error response
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer testSrv.Close()

	dest := NewHTTPDestination(testSrv.URL, nil, 5*time.Second)

	err := dest.SendBatch([]byte(`{"test": "data"}`))
	if err == nil {
		t.Error("SendBatch should fail with 500 status")
	}

	expectedError := "HTTP request failed with status 500"
	if !strings.Contains(err.Error(), expectedError) {
		t.Errorf("Expected error to contain %q, got: %v", expectedError, err)
	}
}

func TestHTTPDestination_SendBatch_NetworkError(t *testing.T) {
	// Test network error (invalid URL)
	dest := NewHTTPDestination("http://invalid-url-that-does-not-exist:9999", nil, 1*time.Second)

	err := dest.SendBatch([]byte(`{"test": "data"}`))
	if err == nil {
		t.Error("SendBatch should fail with network error")
	}

	if !strings.Contains(err.Error(), "error sending HTTP request") {
		t.Errorf("Expected network error message, got: %v", err)
	}
}

func TestHTTPDestination_WithTLS(t *testing.T) {
	// Create HTTPS test server
	testSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	// Use insecure TLS config for testing
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	dest := NewHTTPDestination(testSrv.URL, tlsConfig, 5*time.Second)

	err := dest.SendBatch([]byte(`{"test": "data"}`))
	if err != nil {
		t.Errorf("SendBatch with TLS should succeed, got error: %v", err)
	}
}

func TestHTTPDestination_Close(t *testing.T) {
	dest := NewHTTPDestination("http://example.com", nil, 5*time.Second)
	err := dest.Close()
	if err != nil {
		t.Errorf("Close should not return error, got: %v", err)
	}
}

func TestNewDestination_Stdout(t *testing.T) {
	cfg := DestinationConfig{
		Endpoint: "stdout",
	}

	dest, err := NewDestination(cfg)
	if err != nil {
		t.Fatalf("NewDestination failed: %v", err)
	}

	if _, ok := dest.(*StdoutDestination); !ok {
		t.Errorf("Expected StdoutDestination, got %T", dest)
	}
}

func TestNewDestination_HTTP(t *testing.T) {
	cfg := DestinationConfig{
		Endpoint:        "http://example.com",
		TransmitTimeout: 10 * time.Second,
	}

	dest, err := NewDestination(cfg)
	if err != nil {
		t.Fatalf("NewDestination failed: %v", err)
	}

	if _, ok := dest.(*HTTPDestination); !ok {
		t.Errorf("Expected HTTPDestination, got %T", dest)
	}
}

func TestNewDestination_HTTPS(t *testing.T) {
	cfg := DestinationConfig{
		Endpoint:        "https://example.com",
		TransmitTimeout: 10 * time.Second,
	}

	dest, err := NewDestination(cfg)
	if err != nil {
		t.Fatalf("NewDestination failed: %v", err)
	}

	if _, ok := dest.(*HTTPDestination); !ok {
		t.Errorf("Expected HTTPDestination, got %T", dest)
	}
}

func TestNewDestination_UnsupportedScheme(t *testing.T) {
	cfg := DestinationConfig{
		Endpoint: "ftp://example.com",
	}

	_, err := NewDestination(cfg)
	if err == nil {
		t.Error("NewDestination should fail with unsupported scheme")
	}

	expectedError := `cdc: unsupported scheme "ftp"`
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestNewDestination_InvalidURL(t *testing.T) {
	cfg := DestinationConfig{
		Endpoint: "://invalid-url",
	}

	_, err := NewDestination(cfg)
	if err == nil {
		t.Error("NewDestination should fail with invalid URL")
	}

	if !strings.Contains(err.Error(), "failed to parse endpoint URL") {
		t.Errorf("Expected URL parse error, got: %v", err)
	}
}

func TestNewDestination_Kafka(t *testing.T) {
	cfg := DestinationConfig{
		Endpoint: "kafka://localhost:9092",
	}

	_, err := NewDestination(cfg)
	if err == nil {
		t.Error("NewDestination should fail with kafka scheme (not yet supported)")
	}

	expectedError := "cdc: kafka not yet supported"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}
