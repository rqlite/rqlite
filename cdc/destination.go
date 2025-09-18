package cdc

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	httpurl "github.com/rqlite/rqlite/v9/http/url"
)

// Sink defines the interface that all CDC sinks must implement.
type Sink interface {
	io.WriteCloser
	fmt.Stringer
}

// SinkConfig holds the configuration for creating a sink.
type SinkConfig struct {
	Endpoint        string
	TLSConfig       *tls.Config
	TransmitTimeout time.Duration
}

// StdoutSink implements Sink by writing to os.Stdout.
type StdoutSink struct{}

// NewStdoutSink creates a new StdoutSink.
func NewStdoutSink() *StdoutSink {
	return &StdoutSink{}
}

// Write writes the data to stdout.
func (d *StdoutSink) Write(p []byte) (n int, err error) {
	n, err = fmt.Println(string(p))
	if err != nil {
		return 0, err
	}
	// fmt.Println adds a newline, but we return the original length
	return len(p), nil
}

// Close releases resources (none for stdout).
func (d *StdoutSink) Close() error {
	return nil
}

func (d *StdoutSink) String() string {
	return "stdout"
}

// HTTPSink implements Sink by POSTing batches to the configured endpoint.
type HTTPSink struct {
	endpoint   string
	httpClient *http.Client
}

// NewHTTPSink creates a new HTTPSink.
func NewHTTPSink(endpoint string, tlsConfig *tls.Config, timeout time.Duration) *HTTPSink {
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: timeout,
	}

	return &HTTPSink{
		endpoint:   endpoint,
		httpClient: httpClient,
	}
}

// Write writes the data to the HTTP endpoint.
func (d *HTTPSink) Write(p []byte) (n int, err error) {
	req, err := http.NewRequest("POST", d.endpoint, bytes.NewReader(p))
	if err != nil {
		return 0, fmt.Errorf("error creating HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error sending HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return 0, fmt.Errorf("HTTP request failed with status %d", resp.StatusCode)
	}
	return len(p), nil
}

// Close releases resources held by the HTTP client.
func (d *HTTPSink) Close() error {
	if d.httpClient != nil {
		d.httpClient.CloseIdleConnections()
	}
	return nil
}

func (d *HTTPSink) String() string {
	return httpurl.RemoveBasicAuth(d.endpoint)
}

// NewSink creates a new Sink based on the provided configuration.
func NewSink(cfg SinkConfig) (Sink, error) {
	if strings.EqualFold(cfg.Endpoint, "stdout") {
		return NewStdoutSink(), nil
	}

	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint URL: %w", err)
	}

	switch u.Scheme {
	case "http", "https":
		return NewHTTPSink(cfg.Endpoint, cfg.TLSConfig, cfg.TransmitTimeout), nil
	default:
		return nil, fmt.Errorf("cdc: unsupported scheme %q", u.Scheme)
	}
}
