package cdc

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Destination defines the interface that all CDC sinks must implement.
type Destination interface {
	// SendBatch delivers one batch of already-encoded CDC events.
	// The data is expected to be JSON-encoded (for now).
	SendBatch(data []byte) error

	// Close releases resources held by the destination.
	Close() error
}

// DestinationConfig holds the configuration for creating a destination.
type DestinationConfig struct {
	Endpoint        string
	TLSConfig       *tls.Config
	TransmitTimeout time.Duration
}

// StdoutDestination implements Destination by writing the batch to os.Stdout.
type StdoutDestination struct{}

// NewStdoutDestination creates a new StdoutDestination.
func NewStdoutDestination() *StdoutDestination {
	return &StdoutDestination{}
}

// SendBatch writes the batch data to stdout.
func (d *StdoutDestination) SendBatch(data []byte) error {
	_, err := fmt.Println(string(data))
	return err
}

// Close releases resources (none for stdout).
func (d *StdoutDestination) Close() error {
	return nil
}

// HTTPDestination implements Destination by POSTing batches to the configured endpoint.
type HTTPDestination struct {
	endpoint   string
	httpClient *http.Client
}

// NewHTTPDestination creates a new HTTPDestination.
func NewHTTPDestination(endpoint string, tlsConfig *tls.Config, timeout time.Duration) *HTTPDestination {
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: timeout,
	}

	return &HTTPDestination{
		endpoint:   endpoint,
		httpClient: httpClient,
	}
}

// SendBatch sends the batch data to the HTTP endpoint.
func (d *HTTPDestination) SendBatch(data []byte) error {
	req, err := http.NewRequest("POST", d.endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("HTTP request failed with status %d", resp.StatusCode)
	}

	return nil
}

// Close releases resources held by the HTTP client.
func (d *HTTPDestination) Close() error {
	if d.httpClient != nil {
		d.httpClient.CloseIdleConnections()
	}
	return nil
}

// NewDestination creates a new Destination based on the provided configuration.
func NewDestination(cfg DestinationConfig) (Destination, error) {
	if strings.EqualFold(cfg.Endpoint, "stdout") {
		return NewStdoutDestination(), nil
	}

	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint URL: %w", err)
	}

	switch u.Scheme {
	case "http", "https":
		return NewHTTPDestination(cfg.Endpoint, cfg.TLSConfig, cfg.TransmitTimeout), nil
	default:
		return nil, fmt.Errorf("cdc: unsupported scheme %q", u.Scheme)
	}
}
