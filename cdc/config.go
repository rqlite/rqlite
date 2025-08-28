package cdc

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/rqlite/rqlite/v8/internal/rtls"
)

const (
	// LinearRetryPolicy indicates that the retry delay is constant.
	LinearRetryPolicy = iota

	// ExponentialRetryPolicy indicates that the retry delay increases exponentially with each retry.
	ExponentialRetryPolicy
)

type RetryPolicy int

// Config holds the configuration for the CDC service.
type Config struct {
	// ServiceID is an optional field. If set, it will be part of the event sent to the downstream
	// CDC consumer. This allows CDC consumers to receive events from multiple rqlite systems
	// and differentiate between events sent by those systems.
	ServiceID string `json:"service_id,omitempty"`

	// LogOnly indicates whether the CDC service should only log events instead of sending
	// them to the configured endpoint. This is mostly useful for testing.
	LogOnly bool `json:"log_only"`

	// Endpoint is the HTTP endpoint to which the CDC events are sent.
	Endpoint string `json:"endpoint"`

	// RowIDsOnly indicates whether only the row IDs should be sent in the CDC events.
	RowIDsOnly bool `json:"row_ids_only,omitempty"`

	// TLS configuration fields for JSON serialization and user configuration.
	// These follow the same pattern used elsewhere in rqlite.

	// CACertFile is the path to the CA certificate file for TLS verification.
	CACertFile string `json:"ca_cert_file,omitempty"`

	// CertFile is the path to the client certificate file for mutual TLS.
	CertFile string `json:"cert_file,omitempty"`

	// KeyFile is the path to the client private key file for mutual TLS.
	KeyFile string `json:"key_file,omitempty"`

	// InsecureSkipVerify controls whether the client verifies the server's certificate.
	// If true, TLS accepts any certificate presented by the server.
	InsecureSkipVerify bool `json:"insecure_skip_verify,omitempty"`

	// ServerName is used to verify the hostname on the returned certificates.
	// If empty, the hostname used for the connection is used.
	ServerName string `json:"server_name,omitempty"`

	// MaxBatchSz is the maximum number of events to send in a single batch to the endpoint.
	MaxBatchSz int `json:"max_batch_size"`

	// MaxBatchDelay is the maximum delay before sending a batch of events, regardless
	// of the number of events ready for sending. This is used to ensure that
	// we don't wait too long for a batch to fill up.
	MaxBatchDelay time.Duration `json:"max_batch_delay"`

	// HighWatermarkInterval is the interval at which the high watermark is written to the store.
	HighWatermarkInterval time.Duration `json:"high_watermark_interval"`

	// TransmitTimeout is the timeout for transmitting events to the endpoint.
	// If the transmission takes longer than this, it will be retried.
	TransmitTimeout time.Duration `json:"transmit_timeout"`

	// TransmitMaxRetries is the maximum number of retries for sending events to the endpoint.
	// If the transmission fails after this many retries, it will be dropped.
	TransmitMaxRetries int `json:"transmit_max_retries"`

	// TransmitRetryPolicy defines the retry policy to use when sending events to the endpoint.
	TransmitRetryPolicy RetryPolicy `json:"transmit_retry_policy"`

	// TransmitMinBackoff is the initial backoff time.
	TransmitMinBackoff time.Duration `json:"transmit_min_backoff"`

	// TransmitMaxBackoff is the maximum backoff time for retries when using exponential backoff.
	TransmitMaxBackoff time.Duration `json:"transmit_max_backoff"`
}

// DefaultConfig returns a default configuration for the CDC service.
func DefaultConfig() *Config {
	return &Config{
		Endpoint:              "http://localhost:8080/cdc",
		MaxBatchSz:            100,
		MaxBatchDelay:         time.Second,
		HighWatermarkInterval: 5 * time.Second,
		TransmitTimeout:       5 * time.Second,
		TransmitRetryPolicy:   LinearRetryPolicy,
		TransmitMaxRetries:    5,
		TransmitMaxBackoff:    time.Minute,
		TransmitMinBackoff:    time.Second,
	}
}

// TLSConfig creates a *tls.Config from the individual TLS configuration fields.
// This uses the same TLS utilities as other parts of rqlite.
func (c *Config) TLSConfig() (*tls.Config, error) {
	// If no TLS fields are set, return nil (no TLS)
	if c.CACertFile == "" && c.CertFile == "" && c.KeyFile == "" && !c.InsecureSkipVerify && c.ServerName == "" {
		return nil, nil
	}

	// Use the existing rtls package to create the TLS config
	return rtls.CreateClientConfig(c.CertFile, c.KeyFile, c.CACertFile, c.ServerName, c.InsecureSkipVerify)
}

// NewConfig creates a new Config from a string. If the string can be parsed
// as a URL, it creates a default config with the endpoint set to the URL.
// Otherwise, it treats the string as a file path and attempts to read and
// parse a JSON configuration file.
func NewConfig(s string) (*Config, error) {
	// Try to parse as URL first
	if _, err := url.Parse(s); err == nil && (len(s) > 0 && (s[:4] == "http" || s[:5] == "https")) {
		// Valid URL, create default config with this endpoint
		config := DefaultConfig()
		config.Endpoint = s
		return config, nil
	}

	// Not a URL, treat as file path
	data, err := os.ReadFile(s)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", s, err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %q: %w", s, err)
	}

	return &config, nil
}
