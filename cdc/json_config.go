package cdc

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// JSONConfig represents the JSON configuration structure for CDC.
type JSONConfig struct {
	Endpoint                 string         `json:"endpoint,omitempty"`
	LogOnly                  bool           `json:"log_only,omitempty"`
	MaxBatchSize             int            `json:"max_batch_size,omitempty"`
	MaxBatchDelay            string         `json:"max_batch_delay,omitempty"`
	HighWatermarkingDisabled bool           `json:"high_watermarking_disabled,omitempty"`
	HighWatermarkInterval    string         `json:"high_watermark_interval,omitempty"`
	TransmitTimeout          string         `json:"transmit_timeout,omitempty"`
	TransmitMaxRetries       int            `json:"transmit_max_retries,omitempty"`
	TransmitRetryPolicy      string         `json:"transmit_retry_policy,omitempty"`
	TransmitMinBackoff       string         `json:"transmit_min_backoff,omitempty"`
	TransmitMaxBackoff       string         `json:"transmit_max_backoff,omitempty"`
	TLS                      *JSONTLSConfig `json:"tls,omitempty"`
}

// JSONTLSConfig represents the TLS configuration in JSON.
type JSONTLSConfig struct {
	ClientCert string `json:"client_cert,omitempty"`
	ClientKey  string `json:"client_key,omitempty"`
	CACert     string `json:"ca_cert,omitempty"`
	SkipVerify bool   `json:"skip_verify,omitempty"`
}

// ReadConfigFile reads the CDC config file and returns the data. It also expands
// any environment variables in the config file.
func ReadConfigFile(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data = []byte(os.ExpandEnv(string(data)))
	return data, nil
}

// ParseJSONConfig parses a JSON configuration file and returns a CDC Config.
func ParseJSONConfig(data []byte) (*Config, error) {
	var jsonCfg JSONConfig
	if err := json.Unmarshal(data, &jsonCfg); err != nil {
		return nil, fmt.Errorf("failed to parse JSON config: %w", err)
	}

	// Start with default configuration
	cfg := DefaultConfig()

	// Override with JSON values where provided
	if jsonCfg.Endpoint != "" {
		cfg.Endpoint = jsonCfg.Endpoint
	}

	cfg.LogOnly = jsonCfg.LogOnly
	cfg.HighWatermarkingDisabled = jsonCfg.HighWatermarkingDisabled

	if jsonCfg.MaxBatchSize > 0 {
		cfg.MaxBatchSz = jsonCfg.MaxBatchSize
	}

	if jsonCfg.TransmitMaxRetries > 0 {
		cfg.TransmitMaxRetries = jsonCfg.TransmitMaxRetries
	}

	// Parse duration fields
	if jsonCfg.MaxBatchDelay != "" {
		d, err := time.ParseDuration(jsonCfg.MaxBatchDelay)
		if err != nil {
			return nil, fmt.Errorf("invalid max_batch_delay: %w", err)
		}
		cfg.MaxBatchDelay = d
	}

	if jsonCfg.HighWatermarkInterval != "" {
		d, err := time.ParseDuration(jsonCfg.HighWatermarkInterval)
		if err != nil {
			return nil, fmt.Errorf("invalid high_watermark_interval: %w", err)
		}
		cfg.HighWatermarkInterval = d
	}

	if jsonCfg.TransmitTimeout != "" {
		d, err := time.ParseDuration(jsonCfg.TransmitTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid transmit_timeout: %w", err)
		}
		cfg.TransmitTimeout = d
	}

	if jsonCfg.TransmitMinBackoff != "" {
		d, err := time.ParseDuration(jsonCfg.TransmitMinBackoff)
		if err != nil {
			return nil, fmt.Errorf("invalid transmit_min_backoff: %w", err)
		}
		cfg.TransmitMinBackoff = d
	}

	if jsonCfg.TransmitMaxBackoff != "" {
		d, err := time.ParseDuration(jsonCfg.TransmitMaxBackoff)
		if err != nil {
			return nil, fmt.Errorf("invalid transmit_max_backoff: %w", err)
		}
		cfg.TransmitMaxBackoff = d
	}

	// Parse retry policy
	if jsonCfg.TransmitRetryPolicy != "" {
		policy, err := parseRetryPolicy(jsonCfg.TransmitRetryPolicy)
		if err != nil {
			return nil, fmt.Errorf("invalid transmit_retry_policy: %w", err)
		}
		cfg.TransmitRetryPolicy = policy
	}

	// Parse TLS config
	if jsonCfg.TLS != nil {
		tlsConfig, err := parseTLSConfig(jsonCfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("invalid TLS configuration: %w", err)
		}
		cfg.TLSConfig = tlsConfig
	}

	return cfg, nil
}

// parseRetryPolicy parses a retry policy string and returns the corresponding constant.
func parseRetryPolicy(policy string) (RetryPolicy, error) {
	switch strings.ToLower(policy) {
	case "linear":
		return LinearRetryPolicy, nil
	case "exponential":
		return ExponentialRetryPolicy, nil
	default:
		return LinearRetryPolicy, fmt.Errorf("unknown retry policy: %s", policy)
	}
}

// parseTLSConfig parses a JSON TLS configuration and returns a *tls.Config.
func parseTLSConfig(jsonTLS *JSONTLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: jsonTLS.SkipVerify,
	}

	// Load client certificate if provided
	if jsonTLS.ClientCert != "" && jsonTLS.ClientKey != "" {
		cert, err := tls.LoadX509KeyPair(jsonTLS.ClientCert, jsonTLS.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate if provided
	if jsonTLS.CACert != "" {
		caCert, err := os.ReadFile(jsonTLS.CACert)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}
