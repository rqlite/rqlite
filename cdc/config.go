package cdc

import (
	"crypto/tls"
	"time"
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
	// LogOnly indicates whether the CDC service should only log events instead of sending
	// them to the configured endpoint. This is mostly useful for testing.
	LogOnly bool

	// Endpoint is the HTTP endpoint to which the CDC events are sent.
	Endpoint string

	// TLSConfig is the TLS configuration used for the HTTP client.
	TLSConfig *tls.Config

	// MaxBatchSz is the maximum number of events to send in a single batch to the endpoint.
	MaxBatchSz int

	// MaxBatchDelay is the maximum delay before sending a batch of events, regardless
	// of the number of events ready for sending. This is used to ensure that
	// we don't wait too long for a batch to fill up.
	MaxBatchDelay time.Duration

	// HighWatermarkingDisabled indicates whether high watermarking is disabled.
	// If true, the service will not write or read the high watermark from the store.
	// This is useful for testing or when high watermarking is not needed.
	HighWatermarkingDisabled bool

	// HighWatermarkInterval is the interval at which the high watermark is written to the store.
	HighWatermarkInterval time.Duration

	// TransmitTimeout is the timeout for transmitting events to the endpoint.
	// If the transmission takes longer than this, it will be retried.
	TransmitTimeout time.Duration

	// TransmitMaxRetries is the maximum number of retries for sending events to the endpoint.
	// If the transmission fails after this many retries, it will be dropped.
	TransmitMaxRetries int

	// TransmitRetryPolicy defines the retry policy to use when sending events to the endpoint.
	TransmitRetryPolicy RetryPolicy

	// TransmitMinBackoff is the initial backoff time.
	TransmitMinBackoff time.Duration

	// TransmitMaxBackoff is the maximum backoff time for retries when using exponential backoff.
	TransmitMaxBackoff time.Duration
}

// DefaultConfig returns a default configuration for the CDC service.
func DefaultConfig() *Config {
	return &Config{
		Endpoint:              "http://localhost:8080/cdc",
		MaxBatchSz:            100,
		MaxBatchDelay:         time.Second,
		HighWatermarkInterval: 10 * time.Second,
		TransmitTimeout:       5 * time.Second,
		TransmitRetryPolicy:   LinearRetryPolicy,
		TransmitMaxRetries:    5,
		TransmitMaxBackoff:    time.Minute,
		TransmitMinBackoff:    500 * time.Millisecond,
	}
}
