package otlp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/rqlite/rqlite/v10/internal/rtls"
)

// DefaultInterval is the default period between metric exports.
const DefaultInterval = 30 * time.Second

// Config holds the configuration for OTLP metrics reporting.
type Config struct {
	// Endpoint is the address of the OpenTelemetry Collector, in host:port
	// form. It must not include a protocol scheme.
	Endpoint string

	// Interval is the period between metric exports. It must be greater
	// than zero.
	Interval time.Duration

	// Insecure, when true, means communicate with the Collector using
	// plaintext gRPC.
	Insecure bool

	// InsecureSkipVerify, when true, means use TLS, but skip verification
	// of the Collector's certificate.
	InsecureSkipVerify bool

	// CACertFile is the path to an X.509 CA certificate used to verify
	// the Collector's certificate. If empty, the system CAs are used.
	CACertFile string

	// CertFile and KeyFile are paths to an X.509 certificate and
	// corresponding private key, for mutual TLS with the Collector.
	CertFile string
	KeyFile  string

	// NodeID is this node's ID, and is set as the service.instance.id
	// resource attribute on all exported metrics.
	NodeID string

	// Version is the rqlite version, and is set as the service.version
	// resource attribute on all exported metrics.
	Version string
}

// Validate checks the configuration for validity.
func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("endpoint not set")
	}
	if strings.Contains(c.Endpoint, "://") {
		return fmt.Errorf("endpoint %s must not include a protocol scheme", c.Endpoint)
	}
	if _, _, err := net.SplitHostPort(c.Endpoint); err != nil {
		return fmt.Errorf("endpoint %s must be in host:port form", c.Endpoint)
	}
	if c.Interval <= 0 {
		return errors.New("interval must be greater than zero")
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return errors.New("either both cert and key must be set, or neither")
	}
	if c.Insecure && (c.InsecureSkipVerify || c.CACertFile != "" || c.CertFile != "") {
		return errors.New("insecure connections cannot use TLS options")
	}
	return nil
}

// TLSConfig returns the TLS configuration to use when communicating with
// the Collector, or nil if the connection is plaintext.
func (c *Config) TLSConfig() (*tls.Config, error) {
	if c.Insecure {
		return nil, nil
	}
	return rtls.CreateClientConfig(c.CertFile, c.KeyFile, c.CACertFile, rtls.NoServerName,
		c.InsecureSkipVerify)
}
