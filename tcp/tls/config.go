package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// CreateClientConfig returns a TLS config for a client.
func CreateClientConfig(caCertFile string, tls1011 bool) (*tls.Config, error) {
	var minTLS = uint16(tls.VersionTLS12)
	if tls1011 {
		minTLS = tls.VersionTLS10
	}

	config := &tls.Config{
		NextProtos: []string{"h2", "http/1.1"},
		MinVersion: minTLS,
	}
	if caCertFile != "" {
		if err := SetCertPool(caCertFile, config.RootCAs); err != nil {
			return nil, err
		}
	}
	return config, nil
}

// CreateServerConfig returns a TLS config for a server.
func CreateServerConfig(certFile, keyFile, caCertFile string, tls1011 bool) (*tls.Config, error) {
	var err error

	var minTLS = uint16(tls.VersionTLS12)
	if tls1011 {
		minTLS = tls.VersionTLS10
	}

	config := &tls.Config{
		NextProtos:   []string{"h2", "http/1.1"},
		MinVersion:   minTLS,
		Certificates: make([]tls.Certificate, 1),
	}
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	if caCertFile != "" {
		if err := SetCertPool(caCertFile, config.ClientCAs); err != nil {
			return nil, err
		}
	}
	return config, nil
}

// SetCertPool sets the cert pool for the given cert file. If pool
// is nil, a new pool is created.
func SetCertPool(certFile string, pool *x509.CertPool) error {
	if pool == nil {
		pool = x509.NewCertPool()
	}
	asn1Data, err := os.ReadFile(certFile)
	if err != nil {
		return nil
	}
	ok := pool.AppendCertsFromPEM(asn1Data)
	if !ok {
		return fmt.Errorf("failed to parse root certificate(s) in %q", certFile)
	}
	return nil
}

// CreateConfig returns a TLS config from the given cert and key.
func CreateConfig(certFile, keyFile, caCertFile string, tls1011 bool) (*tls.Config, error) {
	var err error

	var minTLS = uint16(tls.VersionTLS12)
	if tls1011 {
		minTLS = tls.VersionTLS10
	}

	config := &tls.Config{
		NextProtos: []string{"h2", "http/1.1"},
		MinVersion: minTLS,
	}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	if caCertFile != "" {
		asn1Data, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, err
		}
		config.RootCAs = x509.NewCertPool()
		ok := config.RootCAs.AppendCertsFromPEM(asn1Data)
		if !ok {
			return nil, fmt.Errorf("failed to parse root certificate(s) in %q", caCertFile)
		}
	}
	return config, nil
}
