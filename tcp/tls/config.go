package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
)

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

// NewListener returns a net listener which encrypts the traffic using TLS.
func NewListener(ln net.Listener, certFile, keyFile, caCertFile string) (net.Listener, error) {
	config, err := CreateConfig(certFile, keyFile, caCertFile, false)
	if err != nil {
		return nil, err
	}

	return tls.NewListener(ln, config), nil
}

func Listen(network, laddr string, config *tls.Config) (net.Listener, error) {
	return tls.Listen("tcp", laddr, config)
}
