package tls

import (
	gtls "crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

// CreateClientConfig creates a new tls.Config for use by a client. The certFile and keyFile
// parameters are the paths to the client's certificate and key files, which will be used to
// authenticate the client to the server. The caCertFile parameter is the path to the CA
// certificate file, which the client will use to verify any certificate presented by the
// server . If noverify is true, the client will not verify the server's certificate. If
// tls1011 is true, the client will accept TLS 1.0 or 1.1. Otherwise, it will require TLS 1.2
// or higher.
func CreateClientConfig(certFile, keyFile, caCertFile string, noverify, tls1011 bool) (*gtls.Config, error) {
	var err error

	var minTLS = uint16(gtls.VersionTLS12)
	if tls1011 {
		minTLS = gtls.VersionTLS10
	}

	config := &gtls.Config{
		InsecureSkipVerify: noverify,
		NextProtos:         []string{"h2", "http/1.1"},
		MinVersion:         minTLS,
	}
	config.Certificates = make([]gtls.Certificate, 1)
	config.Certificates[0], err = gtls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	if caCertFile != "" {
		asn1Data, err := ioutil.ReadFile(caCertFile)
		if err != nil {
			return nil, err
		}
		config.RootCAs = x509.NewCertPool()
		ok := config.RootCAs.AppendCertsFromPEM(asn1Data)
		if !ok {
			return nil, fmt.Errorf("failed to load CA certificate(s) for server verification in %q", caCertFile)
		}
		config.ClientAuth = gtls.RequireAndVerifyClientCert
	}
	return config, nil
}

// CreateServerConfig creates a new tls.Config for use by a server. The certFile and keyFile
// parameters are the paths to the server's certificate and key files, which will be used to
// authenticate the server to the client. The caCertFile parameter is the path to the CA
// certificate file, which the server will use to verify any certificate presented by the
// client. If noverify is true, the server will not verify the client's certificate. If
// tls1011 is true, the server will accept TLS 1.0 or 1.1. Otherwise, it will require TLS 1.2
// or higher.
func CreateServerConfig(certFile, keyFile, caCertFile string, noverify, tls1011 bool) (*gtls.Config, error) {
	var err error

	var minTLS = uint16(gtls.VersionTLS12)
	if tls1011 {
		minTLS = gtls.VersionTLS10
	}

	config := &gtls.Config{
		InsecureSkipVerify: noverify,
		NextProtos:         []string{"h2", "http/1.1"},
		MinVersion:         minTLS,
	}
	config.Certificates = make([]gtls.Certificate, 1)
	config.Certificates[0], err = gtls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	if caCertFile != "" {
		asn1Data, err := ioutil.ReadFile(caCertFile)
		if err != nil {
			return nil, err
		}
		config.ClientCAs = x509.NewCertPool()
		ok := config.ClientCAs.AppendCertsFromPEM(asn1Data)
		if !ok {
			return nil, fmt.Errorf("failed to load CA certificate(s) for client verification in %q", caCertFile)
		}
		config.ClientAuth = gtls.RequireAndVerifyClientCert
	}
	return config, nil
}
