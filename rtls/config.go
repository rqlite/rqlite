// rtls is for creating TLS configurations for use by servers and clients.

package rtls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

const (
	NoCACert     = ""
	NoServerName = ""
)

// MTLSState indicates whether mutual TLS is enabled or disabled.
type MTLSState tls.ClientAuthType

const (
	MTLSStateDisabled MTLSState = MTLSState(tls.NoClientCert)
	MTLSStateEnabled  MTLSState = MTLSState(tls.RequireAndVerifyClientCert)
)

// CreateClientConfig creates a new tls.Config for use by a client. The certFile and keyFile
// parameters are the paths to the client's certificate and key files, which will be used to
// authenticate the client to the server if mutual TLS is active. The caCertFile parameter
// is the path to the CA certificate file, which the client will use to verify any certificate
// presented by the server. serverName can also be set, informing the client which hostname
// should appear in the returned certificate. If noverify is true, the client will not verify
// the server's certificate.
func CreateClientConfig(certFile, keyFile, caCertFile, serverName string, noverify bool) (*tls.Config, error) {
	var err error

	config := createBaseTLSConfig(serverName, noverify)
	if certFile != "" && keyFile != "" {
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
	}
	if caCertFile != "" {
		asn1Data, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, err
		}
		config.RootCAs = x509.NewCertPool()
		ok := config.RootCAs.AppendCertsFromPEM(asn1Data)
		if !ok {
			return nil, fmt.Errorf("failed to load CA certificate(s) for server verification in %q", caCertFile)
		}
	}
	return config, nil
}

// CreateServerConfig creates a new tls.Config for use by a server. The certFile and keyFile
// parameters are the paths to the server's certificate and key files, which will be used to
// authenticate the server to the client. The caCertFile parameter is the path to the CA
// certificate file, which the server will use to verify any certificate presented by the
// client. If mtls is MTLSStateEnabled, the server will require the client to present a
// valid certificate.
func CreateServerConfig(certFile, keyFile, caCertFile string, mtls MTLSState) (*tls.Config, error) {
	var err error

	config := createBaseTLSConfig(NoServerName, false)
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
		config.ClientCAs = x509.NewCertPool()
		ok := config.ClientCAs.AppendCertsFromPEM(asn1Data)
		if !ok {
			return nil, fmt.Errorf("failed to load CA certificate(s) for client verification in %q", caCertFile)
		}
	}
	config.ClientAuth = tls.ClientAuthType(mtls)
	return config, nil
}

// CreateServerConfigWithFunc creates a new tls.Config for use by a server. The certFunc
// parameter is a function that returns the server's certificate and key. The caCertFile
// parameter is the path to the CA certificate file, which the server will use to verify
// any certificate presented by the client. If mtls is MTLSStateEnabled, the server will
// require the client to present a valid certificate.
func CreateServerConfigWithFunc(certFunc func() (*tls.Certificate, error), caCertFile string, mtls MTLSState) (*tls.Config, error) {
	config := createBaseTLSConfig(NoServerName, false)
	config.GetCertificate = func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		return certFunc()
	}
	if caCertFile != "" {
		asn1Data, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, err
		}
		config.ClientCAs = x509.NewCertPool()
		ok := config.ClientCAs.AppendCertsFromPEM(asn1Data)
		if !ok {
			return nil, fmt.Errorf("failed to load CA certificate(s) for client verification in %q", caCertFile)
		}
	}
	config.ClientAuth = tls.ClientAuthType(mtls)
	return config, nil
}

func createBaseTLSConfig(serverName string, noverify bool) *tls.Config {
	return &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: noverify,
		NextProtos:         []string{"h2", "http/1.1"},
		MinVersion:         uint16(tls.VersionTLS12),
	}
}
