package rtls

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"os"
	"testing"
	"time"
)

func Test_CreateServerConfig(t *testing.T) {
	// generate a cert and key pair, and write both to a temporary file
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	// create a server config with no client verification
	config, err := CreateServerConfig(certFile, keyFile, NoCACert, MTLSStateDisabled)
	if err != nil {
		t.Fatalf("failed to create server config: %v", err)
	}
	if config.ClientAuth != tls.NoClientCert {
		t.Fatalf("expected ClientAuth to be NoClientCert, got %v", config.ClientAuth)
	}

	// Check that the certificate is loaded correctly
	if len(config.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(config.Certificates))
	}
	// parse the certificate in the tls config
	parsedCert, err := x509.ParseCertificate(config.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}
	if parsedCert.Subject.CommonName != "rqlite" {
		t.Fatalf("expected certificate subject to be 'rqlite', got %s", parsedCert.Subject.CommonName)
	}

	// Check that no root CA is loaded
	if config.RootCAs != nil {
		t.Fatalf("expected no root CA, got %v", config.RootCAs)
	}

	// create a server config with client verification
	config, err = CreateServerConfig(certFile, keyFile, NoCACert, MTLSStateEnabled)
	if err != nil {
		t.Fatalf("failed to create server config: %v", err)
	}
	if config.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected ClientAuth to be RequireAndVerifyClientCert, got %v", config.ClientAuth)
	}
}

func Test_CreateServerConfigWithFunc(t *testing.T) {
	// generate a cert and key pair, and write both to a temporary file
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	config, err := CreateServerConfigWithFunc(mustCreateGetCertFunc(t, certFile, keyFile), NoCACert, MTLSStateDisabled)
	if err != nil {
		t.Fatalf("failed to create server config: %v", err)
	}

	cert, err := config.GetCertificate(nil)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}
	if cert.Leaf.Subject.CommonName != "rqlite" {
		t.Fatalf("expected certificate subject to be 'rqlite', got %s", cert.Leaf.Subject.CommonName)
	}
}

func Test_CreateServerConfig_CA(t *testing.T) {
	// generate a cert and key pair, and write both to a temporary file
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	// generate CA cert
	caCertPEM, _, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate CA cert: %v", err)
	}
	caCertFile := mustWriteTempFile(t, caCertPEM)

	config, err := CreateServerConfig(certFile, keyFile, caCertFile, MTLSStateDisabled)
	if err != nil {
		t.Fatalf("failed to create server config: %v", err)
	}
	// Check that CA cert has been loaded
	if config.ClientCAs == nil {
		t.Fatalf("expected root CA, got nil")
	}

	configF, err := CreateServerConfigWithFunc(nil, caCertFile, MTLSStateDisabled)
	if err != nil {
		t.Fatalf("failed to create server config: %v", err)
	}
	// Check that CA cert has been loaded
	if configF.ClientCAs == nil {
		t.Fatalf("expected root CA, got nil")
	}
}

func Test_CreateClientConfig(t *testing.T) {
	// generate a cert and key pair, and write both to a temporary file
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	// create a client config with no server verification
	config, err := CreateClientConfig(certFile, keyFile, NoCACert, NoServerName, true)
	if err != nil {
		t.Fatalf("failed to create client config: %v", err)
	}
	if !config.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify to be true, got false")
	}

	// Check that the certificate is loaded correctly
	if len(config.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(config.Certificates))
	}
	// parse the certificate in the tls config
	parsedCert, err := x509.ParseCertificate(config.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}
	if parsedCert.Subject.CommonName != "rqlite" {
		t.Fatalf("expected certificate subject to be 'rqlite', got %s", parsedCert.Subject.CommonName)
	}

	// Check that no root CA is loaded
	if config.RootCAs != nil {
		t.Fatalf("expected no root CAs, got non-nil root CAs")
	}

	// create a client config with server verification
	config, err = CreateClientConfig(certFile, keyFile, NoCACert, NoServerName, false)
	if err != nil {
		t.Fatalf("failed to create client config: %v", err)
	}
	if config.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify to be false, got true")
	}

	// create a client config with Server Name
	config, err = CreateClientConfig(certFile, keyFile, NoCACert, "expected", false)
	if err != nil {
		t.Fatalf("failed to create client config: %v", err)
	}
	if config.ServerName != "expected" {
		t.Fatalf("expected ServerName to be 'expected', got %s", config.ServerName)
	}
}

func Test_CreateClientConfigWithFunc(t *testing.T) {
	// generate a cert and key pair, and write both to a temporary file
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	config, err := CreateClientConfigWithFunc(mustCreateGetCertFunc(t, certFile, keyFile), NoCACert, NoServerName, true)
	if err != nil {
		t.Fatalf("failed to create server config: %v", err)
	}

	cert, err := config.GetClientCertificate(nil)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}
	if cert.Leaf.Subject.CommonName != "rqlite" {
		t.Fatalf("expected certificate subject to be 'rqlite', got %s", cert.Leaf.Subject.CommonName)
	}
}

func Test_CreateClientConfig_CA(t *testing.T) {
	// generate a cert and key pair, and write both to a temporary file
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	// generate CA cert
	caCertPEM, _, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate CA cert: %v", err)
	}
	caCertFile := mustWriteTempFile(t, caCertPEM)

	// create a client config
	config, err := CreateClientConfig(certFile, keyFile, caCertFile, NoServerName, true)
	if err != nil {
		t.Fatalf("failed to create client config: %v", err)
	}

	// Check that root CA is loaded
	if config.RootCAs == nil {
		t.Fatalf("expected root CA, got nil")
	}

	configF, err := CreateClientConfigWithFunc(nil, caCertFile, NoServerName, true)
	if err != nil {
		t.Fatalf("failed to create client config: %v", err)
	}

	// Check that root CA is loaded
	if configF.RootCAs == nil {
		t.Fatalf("expected root CA, got nil")
	}
}

// mustWriteTempFile writes the given bytes to a temporary file, and returns the
// path to the file. If there is an error, it panics. The file will be automatically
// deleted when the test ends.
func mustWriteTempFile(t *testing.T, b []byte) string {
	f, err := os.CreateTemp(t.TempDir(), "rqlite-test")
	if err != nil {
		panic("failed to create temp file")
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		panic("failed to write to temp file")
	}
	return f.Name()
}

func mustCreateGetCertFunc(t *testing.T, certFile, keyFile string) func() (*tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("failed to load cert: %v", err)
	}
	return func() (*tls.Certificate, error) {
		return &cert, nil
	}
}
