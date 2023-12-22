package rtls

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"os"
	"testing"
	"time"
)

func Test_CreateConfig(t *testing.T) {
	// generate a cert and key pair, and write both to a temporary file
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	// generate a CA cert, and write it to a temporary file
	caCertPEM, _, err := GenerateCACert(pkix.Name{CommonName: "rqlite CA"}, 365*24*time.Hour, 2048)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	caCertFile := mustWriteTempFile(t, caCertPEM)

	// create a config with no server or client verification and no custom server name
	config, err := CreateConfig(certFile, keyFile, caCertFile, "", true, false)
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	if config.ClientAuth != tls.NoClientCert {
		t.Fatalf("expected ClientAuth to be NoClientCert, got %v", config.ClientAuth)
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

	// Check that the root and client CAs are loaded with the correct certificate
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertPEM)

	if config.RootCAs == nil {
		t.Fatalf("got nil root CA")
	}
	if !config.RootCAs.Equal(caCertPool) {
		t.Fatalf("expected root CA to be %v, got %v", caCertPool, config.RootCAs)
	}

	if config.ClientCAs == nil {
		t.Fatalf("got nil client CA")
	}
	if !config.ClientCAs.Equal(caCertPool) {
		t.Fatalf("expected client CA to be %v, got %v", caCertPool, config.ClientCAs)
	}

	// create a config with server cert verification only
	config, err = CreateConfig(certFile, keyFile, caCertFile, "", false, false)
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	if config.ClientAuth != tls.NoClientCert {
		t.Fatalf("expected ClientAuth to be NoClientCert, got %v", config.ClientAuth)
	}
	if config.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify to be false, got true")
	}

	// create a config with both server and client verification
	config, err = CreateConfig(certFile, keyFile, "", "", false, true)
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	if config.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected ClientAuth to be RequireAndVerifyClientCert, got %v", config.ClientAuth)
	}
	if config.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify to be false, got true")
	}

	// create a config with explicit node server name
	config, err = CreateConfig(certFile, keyFile, "", "rqlite.example.com", false, true)
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	if config.ServerName != "rqlite.example.com" {
		t.Fatalf("expected ServerNAme to be 'rqlite.example.com', got %s", config.ServerName)
	}
}

func Test_CreateServerConfig(t *testing.T) {
	// generate a cert and key pair, and write both to a temporary file
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	// create a server config with no client verification
	config, err := CreateServerConfig(certFile, keyFile, NoCACert, true)
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
	config, err = CreateServerConfig(certFile, keyFile, NoCACert, false)
	if err != nil {
		t.Fatalf("failed to create server config: %v", err)
	}
	if config.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected ClientAuth to be RequireAndVerifyClientCert, got %v", config.ClientAuth)
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
		t.Fatalf("expected InsecureSkipVerify to be true, got falsee")
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
