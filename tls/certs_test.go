package tls

import (
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"testing"
	"time"
)

// TestGenerateCACert tests the GenerateCACert function.
func TestGenerateCACert(t *testing.T) {
	// generate a new CA certificate
	certPEM, keyPEM, err := GenerateCACert(pkix.Name{CommonName: "rqlite.io"}, 0, time.Hour, 2048)
	if err != nil {
		t.Fatal(err)
	}

	// decode the certificate and private key
	cert, _ := pem.Decode(certPEM)
	if cert == nil {
		t.Fatal("failed to decode certificate")
	}

	key, _ := pem.Decode(keyPEM)
	if err != nil {
		t.Fatal("failed to decode key")
	}

	// parse the certificate and private key
	certParsed, err := x509.ParseCertificate(cert.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	keyParsed, err := x509.ParsePKCS1PrivateKey(key.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	// verify the certificate and private key
	if certParsed.Subject.CommonName != "rqlite.io" {
		t.Fatal("certificate subject is not correct")
	}

	if !certParsed.IsCA {
		t.Fatal("certificate is not a CA")
	}

	if certParsed.PublicKey.(*rsa.PublicKey).N.Cmp(keyParsed.N) != 0 {
		t.Fatal("certificate and private key do not match")
	}
}

func TestGenerateCASignedCert(t *testing.T) {
	caCert, caKey := mustGenerateCACert(pkix.Name{CommonName: "ca.rqlite"})

	// generate a new certificate signed by the CA
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "test"}, 365*24*time.Hour, 2048, caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}

	cert, _ := pem.Decode(certPEM)
	if cert == nil {
		panic("failed to decode certificate")
	}

	key, _ := pem.Decode(keyPEM)
	if key == nil {
		panic("failed to decode key")
	}

	// parse the certificate and private key
	parsedCert, err := x509.ParseCertificate(cert.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	_, err = x509.ParsePKCS1PrivateKey(key.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	// verify the certificate is signed by the CA
	if err := parsedCert.CheckSignatureFrom(caCert); err != nil {
		t.Fatal(err)
	}

	if parsedCert.NotBefore.After(time.Now()) {
		t.Fatal("certificate is not valid yet")
	}
	if parsedCert.NotAfter.Before(time.Now()) {
		t.Fatal("certificate is expired")
	}

	if parsedCert.Subject.CommonName != "test" {
		t.Fatal("certificate has incorrect subject")
	}

	expUsage := x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature
	if parsedCert.KeyUsage != expUsage {
		t.Fatalf("certificate has incorrect key usage, exp %v, got %v", expUsage, parsedCert.KeyUsage)
	}

	if len(parsedCert.ExtKeyUsage) != 1 || parsedCert.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Fatal("certificate has incorrect extended key usage")
	}

	if parsedCert.IsCA {
		t.Fatal("certificate has incorrect basic constraints")
	}
}

func TestGenerateSelfSignedCert(t *testing.T) {
	certPEM, keyPEM, err := GenerateSelfSignedCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048)
	if err != nil {
		t.Fatal(err)
	}

	// decode the certificate and private key
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		t.Fatal("failed to decode certificate PEM")
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		t.Fatal("failed to decode private key PEM")
	}

	// parse the certificate and private key
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	// verify the certificate
	if err := cert.CheckSignatureFrom(cert); err != nil {
		t.Fatal(err)
	}

	// verify the private key
	if err := cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature); err != nil {
		t.Fatal(err)
	}
	if err := key.Validate(); err != nil {
		t.Fatal(err)
	}
}

// mustGenerateCACert generates a new CA certificate and private key.
func mustGenerateCACert(name pkix.Name) (*x509.Certificate, *rsa.PrivateKey) {
	certPEM, keyPEM, err := GenerateCACert(name, 0, time.Hour, 2048)
	if err != nil {
		panic(err)
	}
	cert, _ := pem.Decode(certPEM)
	if cert == nil {
		panic("failed to decode certificate")
	}

	key, _ := pem.Decode(keyPEM)
	if key == nil {
		panic("failed to decode key")
	}

	parsedCert, err := x509.ParseCertificate(cert.Bytes)
	if err != nil {
		panic(err)
	}
	parsedKey, err := x509.ParsePKCS1PrivateKey(key.Bytes)
	if err != nil {
		panic(err)
	}

	return parsedCert, parsedKey
}
