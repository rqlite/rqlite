package rtls

import (
	"crypto/x509/pkix"
	"os"
	"testing"
	"time"
)

func Test_NewCertMonitor(t *testing.T) {
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	cm, err := NewCertMonitor(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to create CertMonitor: %v", err)
	}
	if cm == nil {
		t.Fatal("CertMonitor is nil")
	}

	certificate, err := cm.GetCertificate()
	if err != nil {
		t.Fatalf("Failed to get certificate: %v", err)
	}
	if certificate == nil {
		t.Fatal("Certificate is nil")
	}
	if certificate.Leaf == nil {
		t.Fatal("Certificate leaf is nil")
	}
	if certificate.Leaf.Subject.CommonName != "rqlite" {
		t.Fatalf("Expected CommonName to be 'rqlite', got '%s'", certificate.Leaf.Subject.CommonName)
	}
}

func Test_CertMonitor_Reload(t *testing.T) {
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	cm, err := NewCertMonitor(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to create CertMonitor: %v", err)
	}
	if cm == nil {
		t.Fatal("CertMonitor is nil")
	}

	certificate, err := cm.GetCertificate()
	if err != nil {
		t.Fatalf("Failed to get certificate: %v", err)
	}
	if certificate == nil {
		t.Fatal("Certificate is nil")
	}
	if certificate.Leaf == nil {
		t.Fatal("Certificate leaf is nil")
	}
	if certificate.Leaf.Subject.CommonName != "rqlite" {
		t.Fatalf("Expected CommonName to be 'rqlite', got '%s'", certificate.Leaf.Subject.CommonName)
	}

	cm.Start()
	defer cm.Stop()

	certPEM2, keyPEM2, err := GenerateCert(pkix.Name{CommonName: "rqlite2"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile2 := mustWriteTempFile(t, certPEM2)
	keyFile2 := mustWriteTempFile(t, keyPEM2)

	// Rename files for atomic update
	time.Sleep(time.Second)
	if err := os.Rename(keyFile2, keyFile); err != nil {
		t.Fatalf("failed to rename key file: %v", err)
	}
	if err := os.Rename(certFile2, certFile); err != nil {
		t.Fatalf("failed to rename cert file: %v", err)
	}

	// Wait for the certificate to be reloaded
	time.Sleep(time.Second)
	certificate, err = cm.GetCertificate()
	if err != nil {
		t.Fatalf("Failed to get certificate: %v", err)
	}
	if certificate == nil {
		t.Fatal("Certificate is nil")
	}
	if certificate.Leaf == nil {
		t.Fatal("Certificate leaf is nil")
	}
	if certificate.Leaf.Subject.CommonName != "rqlite2" {
		t.Fatalf("Expected CommonName to be 'rqlite2', got '%s'", certificate.Leaf.Subject.CommonName)
	}
}

func Test_CertMonitor_ReloadInvalid(t *testing.T) {
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	cm, err := NewCertMonitor(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to create CertMonitor: %v", err)
	}

	cm.Start()
	defer cm.Stop()

	original, err := cm.GetCertificate()
	if err != nil {
		t.Fatalf("Failed to get original certificate: %v", err)
	}

	// Overwrite the certificate file with invalid data
	time.Sleep(time.Second)
	if err := os.WriteFile(certFile, []byte("INVALID DATA"), 0644); err != nil {
		t.Fatalf("failed to overwrite cert file: %v", err)
	}

	// Wait for the certificate to be reloaded
	time.Sleep(time.Second)
	updated, err := cm.GetCertificate()
	if err != nil {
		t.Fatalf("Failed to get certificate after invalid overwrite: %v", err)
	}
	if updated.Leaf.Subject.CommonName != original.Leaf.Subject.CommonName {
		t.Fatalf("Expected certificate to remain '%s', got '%s'", original.Leaf.Subject.CommonName, updated.Leaf.Subject.CommonName)
	}
}

func Test_CertMonitor_Symlinks(t *testing.T) {
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "symlink"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFileReal := mustWriteTempFile(t, certPEM)
	keyFileReal := mustWriteTempFile(t, keyPEM)

	certLink := certFileReal + ".link"
	keyLink := keyFileReal + ".link"
	if err := os.Symlink(certFileReal, certLink); err != nil {
		t.Fatalf("failed to create cert symlink: %v", err)
	}
	if err := os.Symlink(keyFileReal, keyLink); err != nil {
		t.Fatalf("failed to create key symlink: %v", err)
	}

	cm, err := NewCertMonitor(certLink, keyLink)
	if err != nil {
		t.Fatalf("Failed to create CertMonitor with symlinks: %v", err)
	}

	certificate, err := cm.GetCertificate()
	if err != nil {
		t.Fatalf("Failed to get certificate: %v", err)
	}
	if certificate.Leaf.Subject.CommonName != "symlink" {
		t.Fatalf("Expected CommonName to be 'symlink', got '%s'", certificate.Leaf.Subject.CommonName)
	}
}

func Test_CertMonitor_Symlinks_Reload(t *testing.T) {
	// Create the test certs
	certPEM1, keyPEM1, err := GenerateCert(pkix.Name{CommonName: "symlink1"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFileReal1 := mustWriteTempFile(t, certPEM1)
	keyFileReal1 := mustWriteTempFile(t, keyPEM1)
	certPEM2, keyPEM2, err := GenerateCert(pkix.Name{CommonName: "symlink2"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFileReal2 := mustWriteTempFile(t, certPEM2)
	keyFileReal2 := mustWriteTempFile(t, keyPEM2)

	// Create symlinks pointing to the first cert
	certLink := certFileReal1 + ".link"
	keyLink := keyFileReal1 + ".link"
	if err := os.Symlink(certFileReal1, certLink); err != nil {
		t.Fatalf("failed to create cert symlink: %v", err)
	}
	if err := os.Symlink(keyFileReal1, keyLink); err != nil {
		t.Fatalf("failed to create key symlink: %v", err)
	}

	// Monitor the symlinks
	cm, err := NewCertMonitor(certLink, keyLink)
	if err != nil {
		t.Fatalf("Failed to create CertMonitor with symlinks: %v", err)
	}
	cm.Start()
	defer cm.Stop()

	certificate, err := cm.GetCertificate()
	if err != nil {
		t.Fatalf("Failed to get certificate: %v", err)
	}
	if certificate.Leaf.Subject.CommonName != "symlink1" {
		t.Fatalf("Expected CommonName to be 'symlink', got '%s'", certificate.Leaf.Subject.CommonName)
	}

	// Point the symlinks to the second cert
	if err := os.Remove(certLink); err != nil {
		t.Fatalf("failed to remove cert symlink: %v", err)
	}
	if err := os.Remove(keyLink); err != nil {
		t.Fatalf("failed to remove key symlink: %v", err)
	}
	time.Sleep(2 * time.Second)
	if err := os.Symlink(certFileReal2, certLink); err != nil {
		t.Fatalf("failed to create cert symlink: %v", err)
	}
	if err := os.Symlink(keyFileReal2, keyLink); err != nil {
		t.Fatalf("failed to create key symlink: %v", err)
	}

	// Wait for the certificate to be reloaded
	time.Sleep(time.Second)
	certificate, err = cm.GetCertificate()
	if err != nil {
		t.Fatalf("Failed to get certificate: %v", err)
	}
	if certificate.Leaf.Subject.CommonName != "symlink2" {
		t.Fatalf("Expected CommonName to be 'symlink2', got '%s'", certificate.Leaf.Subject.CommonName)
	}
}
