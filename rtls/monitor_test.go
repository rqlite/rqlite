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

	dur := 10 * time.Millisecond
	cm, err := NewCertMonitor(certFile, keyFile, dur)
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

func Test_NewCertMonitor_Reload(t *testing.T) {
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certFile := mustWriteTempFile(t, certPEM)
	keyFile := mustWriteTempFile(t, keyPEM)

	dur := 10 * time.Millisecond
	cm, err := NewCertMonitor(certFile, keyFile, dur)
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

	// Rename keyfile2 to keyfile
	if err := os.Rename(keyFile2, keyFile); err != nil {
		t.Fatalf("failed to rename key file: %v", err)
	}
	// Rename certfile2 to certfile
	if err := os.Rename(certFile2, certFile); err != nil {
		t.Fatalf("failed to rename cert file: %v", err)
	}

	// Wait for the certificate to be reloaded
	time.Sleep(dur * 3)
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
