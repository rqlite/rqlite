package rtls

import (
	"crypto/x509/pkix"
	"os"
	"testing"
	"time"
)

func Test_NewReloader(t *testing.T) {
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certPath := mustWriteTempFile(t, certPEM)
	keyPath := mustWriteTempFile(t, keyPEM)

	cr, err := NewReloader(certPath, keyPath)
	if err != nil {
		t.Fatalf("NewReloader returned error: %v", err)
	}

	c, err := cr.GetCertificate()
	if err != nil {
		t.Fatalf("GetCertificate error: %v", err)
	}
	if c.Leaf.Subject.CommonName != "rqlite" {
		t.Fatalf("Expected CommonName to be 'rqlite', got '%s'", c.Leaf.Subject.CommonName)
	}
}

func Test_NewReloaderNoFiles(t *testing.T) {
	_, err := NewReloader("xxx", "yyyy")
	if err == nil {
		t.Fatalf("NewReloader should have returned error")
	}
}

func Test_NewReloaderBadFiles(t *testing.T) {
	certPath := mustWriteTempFile(t, []byte("bad cert"))
	keyPath := mustWriteTempFile(t, []byte("bad key"))
	_, err := NewReloader(certPath, keyPath)
	if err == nil {
		t.Fatalf("NewReloader should have returned error")
	}
}

func Test_NoReloadWhenUnchanged(t *testing.T) {
	certPEM, keyPEM, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certPath := mustWriteTempFile(t, certPEM)
	keyPath := mustWriteTempFile(t, keyPEM)

	cr, err := NewReloader(certPath, keyPath)
	if err != nil {
		t.Fatalf("NewReloader error: %v", err)
	}

	c1, _ := cr.GetCertificate()
	c2, _ := cr.GetCertificate()

	if c1 != c2 {
		t.Fatalf("expected same *tls.Certificate pointer when files unchanged")
	}
}

func Test_ReloadOnFileChange(t *testing.T) {
	certPEM1, keyPEM1, err := GenerateCert(pkix.Name{CommonName: "rqlite1"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certPath1 := mustWriteTempFile(t, certPEM1)
	keyPath1 := mustWriteTempFile(t, keyPEM1)

	certPEM2, keyPEM2, err := GenerateCert(pkix.Name{CommonName: "rqlite2"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certPath2 := mustWriteTempFile(t, certPEM2)
	keyPath2 := mustWriteTempFile(t, keyPEM2)

	cr, err := NewReloader(certPath1, keyPath1)
	if err != nil {
		t.Fatalf("NewReloader error: %v", err)
	}
	c, err := cr.GetCertificate()
	if err != nil {
		t.Fatalf("GetCertificate error: %v", err)
	}
	if c.Leaf.Subject.CommonName != "rqlite1" {
		t.Fatalf("Expected CommonName to be 'rqlite', got '%s'", c.Leaf.Subject.CommonName)
	}

	if err := os.Rename(keyPath2, keyPath1); err != nil {
		t.Fatalf("failed to rename key file: %v", err)
	}
	if err := os.Rename(certPath2, certPath1); err != nil {
		t.Fatalf("failed to rename cert file: %v", err)
	}
	mustAdvanceFileOneSec(certPath1)
	mustAdvanceFileOneSec(keyPath1)

	c, err = cr.GetCertificate()
	if err != nil {
		t.Fatalf("GetCertificate error: %v", err)
	}
	if c.Leaf.Subject.CommonName != "rqlite2" {
		t.Fatalf("Expected CommonName to be 'rqlite2', got '%s'", c.Leaf.Subject.CommonName)
	}
}

func Test_NoReloadOnMismatch(t *testing.T) {
	certPEM1, keyPEM1, err := GenerateCert(pkix.Name{CommonName: "rqlite1"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	certPath1 := mustWriteTempFile(t, certPEM1)
	keyPath1 := mustWriteTempFile(t, keyPEM1)

	_, keyPEM2, err := GenerateCert(pkix.Name{CommonName: "rqlite2"}, 365*24*time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatalf("failed to generate cert: %v", err)
	}
	keyPath2 := mustWriteTempFile(t, keyPEM2)

	cr, err := NewReloader(certPath1, keyPath1)
	if err != nil {
		t.Fatalf("NewReloader error: %v", err)
	}
	c, err := cr.GetCertificate()
	if err != nil {
		t.Fatalf("GetCertificate error: %v", err)
	}
	if c.Leaf.Subject.CommonName != "rqlite1" {
		t.Fatalf("Expected CommonName to be 'rqlite', got '%s'", c.Leaf.Subject.CommonName)
	}

	// Only replace the key file so the new files are "mismatched"
	if err := os.Rename(keyPath2, keyPath1); err != nil {
		t.Fatalf("failed to rename key file: %v", err)
	}
	mustAdvanceFileOneSec(certPath1)
	mustAdvanceFileOneSec(keyPath1)

	c, err = cr.GetCertificate()
	if err != nil {
		t.Fatalf("GetCertificate error: %v", err)
	}
	if c.Leaf.Subject.CommonName != "rqlite1" {
		t.Fatalf("Expected CommonName to be 'rqlite1', got '%s'", c.Leaf.Subject.CommonName)
	}
}

// getModTime returns the latest modification time of the given files.
func getModTime(file ...string) (time.Time, error) {
	if len(file) == 0 {
		return time.Time{}, os.ErrNotExist
	}
	latest := time.Time{}
	for _, f := range file {
		info, err := os.Stat(f)
		if err != nil {
			return time.Time{}, err
		}
		if info.ModTime().After(latest) {
			latest = info.ModTime()
		}
	}
	return latest, nil
}

func mustAdvanceFileOneSec(file string) {
	lm, err := getModTime(file)
	if err != nil {
		panic("failed to get file time")
	}
	if os.Chtimes(file, lm.Add(1*time.Second), lm.Add(1*time.Second)) != nil {
		panic("failed to set file time")
	}
}
