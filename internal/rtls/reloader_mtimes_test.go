package rtls

import (
	"crypto/x509/pkix"
	"os"
	"testing"
	"time"
)

func Test_CertReloaderTracksBothModificationTimes(t *testing.T) {
	cert, key, err := GenerateCert(pkix.Name{CommonName: "rqlite"}, time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	replacementCert, replacementKey, err := GenerateCert(pkix.Name{CommonName: "replacement"}, time.Hour, 2048, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []string{"certificate", "key"} {
		t.Run(changed, func(t *testing.T) {
			certPath := mustWriteTempFile(t, cert)
			keyPath := mustWriteTempFile(t, key)
			changedPath, otherPath := certPath, keyPath
			if changed == "key" {
				changedPath, otherPath = keyPath, certPath
			}
			start := time.Unix(1700000000, 0)
			if err := os.Chtimes(changedPath, start, start); err != nil {
				t.Fatal(err)
			}
			later := start.Add(2 * time.Hour)
			if err := os.Chtimes(otherPath, later, later); err != nil {
				t.Fatal(err)
			}
			reloader, err := NewCertReloader(certPath, keyPath)
			if err != nil {
				t.Fatal(err)
			}
			first, err := reloader.GetCertificate()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(certPath, replacementCert, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(keyPath, replacementKey, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(otherPath, later, later); err != nil {
				t.Fatal(err)
			}
			changedTime := start.Add(time.Hour)
			if err := os.Chtimes(changedPath, changedTime, changedTime); err != nil {
				t.Fatal(err)
			}
			second, err := reloader.GetCertificate()
			if err != nil {
				t.Fatal(err)
			}
			if first == second || second.Leaf.Subject.CommonName != "replacement" {
				t.Fatal("did not reload after the older file changed")
			}
			third, err := reloader.GetCertificate()
			if err != nil {
				t.Fatal(err)
			}
			if second != third {
				t.Fatal("reloaded unchanged files")
			}
		})
	}
}
