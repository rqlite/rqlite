package rtls

import (
	"bytes" // For comparing certificate data
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"reflect" // For comparing complex structs if needed, though CN check is simpler
	"sync"
	"testing"
	"time"
)

const (
	testMonitorInterval = 50 * time.Millisecond  // Faster interval for tests
	testWaitTimeout     = 500 * time.Millisecond // Time to wait for reloads
	testKey1            = "service-a"
	testKey2            = "service-b"
)

// testLogger creates a logger outputting to the test log if -v is used.
func testLogger(t *testing.T) *log.Logger {
	t.Helper()
	// Note: testing.Verbose() doesn't guarantee output goes to stderr easily,
	// but it controls whether t.Logf prints. We'll just use Discard or Stderr.
	// If verbose, use stderr, otherwise discard.
	writer := io.Discard
	if testing.Verbose() {
		writer = os.Stderr
	}
	return log.New(writer, "[cert-monitor-test] ", log.LstdFlags|log.Lmicroseconds)
}

// Test_CertMonitor_Monitor_Basic tests adding a monitor and retrieving its cert func.
func Test_CertMonitor_Monitor_Basic(t *testing.T) {
	cm := NewCertMonitor(WithLogger(testLogger(t)), WithInterval(testMonitorInterval))
	defer cm.Stop() // Ensure cleanup even on failure

	certFile, keyFile := mustGenerateAndWriteCert(t, "basic-cn", nil)

	err := cm.Monitor(testKey1, certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to monitor initial cert: %v", err)
	}

	getCertFunc, err := cm.GetCertificateFunc(testKey1)
	if err != nil {
		t.Fatalf("Failed to get certificate func for key %s: %v", testKey1, err)
	}
	if getCertFunc == nil {
		t.Fatal("GetCertificateFunc returned nil func")
	}

	// Simulate TLS handshake calling the function
	cert, err := getCertFunc(nil) // Pass nil ClientHelloInfo as per our implementation agreement
	if err != nil {
		t.Fatalf("getCertFunc callback returned an error: %v", err)
	}
	if cert == nil {
		t.Fatal("getCertFunc callback returned nil certificate")
	}
	if cert.Leaf == nil {
		t.Fatal("getCertFunc callback returned certificate with nil Leaf")
	}
	if cert.Leaf.Subject.CommonName != "basic-cn" {
		t.Errorf("Expected CommonName 'basic-cn', got '%s'", cert.Leaf.Subject.CommonName)
	}

	// Test getting a func for a non-existent key
	_, err = cm.GetCertificateFunc("non-existent-key")
	if err == nil {
		t.Fatal("Expected error getting func for non-existent key, got nil")
	}
}

// Test_CertMonitor_Monitor_InitialLoadFailure tests Monitor returning error for bad paths/pairs.
func Test_CertMonitor_Monitor_InitialLoadFailure(t *testing.T) {
	cm := NewCertMonitor(WithLogger(testLogger(t)), WithInterval(testMonitorInterval))
	defer cm.Stop()

	// 1. Non-existent file
	err := cm.Monitor(testKey1, "non-existent-cert", "non-existent-key")
	if err == nil {
		t.Fatal("Expected error for non-existent files, got nil")
	}
	t.Logf("Got expected error for non-existent files: %v", err)

	// 2. Mismatched pair
	certFile1, _ := mustGenerateAndWriteCert(t, "mismatch-cn1", nil)
	_, keyFile2 := mustGenerateAndWriteCert(t, "mismatch-cn2", nil)
	err = cm.Monitor(testKey1, certFile1, keyFile2)
	if err == nil {
		t.Fatal("Expected error for mismatched cert/key pair, got nil")
	}
	t.Logf("Got expected error for mismatched files: %v", err)

	// Ensure no func was added
	_, err = cm.GetCertificateFunc(testKey1)
	if err == nil {
		t.Fatal("GetCertificateFunc should fail after Monitor error, but succeeded")
	}
}

// Test_CertMonitor_Reload tests successful reloading of a certificate.
func Test_CertMonitor_Reload(t *testing.T) {
	cm := NewCertMonitor(WithLogger(testLogger(t)), WithInterval(testMonitorInterval))
	defer cm.Stop()

	// --- Initial Cert ---
	certFile, keyFile := mustGenerateAndWriteCert(t, "reload-cn1", nil)
	err := cm.Monitor(testKey1, certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to monitor initial cert: %v", err)
	}

	getCertFunc, err := cm.GetCertificateFunc(testKey1)
	if err != nil {
		t.Fatalf("Failed to get initial cert func: %v", err)
	}
	cert1, err := getCertFunc(nil)
	if err != nil {
		t.Fatalf("Failed to get initial cert via func: %v", err)
	}
	if cert1.Leaf.Subject.CommonName != "reload-cn1" {
		t.Fatalf("Initial CN mismatch: expected 'reload-cn1', got '%s'", cert1.Leaf.Subject.CommonName)
	}

	// --- Generate and Write Second Cert ---
	certFileTmp, keyFileTmp := mustGenerateAndWriteCert(t, "reload-cn2", nil)

	// --- Atomically Replace Files and Update ModTime ---
	if err := os.Rename(keyFileTmp, keyFile); err != nil {
		t.Fatalf("Failed to rename key file: %v", err)
	}
	if err := os.Rename(certFileTmp, certFile); err != nil {
		t.Fatalf("Failed to rename cert file: %v", err)
	}
	mustAdvanceFileOneSec(t, certFile) // Update mod time *after* rename

	// --- Wait and Verify Reload ---
	time.Sleep(testWaitTimeout) // Allow time for monitor to detect and reload

	cert2, err := getCertFunc(nil)
	if err != nil {
		t.Fatalf("getCertFunc failed after reload: %v", err)
	}
	if cert2 == nil {
		t.Fatal("Certificate is nil after reload")
	}
	if cert2.Leaf == nil {
		t.Fatal("Certificate Leaf is nil after reload")
	}
	if cert2.Leaf.Subject.CommonName != "reload-cn2" {
		t.Errorf("Certificate CN mismatch after reload: expected 'reload-cn2', got '%s'", cert2.Leaf.Subject.CommonName)
	}
	if bytes.Equal(cert1.Certificate[0], cert2.Certificate[0]) { // Compare raw cert data
		t.Error("Certificate should be different after reload, but raw data matches")
	}
}

// Test_CertMonitor_Reload_InvalidData tests that reloading with invalid data keeps the old cert.
func Test_CertMonitor_Reload_InvalidData(t *testing.T) {
	cm := NewCertMonitor(WithLogger(testLogger(t)), WithInterval(testMonitorInterval))
	defer cm.Stop()

	// --- Initial Cert ---
	certFile, keyFile := mustGenerateAndWriteCert(t, "invalid-reload-cn", nil)
	err := cm.Monitor(testKey1, certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to monitor initial cert: %v", err)
	}

	getCertFunc, err := cm.GetCertificateFunc(testKey1)
	if err != nil {
		t.Fatalf("Failed to get initial cert func: %v", err)
	}
	certOriginal, err := getCertFunc(nil)
	if err != nil {
		t.Fatalf("Failed to get initial cert via func: %v", err)
	}
	if certOriginal.Leaf.Subject.CommonName != "invalid-reload-cn" {
		t.Fatalf("Initial CN mismatch: expected 'invalid-reload-cn', got '%s'", certOriginal.Leaf.Subject.CommonName)
	}

	// --- Overwrite Cert with Bad Data ---
	if err := os.WriteFile(certFile, []byte("<<<INVALID CERT DATA>>>"), 0644); err != nil {
		t.Fatalf("Failed to write invalid cert data: %v", err)
	}
	mustAdvanceFileOneSec(t, certFile) // Trigger reload check

	// --- Wait and Verify Cert Did NOT Change ---
	time.Sleep(testWaitTimeout)

	certAfter, err := getCertFunc(nil)
	if err != nil {
		t.Fatalf("getCertFunc failed after invalid reload attempt: %v", err)
	}
	if certAfter == nil {
		t.Fatal("Certificate is nil after invalid reload attempt")
	}
	if certAfter.Leaf == nil {
		t.Fatal("Certificate Leaf is nil after invalid reload attempt")
	}
	if certAfter.Leaf.Subject.CommonName != "invalid-reload-cn" {
		t.Errorf("CN should not change after invalid reload: expected '%s', got '%s'", certOriginal.Leaf.Subject.CommonName, certAfter.Leaf.Subject.CommonName)
	}
	if !bytes.Equal(certOriginal.Certificate[0], certAfter.Certificate[0]) { // Compare raw cert data
		t.Error("Certificate should be the same after invalid reload, but raw data differs")
	}
}

// Test_CertMonitor_Monitor_Replace tests replacing an existing monitor for a key.
func Test_CertMonitor_Monitor_Replace(t *testing.T) {
	cm := NewCertMonitor(WithLogger(testLogger(t)), WithInterval(testMonitorInterval))
	defer cm.Stop()

	// --- Monitor First Cert ---
	certFile1, keyFile1 := mustGenerateAndWriteCert(t, "replace-cn1", nil)
	err := cm.Monitor(testKey1, certFile1, keyFile1)
	if err != nil {
		t.Fatalf("Failed to monitor initial cert: %v", err)
	}

	getCertFunc1, err := cm.GetCertificateFunc(testKey1)
	if err != nil {
		t.Fatalf("Failed to get func for cert 1: %v", err)
	}
	cert1, err := getCertFunc1(nil)
	if err != nil {
		t.Fatalf("Failed to get cert 1 via func: %v", err)
	}
	if cert1.Leaf.Subject.CommonName != "replace-cn1" {
		t.Fatalf("Cert 1 CN mismatch: expected 'replace-cn1', got '%s'", cert1.Leaf.Subject.CommonName)
	}

	// --- Monitor Second Cert with Same Key ---
	certFile2, keyFile2 := mustGenerateAndWriteCert(t, "replace-cn2", nil)
	err = cm.Monitor(testKey1, certFile2, keyFile2) // Replace existing monitor
	if err != nil {
		t.Fatalf("Failed to replace monitor for key %s: %v", testKey1, err)
	}

	// --- Verify the *New* Cert Func gets the *Second* Cert ---
	getCertFunc2, err := cm.GetCertificateFunc(testKey1)
	if err != nil {
		t.Fatalf("Failed to get certificate func after replacement: %v", err)
	}
	cert2, err := getCertFunc2(nil)
	if err != nil {
		t.Fatalf("getCertFunc (post-replace) failed: %v", err)
	}
	if cert2 == nil {
		t.Fatal("Certificate is nil after replacement")
	}
	if cert2.Leaf == nil {
		t.Fatal("Certificate Leaf is nil after replacement")
	}
	if cert2.Leaf.Subject.CommonName != "replace-cn2" {
		t.Errorf("Certificate CN mismatch after replacement: expected 'replace-cn2', got '%s'", cert2.Leaf.Subject.CommonName)
	}
	if bytes.Equal(cert1.Certificate[0], cert2.Certificate[0]) {
		t.Error("Certificates should differ after replacement, but raw data matches")
	}

	// --- Test Reload on the *Replaced* Monitor ---
	certFileTmp, keyFileTmp := mustGenerateAndWriteCert(t, "replace-cn3", nil)
	if err := os.Rename(keyFileTmp, keyFile2); err != nil { // Rename into the *second* key file path
		t.Fatalf("Failed to rename key file for reload: %v", err)
	}
	if err := os.Rename(certFileTmp, certFile2); err != nil { // Rename into the *second* cert file path
		t.Fatalf("Failed to rename cert file for reload: %v", err)
	}
	mustAdvanceFileOneSec(t, certFile2)

	time.Sleep(testWaitTimeout) // Wait for reload

	cert3, err := getCertFunc2(nil) // Use the func obtained *after* replacement
	if err != nil {
		t.Fatalf("Failed to get cert 3 via func: %v", err)
	}
	if cert3 == nil {
		t.Fatal("Cert 3 is nil")
	}
	if cert3.Leaf == nil {
		t.Fatal("Cert 3 leaf is nil")
	}
	if cert3.Leaf.Subject.CommonName != "replace-cn3" {
		t.Errorf("Certificate CN mismatch after reload post-replace: expected 'replace-cn3', got '%s'", cert3.Leaf.Subject.CommonName)
	}
	if bytes.Equal(cert2.Certificate[0], cert3.Certificate[0]) {
		t.Error("Certificates 2 and 3 should differ after reload post-replace, but raw data matches")
	}
}

// Test_CertMonitor_MultipleKeys tests monitoring multiple keys concurrently.
func Test_CertMonitor_MultipleKeys(t *testing.T) {
	cm := NewCertMonitor(WithLogger(testLogger(t)), WithInterval(testMonitorInterval))
	defer cm.Stop()

	// --- Monitor Key 1 ---
	certFile1, keyFile1 := mustGenerateAndWriteCert(t, "multi-cn1-v1", nil)
	err := cm.Monitor(testKey1, certFile1, keyFile1)
	if err != nil {
		t.Fatalf("Failed to monitor key %s: %v", testKey1, err)
	}
	getCertFunc1, err := cm.GetCertificateFunc(testKey1)
	if err != nil {
		t.Fatalf("Failed to get func for key %s: %v", testKey1, err)
	}

	// --- Monitor Key 2 ---
	certFile2, keyFile2 := mustGenerateAndWriteCert(t, "multi-cn2-v1", nil)
	err = cm.Monitor(testKey2, certFile2, keyFile2)
	if err != nil {
		t.Fatalf("Failed to monitor key %s: %v", testKey2, err)
	}
	getCertFunc2, err := cm.GetCertificateFunc(testKey2)
	if err != nil {
		t.Fatalf("Failed to get func for key %s: %v", testKey2, err)
	}

	// --- Verify Initial Certs ---
	cert1v1, err := getCertFunc1(nil)
	if err != nil {
		t.Fatalf("Failed to get cert 1v1: %v", err)
	}
	if cert1v1.Leaf.Subject.CommonName != "multi-cn1-v1" {
		t.Errorf("Initial CN mismatch key 1: expected 'multi-cn1-v1', got '%s'", cert1v1.Leaf.Subject.CommonName)
	}
	cert2v1, err := getCertFunc2(nil)
	if err != nil {
		t.Fatalf("Failed to get cert 2v1: %v", err)
	}
	if cert2v1.Leaf.Subject.CommonName != "multi-cn2-v1" {
		t.Errorf("Initial CN mismatch key 2: expected 'multi-cn2-v1', got '%s'", cert2v1.Leaf.Subject.CommonName)
	}

	// --- Reload Key 1 ---
	certFile1Tmp, keyFile1Tmp := mustGenerateAndWriteCert(t, "multi-cn1-v2", nil)
	if err := os.Rename(keyFile1Tmp, keyFile1); err != nil {
		t.Fatalf("Failed to rename key 1: %v", err)
	}
	if err := os.Rename(certFile1Tmp, certFile1); err != nil {
		t.Fatalf("Failed to rename cert 1: %v", err)
	}
	mustAdvanceFileOneSec(t, certFile1)

	// --- Reload Key 2 ---
	certFile2Tmp, keyFile2Tmp := mustGenerateAndWriteCert(t, "multi-cn2-v2", nil)
	if err := os.Rename(keyFile2Tmp, keyFile2); err != nil {
		t.Fatalf("Failed to rename key 2: %v", err)
	}
	if err := os.Rename(certFile2Tmp, certFile2); err != nil {
		t.Fatalf("Failed to rename cert 2: %v", err)
	}
	mustAdvanceFileOneSec(t, certFile2)

	// --- Wait and Verify Both Reloaded ---
	// Use a slightly longer wait to increase likelihood both tickers fire
	time.Sleep(testWaitTimeout + testMonitorInterval)

	cert1v2, err := getCertFunc1(nil)
	if err != nil {
		t.Fatalf("Failed to get cert 1v2: %v", err)
	}
	if cert1v2.Leaf.Subject.CommonName != "multi-cn1-v2" {
		t.Errorf("Key 1 CN mismatch after reload: expected 'multi-cn1-v2', got '%s'", cert1v2.Leaf.Subject.CommonName)
	}
	cert2v2, err := getCertFunc2(nil)
	if err != nil {
		t.Fatalf("Failed to get cert 2v2: %v", err)
	}
	if cert2v2.Leaf.Subject.CommonName != "multi-cn2-v2" {
		t.Errorf("Key 2 CN mismatch after reload: expected 'multi-cn2-v2', got '%s'", cert2v2.Leaf.Subject.CommonName)
	}

	if bytes.Equal(cert1v1.Certificate[0], cert1v2.Certificate[0]) {
		t.Error("Key 1 certificate should differ after reload, but raw data matches")
	}
	if bytes.Equal(cert2v1.Certificate[0], cert2v2.Certificate[0]) {
		t.Error("Key 2 certificate should differ after reload, but raw data matches")
	}
}

// Test_CertMonitor_Stop tests that Stop terminates monitoring goroutines.
func Test_CertMonitor_Stop(t *testing.T) {
	cm := NewCertMonitor(WithLogger(testLogger(t)), WithInterval(testMonitorInterval))

	certFile1, keyFile1 := mustGenerateAndWriteCert(t, "stop-cn1", nil)
	err := cm.Monitor(testKey1, certFile1, keyFile1)
	if err != nil {
		t.Fatalf("Failed to monitor key 1: %v", err)
	}

	certFile2, keyFile2 := mustGenerateAndWriteCert(t, "stop-cn2", nil)
	err = cm.Monitor(testKey2, certFile2, keyFile2)
	if err != nil {
		t.Fatalf("Failed to monitor key 2: %v", err)
	}

	getCertFunc1, err := cm.GetCertificateFunc(testKey1)
	if err != nil {
		t.Fatalf("Failed to get func for key 1: %v", err)
	}

	// Ensure monitor is running (check initial cert)
	cert1, err := getCertFunc1(nil)
	if err != nil {
		t.Fatalf("Failed to get initial cert 1: %v", err)
	}
	if cert1.Leaf.Subject.CommonName != "stop-cn1" {
		t.Fatalf("Initial CN mismatch key 1: expected 'stop-cn1', got '%s'", cert1.Leaf.Subject.CommonName)
	}

	// Stop the monitor
	stopDone := make(chan struct{})
	go func() {
		cm.Stop()
		close(stopDone)
	}()

	// Wait for Stop() to complete, with a timeout
	select {
	case <-stopDone:
		t.Log("CertMonitor Stop() completed.")
	case <-time.After(2 * testWaitTimeout): // Use a slightly longer timeout for stop
		t.Fatal("CertMonitor Stop() timed out")
	}

	// Try to trigger a reload *after* Stop() should have finished
	certFile1Tmp, keyFile1Tmp := mustGenerateAndWriteCert(t, "stop-cn1-poststop", nil)
	if err := os.Rename(keyFile1Tmp, keyFile1); err != nil {
		t.Fatalf("Failed to rename key file post-stop: %v", err)
	}
	if err := os.Rename(certFile1Tmp, certFile1); err != nil {
		t.Fatalf("Failed to rename cert file post-stop: %v", err)
	}
	mustAdvanceFileOneSec(t, certFile1)

	// Wait a bit, check if the cert *did not* change
	time.Sleep(testWaitTimeout)

	cert1AfterStop, err := getCertFunc1(nil) // Use the previously obtained func
	if err != nil {
		t.Fatalf("Failed to get cert 1 post-stop: %v", err)
	}
	if cert1AfterStop.Leaf.Subject.CommonName != "stop-cn1" {
		t.Errorf("Certificate should not have changed after Stop(): expected '%s', got '%s'", cert1.Leaf.Subject.CommonName, cert1AfterStop.Leaf.Subject.CommonName)
	}

	// Calling Stop() again should be a no-op and return quickly
	stopAgainDone := make(chan struct{})
	go func() {
		cm.Stop()
		close(stopAgainDone)
	}()
	select {
	case <-stopAgainDone:
		t.Log("Second CertMonitor Stop() completed quickly.")
	case <-time.After(testMonitorInterval * 2): // Should be very fast
		t.Fatal("Second CertMonitor Stop() call blocked")
	}
}

// Test_CertMonitor_Symlinks tests initial load and reload via symlinks.
func Test_CertMonitor_Symlinks(t *testing.T) {
	cm := NewCertMonitor(WithLogger(testLogger(t)), WithInterval(testMonitorInterval))
	defer cm.Stop()

	// --- Create Cert 1 ---
	certFileReal1, keyFileReal1 := mustGenerateAndWriteCert(t, "symlink-cn1", nil)

	// --- Create Symlinks pointing to Cert 1 ---
	tmpDir := t.TempDir() // Use test specific temp dir for links
	certLink := filepath.Join(tmpDir, "cert.pem.link")
	keyLink := filepath.Join(tmpDir, "key.pem.link")
	if err := os.Symlink(certFileReal1, certLink); err != nil {
		t.Fatalf("Failed to create cert symlink: %v", err)
	}
	if err := os.Symlink(keyFileReal1, keyLink); err != nil {
		t.Fatalf("Failed to create key symlink: %v", err)
	}

	// --- Monitor the Symlinks ---
	err := cm.Monitor(testKey1, certLink, keyLink)
	if err != nil {
		t.Fatalf("Failed to monitor symlinks: %v", err)
	}

	getCertFunc, err := cm.GetCertificateFunc(testKey1)
	if err != nil {
		t.Fatalf("Failed to get func for symlink monitor: %v", err)
	}

	// --- Verify Initial Load (Cert 1) ---
	cert1, err := getCertFunc(nil)
	if err != nil {
		t.Fatalf("Failed to get initial cert via symlink func: %v", err)
	}
	if cert1 == nil || cert1.Leaf == nil {
		t.Fatalf("Initial cert or leaf is nil via symlink")
	}
	if cert1.Leaf.Subject.CommonName != "symlink-cn1" {
		t.Fatalf("Initial CN mismatch with symlinks: expected 'symlink-cn1', got '%s'", cert1.Leaf.Subject.CommonName)
	}

	// --- Create Cert 2 ---
	certFileReal2, keyFileReal2 := mustGenerateAndWriteCert(t, "symlink-cn2", nil)

	// --- Update Symlinks to Point to Cert 2 ---
	if err := os.Remove(certLink); err != nil {
		t.Fatalf("Failed to remove old cert link: %v", err)
	}
	if err := os.Remove(keyLink); err != nil {
		t.Fatalf("Failed to remove old key link: %v", err)
	}
	if err := os.Symlink(certFileReal2, certLink); err != nil {
		t.Fatalf("Failed to recreate cert symlink: %v", err)
	}
	if err := os.Symlink(keyFileReal2, keyLink); err != nil {
		t.Fatalf("Failed to recreate key symlink: %v", err)
	}

	// Crucially, update the modification time of the *symlink itself*
	mustAdvanceFileOneSec(t, certLink)
	mustAdvanceFileOneSec(t, keyLink)

	// --- Wait and Verify Reload (Cert 2) ---
	time.Sleep(testWaitTimeout)

	cert2, err := getCertFunc(nil)
	if err != nil {
		t.Fatalf("getCertFunc failed after symlink reload: %v", err)
	}
	if cert2 == nil || cert2.Leaf == nil {
		t.Fatal("Certificate or leaf is nil after symlink reload")
	}
	if cert2.Leaf.Subject.CommonName != "symlink-cn2" {
		t.Errorf("CN mismatch after symlink reload: expected 'symlink-cn2', got '%s'", cert2.Leaf.Subject.CommonName)
	}
	if bytes.Equal(cert1.Certificate[0], cert2.Certificate[0]) {
		t.Error("Certificate should be different after symlink reload, but raw data matches")
	}
}

// --- Helper Functions ---

// mustGenerateAndWriteCert generates a cert/key pair and writes them to temporary files.
// It returns the paths to the cert and key files.
func mustGenerateAndWriteCert(t *testing.T, cn string, parentCert *x509.Certificate) (certFilePath, keyFilePath string) {
	t.Helper()
	certPEM, keyPEM, err := generateCert(pkix.Name{CommonName: cn}, time.Hour, 2048, parentCert, nil) // Short validity for tests
	if err != nil {
		t.Fatalf("failed to generate cert with CN %s: %v", cn, err)
	}

	certFile := mustWriteTempFile(t, certPEM, "testcert-*.pem")
	keyFile := mustWriteTempFile(t, keyPEM, "testkey-*.pem")

	return certFile, keyFile
}

// mustWriteTempFile writes data to a temporary file and returns its path.
func mustWriteTempFile(t *testing.T, data []byte, pattern string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), pattern)
	if err != nil {
		t.Fatalf("failed to create temp file with pattern %s: %v", pattern, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			// Log closing error but don't fail test here as write might have succeeded
			t.Logf("Warning: failed to close temp file %s: %v", f.Name(), cerr)
		}
	}()
	_, err = f.Write(data)
	if err != nil {
		t.Fatalf("failed to write to temp file %s: %v", f.Name(), err)
	}
	// Close happens in defer, get name before it might be invalid
	return f.Name()
}

// mustAdvanceFileOneSec updates the modification time of a file by one second.
func mustAdvanceFileOneSec(t *testing.T, file string) {
	t.Helper()
	// Use Lstat for consistency with symlink test logic, though Stat would also work for regular files.
	info, err := os.Lstat(file)
	if err != nil {
		t.Fatalf("failed to lstat file %s: %v", file, err)
	}
	newTime := info.ModTime().Add(1 * time.Second)
	// Ensure the new time is strictly after the current time in case mod time resolution is low
	if !newTime.After(info.ModTime()) {
		newTime = time.Now().Add(1 * time.Second) // Fallback to ensure advancement
	}

	err = os.Chtimes(file, newTime, newTime)
	if err != nil {
		t.Fatalf("failed to chtimes file %s: %v", file, err)
	}
	// Log the action for debugging, especially useful with -v
	t.Logf("Advanced mod time for %s to %s", file, newTime.Format(time.RFC3339Nano))

	// Optional: Verify the time was actually set (can help debug FS quirks)
	// verifyInfo, err := os.Lstat(file)
	// if err != nil {
	// 	 t.Logf("Warning: Could not verify mod time for %s after chtimes: %v", file, err)
	// } else if !verifyInfo.ModTime().Equal(newTime) {
	// 	 // Use Equal check as After check might fail due to time truncation by FS
	// 	 // Some FS might not store nano precision. Allow a small tolerance.
	// 	 if verifyInfo.ModTime().Truncate(time.Millisecond) != newTime.Truncate(time.Millisecond) {
	// 			t.Logf("Warning: Mod time for %s (%s) did not precisely match expected (%s) after chtimes",
	// 					file, verifyInfo.ModTime().Format(time.RFC3339Nano), newTime.Format(time.RFC3339Nano))
	// 	 }
	// }
}


// generateCert generates a new x509 certificate. (Adapted from original test helpers)
func generateCert(subject pkix.Name, validFor time.Duration, keySize int, parent *x509.Certificate, parentKey crypto.PrivateKey) ([]byte, []byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate serial number: %w", err)
	}

	notBefore := time.Now().Add(-2 * time.Minute) // Give 2 mins leeway for clock skew
	notAfter := notBefore.Add(validFor)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      subject,
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	var signerCert *x509.Certificate
	var signerKey crypto.PrivateKey
	if parent == nil {
		// Self-signed
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
		signerCert = &template
		signerKey = key
	} else {
		signerCert = parent
		signerKey = parentKey
		if signerKey == nil { // If parent cert provided but not key, assume self-signing parent needed key
			// This case shouldn't happen with current test usage, but defensively handle
             return nil, nil, fmt.Errorf("parent certificate provided but parent key is nil")
		}
	}


	certBytes, err := x509.CreateCertificate(rand.Reader, &template, signerCert, &key.PublicKey, signerKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
