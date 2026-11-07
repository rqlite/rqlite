package rtls

import (
	"crypto/tls"
	"log"
	"os"
	"sync"
	"time"
)

// CertReloader is a simple TLS certificate reloader. It loads the certificate
// and key files from the specified paths and reloads them if they have been
// modified since the last load.
type CertReloader struct {
	certPath, keyPath string
	modTimes          [2]time.Time
	logger            *log.Logger

	mu   sync.RWMutex
	cert *tls.Certificate
}

// NewCertReloader creates a new CertReloader instance.
func NewCertReloader(cert, key string) (*CertReloader, error) {
	cr := &CertReloader{
		certPath: cert,
		keyPath:  key,
		logger:   log.New(os.Stderr, "[cert-reloader] ", log.LstdFlags),
	}
	pair, err := loadKeyPair(cr.certPath, cr.keyPath)
	if err != nil {
		return nil, err
	}
	cr.cert = &pair
	modTimes, err := fileModTimes(cr.certPath, cr.keyPath)
	if err != nil {
		return nil, err
	}
	cr.modTimes = modTimes
	return cr, nil
}

// GetCertificate returns the current certificate. It reloads the certificate
// if it has been modified since the last load. GetCertificate is thread-safe
// and if the cert has not changed this function will execute concurrently with
// other GetCertificate calls.
func (cr *CertReloader) GetCertificate() (*tls.Certificate, error) {
	cr.mu.RLock()
	modTimes, err := fileModTimes(cr.certPath, cr.keyPath)
	if err != nil || (modTimes[0].Equal(cr.modTimes[0]) && modTimes[1].Equal(cr.modTimes[1])) {
		defer cr.mu.RUnlock()
		if err != nil {
			cr.logger.Printf("failed to get latest modification time (%s), returning prior cert", err)
		}
		return cr.cert, nil
	}

	// The certificate or key file has changed, we need to reload. First we release
	// the read lock, then we acquire a write lock to reload the certificate.
	cr.mu.RUnlock()
	cr.mu.Lock()
	defer cr.mu.Unlock()

	// Now that we have the write lock, we check again if the certificate has actually
	// been updated by another concurrent call to this function.
	modTimes, err = fileModTimes(cr.certPath, cr.keyPath)
	if err != nil || (modTimes[0].Equal(cr.modTimes[0]) && modTimes[1].Equal(cr.modTimes[1])) {
		if err != nil {
			cr.logger.Printf("failed to get latest modification time (%s), returning prior cert", err)
		}
		return cr.cert, nil
	}

	pair, err := loadKeyPair(cr.certPath, cr.keyPath)
	if err != nil {
		cr.logger.Printf("failed to reload certificate (%s), returning prior cert", err)
		return cr.cert, nil
	}
	cr.logger.Printf("reloading certificate at %s and key at %s", cr.certPath, cr.keyPath)
	cr.cert = &pair
	cr.modTimes = modTimes
	return cr.cert, nil
}

// loadKeyPair loads a TLS certificate and key pair from the given files.
// It simply wraps tls.LoadX509KeyPair, ensuring that the rest of the code
// uses the same function to load the key pair.
func loadKeyPair(certFile, keyFile string) (tls.Certificate, error) {
	return tls.LoadX509KeyPair(certFile, keyFile)
}

// fileModTimes returns the modification time of each certificate file.
// Tracking both times avoids masking changes to the older of the two files.
func fileModTimes(certFile, keyFile string) ([2]time.Time, error) {
	var times [2]time.Time
	for i, path := range []string{certFile, keyFile} {
		info, err := os.Stat(path)
		if err != nil {
			return times, err
		}
		times[i] = info.ModTime()
	}
	return times, nil
}
