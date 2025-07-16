package rtls

import (
	"crypto/tls"
	"os"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v8/rqlog"
)

// CertReloader is a simple TLS certificate reloader. It loads the certificate
// and key files from the specified paths and reloads them if they have been
// modified since the last load.
type CertReloader struct {
	certPath, keyPath string
	modTime           time.Time
	logger            rqlog.Logger

	mu   sync.RWMutex
	cert *tls.Certificate
}

// NewCertReloader creates a new CertReloader instance.
func NewCertReloader(cert, key string) (*CertReloader, error) {
	cr := &CertReloader{
		certPath: cert,
		keyPath:  key,
		logger:   rqlog.Default().WithName("[cert-reloader] ").WithOutput(os.Stderr),
	}
	pair, err := loadKeyPair(cr.certPath, cr.keyPath)
	if err != nil {
		return nil, err
	}
	cr.cert = &pair
	latestTime, err := latestModTime(cr.certPath, cr.keyPath)
	if err != nil {
		return nil, err
	}
	cr.modTime = latestTime
	return cr, nil
}

// GetCertificate returns the current certificate. It reloads the certificate
// if it has been modified since the last load. GetCertificate is thread-safe
// and if the cert has not changed this function will execute concurrently with
// other GetCertificate calls.
func (cr *CertReloader) GetCertificate() (*tls.Certificate, error) {
	cr.mu.RLock()
	latestTime, err := latestModTime(cr.certPath, cr.keyPath)
	if err != nil || !latestTime.After(cr.modTime) {
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
	latestTime, err = latestModTime(cr.certPath, cr.keyPath)
	if err != nil || !latestTime.After(cr.modTime) {
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
	cr.modTime = latestTime
	return cr.cert, nil
}

// loadKeyPair loads a TLS certificate and key pair from the given files.
// It simply wraps tls.LoadX509KeyPair, ensuring that the rest of the code
// uses the same function to load the key pair.
func loadKeyPair(certFile, keyFile string) (tls.Certificate, error) {
	return tls.LoadX509KeyPair(certFile, keyFile)
}

// latestModTime returns the latest modification time of the given files.
// It returns os.ErrNotExist if no files are provided.
func latestModTime(file ...string) (time.Time, error) {
	if len(file) == 0 {
		return time.Time{}, os.ErrNotExist
	}
	var latest time.Time
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
