package rtls

import (
	"crypto/tls"
	"log"
	"os"
	"sync/atomic"
	"time"
)

// CertReloader is a simple TLS certificate reloader. It loads the certificate
// and key files from the specified paths and reloads them if they have been
// modified since the last load.
type CertReloader struct {
	certPath, keyPath string
	modTime           time.Time
	current           atomic.Value
	logger            *log.Logger
}

// NewReloader creates a new CertReloader instance.
func NewReloader(cert, key string) (*CertReloader, error) {
	cr := &CertReloader{
		certPath: cert,
		keyPath:  key,
		logger:   log.New(os.Stderr, "[cert-reloader] ", log.LstdFlags),
	}
	if err := cr.reload(); err != nil {
		return nil, err
	}
	return cr, nil
}

func (cr *CertReloader) reload() error {
	lm, ok, err := newerThan(cr.modTime, cr.certPath, cr.keyPath)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}
	pair, err := loadKeyPair(cr.certPath, cr.keyPath)
	if err != nil {
		return err
	}
	cr.current.Store(&pair)
	cr.modTime = lm
	return nil
}

// GetCertificate returns the current certificate. It reloads the certificate
// if it has been modified since the last load.
func (cr *CertReloader) GetCertificate() (*tls.Certificate, error) {
	err := cr.reload()
	if err != nil {
		cr.logger.Printf("failed to reload certificate (%s), returning prior cert", err)
	}
	return cr.current.Load().(*tls.Certificate), nil
}

// loadKeyPair loads a TLS certificate and key pair from the given files.
// It simply wraps tls.LoadX509KeyPair, ensuring that the rest of the code
// uses the same function to load the key pair.
func loadKeyPair(certFile, keyFile string) (tls.Certificate, error) {
	return tls.LoadX509KeyPair(certFile, keyFile)
}

func newerThan(lm time.Time, file ...string) (time.Time, bool, error) {
	if len(file) == 0 {
		return time.Time{}, false, os.ErrNotExist
	}
	for _, f := range file {
		info, err := os.Stat(f)
		if err != nil {
			return time.Time{}, false, err
		}
		if info.ModTime().After(lm) {
			return info.ModTime(), true, nil
		}
	}
	return time.Time{}, false, nil
}
