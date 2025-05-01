package rtls

import (
	"crypto/tls"
	"sync/atomic"
	"time"
)

// CertReloader is a simple TLS certificate reloader. It loads the certificate
// and key files from the specified paths and reloads them if they have been
// modified since the last load.
type CertReloader struct {
	certPath, keyPath string
	modTime           time.Time    // last successful load
	current           atomic.Value // *tls.Certificate
}

// NewReloader creates a new CertReloader instance.
func NewReloader(cert, key string) (*CertReloader, error) {
	cr := &CertReloader{certPath: cert, keyPath: key}
	if err := cr.reload(); err != nil {
		return nil, err
	}
	return cr, nil
}

func (cr *CertReloader) reload() error {
	lm, err := getModTime(cr.certPath, cr.keyPath)
	if err != nil {
		return err
	}
	if !lm.After(cr.modTime) {
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
// if it has been modified since the last load. It is deliberately the same
// signature as the tls.Config.GetCertificate method, which allows this
// to be used as a GetCertificate function in a tls.Config.
func (cr *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	_ = cr.reload() // ignore error, fall back to last cert
	return cr.current.Load().(*tls.Certificate), nil
}
