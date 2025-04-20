package rtls

import (
	"crypto/tls"
	"log"
	"os"
	"sync"
	"time"
)

// CertMonitor monitors a TLS certificate and key file for changes. If a change
// is detected, it reloads the certificate and key. The minimum duration
// for checking changes is one second.
//
// CertMonitor is based on checking the modification time of the certificate file.
// Changes that occur less than one second apart may not be detected.
type CertMonitor struct {
	certFile string
	keyFile  string

	lastModified time.Time
	dur          time.Duration
	done         chan struct{}
	wg           sync.WaitGroup

	mu          sync.Mutex
	certificate *tls.Certificate

	logger *log.Logger
}

// NewCertMonitor creates a new CertMonitor instance. The certificate and key files are loaded
// and monitored for changes once a second.
func NewCertMonitor(certFile, keyFile string) (*CertMonitor, error) {
	return NewCertMonitorWithDuration(certFile, keyFile, time.Second)
}

// NewCertMonitorWithDuration creates a new CertMonitor instance.
func NewCertMonitorWithDuration(certFile, keyFile string, dur time.Duration) (*CertMonitor, error) {
	parsedCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	modTime, err := getModTime(certFile)
	if err != nil {
		return nil, err
	}

	return &CertMonitor{
		certificate:  &parsedCert,
		certFile:     certFile,
		keyFile:      keyFile,
		lastModified: modTime,
		dur:          dur,
		done:         make(chan struct{}),
		logger:       log.New(os.Stderr, "[cert-monitor] ", log.LstdFlags),
	}, nil
}

// GetCertificate returns the current TLS certificate.
func (cm *CertMonitor) GetCertificate() (*tls.Certificate, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.certificate, nil
}

// Start starts the certificate monitor.
func (cm *CertMonitor) Start() {
	cm.wg.Add(1)
	go cm.do()
}

// Stop stops the certificate monitor.
func (cm *CertMonitor) Stop() {
	close(cm.done)
	cm.wg.Wait()
}

func (cm *CertMonitor) do() {
	defer cm.wg.Done()
	ticker := time.NewTicker(cm.dur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			modTime, err := getModTime(cm.certFile)
			if err != nil {
				cm.logger.Printf("failed to get modification time for %s: %v", cm.certFile, err)
				continue
			}

			if modTime.After(cm.lastModified) {
				cm.logger.Printf("reloading certificate %s as it has been modified", cm.certFile)
				cm.lastModified = modTime
				parsedCert, err := tls.LoadX509KeyPair(cm.certFile, cm.keyFile)
				if err != nil {
					cm.logger.Printf("failed to load certificate %s: %s", cm.certFile, err)
					continue
				}
				cm.mu.Lock()
				cm.certificate = &parsedCert
				cm.mu.Unlock()
			}
		case <-cm.done:
			return
		}
	}
}

func getModTime(file string) (time.Time, error) {
	info, err := os.Stat(file)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}
