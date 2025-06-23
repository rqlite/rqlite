package rtls

import (
	"crypto/tls"
	"log"
	"os"
	"sync/atomic"
	"time"
)

// monitorFiles watches the given files for changes, calling the supplied callback when a
// change is detected. The callback is passed a slice of the files that changed. The loop
// never terminates, so it's expected the caller will invoke this in a goroutine.
func monitorFiles(files []string, cb func([]string)) {
	mtimes := make([]time.Time, len(files))
	for {
		var changed []string
		for n, file := range files {
			info, err := os.Stat(file)
			if err != nil {
				continue
			}
			if !mtimes[n].IsZero() && info.ModTime() != mtimes[n] {
				changed = append(changed, file)
			}
			mtimes[n] = info.ModTime()
		}
		if changed != nil {
			cb(changed)
		}
		time.Sleep(2 * time.Second)
	}
}

// monitoredCertificate watches the given certificate and key files for changes, reloading
// the certificate if a change is detected.
type monitoredCertificate struct {
	certFile string
	keyFile  string
	// This holds a *tls.Certificate which can be updated from another goroutine, so use
	// an atomic value to synchronize access
	cert atomic.Value
}

// newMonitoredCertificate loads the certificate and key and spawns a goroutine to watch
// the files for changes.
func newMonitoredCertificate(certFile, keyFile string) (*monitoredCertificate, error) {
	mc := &monitoredCertificate{
		certFile: certFile,
		keyFile:  keyFile,
	}
	// Prime the cached certificate and induce any errors immediately
	if err := mc.load(); err != nil {
		return nil, err
	}
	// Now start watching for changes
	go monitorFiles([]string{certFile, keyFile}, func(changed []string) {
		if err := mc.load(); err != nil {
			log.Printf("error reloading certificate %s, not replacing: %s", certFile, err)
		} else {
			log.Printf("reloaded certificate %s", certFile)
		}
	})
	return mc, nil
}

// load reads the certificate and key files from disk and caches the result for Get()
func (mc *monitoredCertificate) load() error {
	cert, err := tls.LoadX509KeyPair(mc.certFile, mc.keyFile)
	if err != nil {
		return err
	}
	mc.cert.Store(&cert)
	return nil
}

// Get returns the certificate.
//
// This will never return nil provided the monitoredCertificate object was created via
// newMonitoredCertificate(),
func (mc *monitoredCertificate) Get() *tls.Certificate {
	return mc.cert.Load().(*tls.Certificate)
}
