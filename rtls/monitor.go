package rtls

import (
    "context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Ctx = context.Context // Alias for convenience

// DefaultMonitorInterval is the default interval for checking file modifications.
const DefaultMonitorInterval = time.Second

// monitoredCert holds the state for a single monitored certificate pair.
type monitoredCert struct {
	mu           sync.RWMutex // Protects certificate and lastModified
	certificate  *tls.Certificate
	lastModified time.Time
	certFile     string
	keyFile      string
	cancel       func() // Function to cancel the monitoring goroutine
	wg           sync.WaitGroup
}

// CertMonitor manages the monitoring of multiple TLS certificate/key pairs,
// each identified by a unique key string.
type CertMonitor struct {
	certs    map[string]*monitoredCert
	certsMu  sync.RWMutex // Protects the certs map
	stopOnce sync.Once    // Ensures global Stop logic runs only once
	wg       sync.WaitGroup // Tracks all active monitoring goroutines
	done     chan struct{}  // Signals global shutdown

	interval time.Duration
	logger   *log.Logger
}

// MonitorOption is used to configure a CertMonitor.
type MonitorOption func(*CertMonitor)

// WithInterval sets the monitoring interval for all certificates.
func WithInterval(d time.Duration) MonitorOption {
	return func(cm *CertMonitor) {
		if d > 0 {
			cm.interval = d
		}
	}
}

// WithLogger sets the logger for the monitor.
// If nil, logging is disabled using io.Discard.
func WithLogger(l *log.Logger) MonitorOption {
	return func(cm *CertMonitor) {
		cm.logger = l
	}
}

// WithLogOutput sets the output destination for the default logger.
func WithLogOutput(w io.Writer) MonitorOption {
	return func(cm *CertMonitor) {
		if w != nil && cm.logger != nil { // Ensure logger exists before setting output
			cm.logger.SetOutput(w)
		}
	}
}

// NewCertMonitor creates a new CertMonitor instance capable of managing multiple certificates.
func NewCertMonitor(opts ...MonitorOption) *CertMonitor {
	cm := &CertMonitor{
		certs:    make(map[string]*monitoredCert),
		interval: DefaultMonitorInterval,
		done:     make(chan struct{}),
		logger:   log.New(os.Stderr, "[cert-monitor] ", log.LstdFlags), // Default logger
	}

	for _, opt := range opts {
		opt(cm)
	}

	// Ensure logger is non-nil, defaulting to discard if necessary
	if cm.logger == nil {
		cm.logger = log.New(io.Discard, "", 0)
	}

	return cm
}

// Monitor starts monitoring a specific certificate/key pair identified by `key`.
// If a monitor for `key` already exists, it is stopped and replaced.
// Performs an initial load of the certificate and key pair.
func (cm *CertMonitor) Monitor(key, certFile, keyFile string) error {
	// Initial load check outside the lock
	parsedCert, err := loadKeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("initial load for key '%s' failed (%s, %s): %w", key, certFile, keyFile, err)
	}
	modTime, err := getModTime(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("initial mod time check for key '%s' failed (%s, %s): %w", key, certFile, keyFile, err)
	}

	cm.certsMu.Lock()
	defer cm.certsMu.Unlock()

	// Check if monitor for this key already exists and stop it
	if existing, ok := cm.certs[key]; ok {
		cm.logger.Printf("Replacing existing monitor for key: %s", key)
		existing.cancel()    // Signal the existing goroutine to stop
		existing.wg.Wait()   // Wait for it to fully stop
		cm.wg.Done()         // Decrement global waitgroup count for the old goroutine
		delete(cm.certs, key) // Remove from map (though it will be replaced)
	}

	// Create context for cancellation of the new goroutine
	// Use a context derived from a cancellable parent tied to the *specific* monitor entry
	// NOT derived from the global 'done' channel, because we need per-key cancellation.
	ctx, cancelFunc := cm.newContextWithCancel()

	mc := &monitoredCert{
		certificate:  &parsedCert,
		lastModified: modTime,
		certFile:     certFile,
		keyFile:      keyFile,
		cancel:       cancelFunc,
	}

	cm.certs[key] = mc
	mc.wg.Add(1) // Add to the specific cert's waitgroup
	cm.wg.Add(1) // Add to the global waitgroup

	go cm.runMonitor(ctx, key, mc) // Pass context to the goroutine

	cm.logger.Printf("Started monitoring for key: %s (Cert: %s, Key: %s)", key, certFile, keyFile)
	return nil
}

// newContextWithCancel creates a new context that is cancelled
// when the provided cancel function is called OR when the global
// done channel is closed.
func (cm *CertMonitor) newContextWithCancel() (ctx Ctx, cancel func()) {
	// Create a child context that can be cancelled independently
    parentCtx, cancelFunc := context.WithCancel(context.Background())

	// Goroutine to link global shutdown with this specific context's cancel
	go func() {
		select {
		case <-parentCtx.Done(): // If cancelled locally
			// Do nothing, already handled
		case <-cm.done:         // If global shutdown happens
			cancelFunc() // Trigger this context's cancellation
		}
	}()

	return parentCtx, cancelFunc
}


// GetCertificateFunc returns a function suitable for `tls.Config.GetCertificate`.
// The returned function, when invoked, provides the currently loaded certificate
// associated with the given `key`. It returns an error if the key is not monitored.
func (cm *CertMonitor) GetCertificateFunc(key string) (func(*tls.ClientHelloInfo) (*tls.Certificate, error), error) {
	cm.certsMu.RLock()
	mc, ok := cm.certs[key]
	cm.certsMu.RUnlock() // Release lock before returning the closure

	if !ok {
		return nil, fmt.Errorf("no certificate monitor found for key: %s", key)
	}

	// Return the closure
	return func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		mc.mu.RLock()
		defer mc.mu.RUnlock()
		if mc.certificate == nil {
			// This should ideally not happen if initial load succeeded,
			// but could occur if a reload failed catastrophically
			// and wasn't handled by keeping the old cert.
			return nil, fmt.Errorf("certificate for key '%s' is not currently loaded", key)
		}
		return mc.certificate, nil
	}, nil
}

// Stop gracefully shuts down all active certificate monitoring goroutines.
// It waits for all monitors to complete their shutdown.
func (cm *CertMonitor) Stop() {
	cm.stopOnce.Do(func() {
		cm.logger.Printf("Stopping all certificate monitors...")
		cm.certsMu.Lock() // Lock the map while initiating stop for all
		close(cm.done)   // Signal global shutdown

		// Explicitly cancel each individual monitor's context.
		// While closing cm.done *should* trigger cancellation via newContextWithCancel,
		// being explicit ensures cancellation happens even if that goroutine hasn't run yet.
		for key, mc := range cm.certs {
            cm.logger.Printf("Signalling stop for monitor key: %s", key)
			mc.cancel()
		}
		cm.certsMu.Unlock() // Unlock map before waiting

		cm.logger.Printf("Waiting for all monitors to stop...")
		cm.wg.Wait() // Wait for all runMonitor goroutines to exit
		cm.logger.Printf("All certificate monitors stopped.")
	})
}

// runMonitor is the goroutine responsible for monitoring a single cert pair.
// It takes a context for cancellation.
func (cm *CertMonitor) runMonitor(ctx context.Context, key string, mc *monitoredCert) {
	defer cm.wg.Done()    // Decrement global counter when exiting
	defer mc.wg.Done()   // Decrement specific cert counter when exiting
	defer cm.logger.Printf("Monitor routine for key '%s' stopped.", key) // Log exit

	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.checkAndReload(key, mc)
		case <-ctx.Done(): // Use context for cancellation
			return
		}
	}
}

// checkAndReload checks for modifications and reloads a specific certificate pair.
func (cm *CertMonitor) checkAndReload(key string, mc *monitoredCert) {
	modTime, err := getModTime(mc.certFile, mc.keyFile)
	if err != nil {
		cm.logger.Printf("ERROR (key: %s): Failed check mod time (%s, %s): %v", key, mc.certFile, mc.keyFile, err)
		return // Continue loop, maybe files are temporarily unavailable
	}

	// Acquire read lock only to read lastModified
	mc.mu.RLock()
	lastMod := mc.lastModified
	mc.mu.RUnlock()

	if modTime.After(lastMod) {
		cm.logger.Printf("Detected modification for key '%s' (Cert: %s, Key: %s). Reloading.", key, mc.certFile, mc.keyFile)

		parsedCert, err := loadKeyPair(mc.certFile, mc.keyFile)
		if err != nil {
			cm.logger.Printf("ERROR (key: %s): Failed reload (%s, %s): %v", key, mc.certFile, mc.keyFile, err)
			// Keep the old certificate, but update mod time to prevent spamming reloads
			// on a persistently bad file.
			mc.mu.Lock()
			mc.lastModified = modTime
			mc.mu.Unlock()
			return
		}

		// Acquire write lock to update certificate and lastModified
		mc.mu.Lock()
		mc.certificate = &parsedCert
		mc.lastModified = modTime
		mc.mu.Unlock()
		cm.logger.Printf("Successfully reloaded key '%s' (Cert: %s, Key: %s)", key, mc.certFile, mc.keyFile)
	}
}

// loadKeyPair loads a TLS certificate and key pair from the given files.
func loadKeyPair(certFile, keyFile string) (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("loading key pair failed: %w", err)
	}
	return cert, nil
}

// getModTime returns the latest modification time of the given files.
func getModTime(files ...string) (time.Time, error) {
	if len(files) == 0 {
		return time.Time{}, fmt.Errorf("no files provided to getModTime")
	}
	var latest time.Time

	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			return time.Time{}, fmt.Errorf("stat failed for %q: %w", f, err)
		}
		if info.ModTime().After(latest) {
			latest = info.ModTime()
		}
	}
	return latest, nil
}
