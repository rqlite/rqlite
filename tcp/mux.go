package tcp

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	// DefaultTimeout is the default length of time to wait for first byte.
	DefaultTimeout = 30 * time.Second
)

// stats captures stats for the mux system.
var stats *expvar.Map

const (
	numConnectionsHandled   = "num_connections_handled"
	numUnregisteredHandlers = "num_unregistered_handlers"
)

func init() {
	stats = expvar.NewMap("mux")
	stats.Add(numConnectionsHandled, 0)
	stats.Add(numUnregisteredHandlers, 0)
}

// Layer represents the connection between nodes. It can be both used to
// make connections to other nodes, and receive connections from other
// nodes.
type Layer struct {
	ln   net.Listener
	addr net.Addr

	dialer *Dialer

	nodeX509CACert string
	tlsConfig      *tls.Config
}

// Dial creates a new network connection.
func (l *Layer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return l.dialer.Dial(addr, timeout)
}

// Accept waits for the next connection.
func (l *Layer) Accept() (net.Conn, error) { return l.ln.Accept() }

// Close closes the layer.
func (l *Layer) Close() error { return l.ln.Close() }

// Addr returns the local address for the layer.
func (l *Layer) Addr() net.Addr {
	return l.addr
}

// Mux multiplexes a network connection.
type Mux struct {
	ln   net.Listener
	addr net.Addr
	m    map[byte]*listener

	wg sync.WaitGroup

	remoteEncrypted bool

	// The amount of time to wait for the first header byte.
	Timeout time.Duration

	// Out-of-band error logger
	Logger *log.Logger

	// Path to root X.509 certificate.
	x509CACert string

	// Path to X509 certificate
	x509Cert string

	// Path to X509 key.
	x509Key string

	// Whether to skip verification of other nodes' certificates.
	InsecureSkipVerify bool

	tlsConfig *tls.Config
}

// NewMux returns a new instance of Mux for ln. If adv is nil,
// then the addr of ln is used.
func NewMux(ln net.Listener, adv net.Addr) (*Mux, error) {
	addr := adv
	if addr == nil {
		addr = ln.Addr()
	}

	return &Mux{
		ln:      ln,
		addr:    addr,
		m:       make(map[byte]*listener),
		Timeout: DefaultTimeout,
		Logger:  log.New(os.Stderr, "[mux] ", log.LstdFlags),
	}, nil
}

// NewTLSMux returns a new instance of Mux for ln, and encrypts all traffic
// using TLS. If adv is nil, then the addr of ln is used.
func NewTLSMux(ln net.Listener, adv net.Addr, cert, key, caCert string) (*Mux, error) {
	mux, err := NewMux(ln, adv)
	if err != nil {
		return nil, err
	}

	mux.tlsConfig, err = createTLSConfig(cert, key, caCert)
	if err != nil {
		return nil, err
	}

	mux.ln = tls.NewListener(ln, mux.tlsConfig)
	mux.remoteEncrypted = true
	mux.x509CACert = caCert
	mux.x509Cert = cert
	mux.x509Key = key

	return mux, nil
}

// Serve handles connections from ln and multiplexes then across registered listener.
func (mux *Mux) Serve() error {
	tlsStr := ""
	if mux.tlsConfig != nil {
		tlsStr = "TLS "
	}
	mux.Logger.Printf("%smux serving on %s, advertising %s", tlsStr, mux.ln.Addr().String(), mux.addr)

	for {
		// Wait for the next connection.
		// If it returns a temporary error then simply retry.
		// If it returns any other error then exit immediately.
		conn, err := mux.ln.Accept()
		if err, ok := err.(interface {
			Temporary() bool
		}); ok && err.Temporary() {
			continue
		}
		if err != nil {
			// Wait for all connections to be demuxed
			mux.wg.Wait()
			for _, ln := range mux.m {
				close(ln.c)
			}
			return err
		}

		// Demux in a goroutine to
		mux.wg.Add(1)
		go mux.handleConn(conn)
	}
}

// Stats returns status of the mux.
func (mux *Mux) Stats() (interface{}, error) {
	s := map[string]string{
		"addr":      mux.addr.String(),
		"timeout":   mux.Timeout.String(),
		"encrypted": strconv.FormatBool(mux.remoteEncrypted),
	}

	if mux.remoteEncrypted {
		s["certificate"] = mux.x509Cert
		s["key"] = mux.x509Key
		s["ca_certificate"] = mux.x509CACert
		s["skip_verify"] = strconv.FormatBool(mux.InsecureSkipVerify)
	}

	return s, nil
}

func (mux *Mux) handleConn(conn net.Conn) {
	stats.Add(numConnectionsHandled, 1)

	defer mux.wg.Done()
	// Set a read deadline so connections with no data don't timeout.
	if err := conn.SetReadDeadline(time.Now().Add(mux.Timeout)); err != nil {
		conn.Close()
		mux.Logger.Printf("tcp.Mux: cannot set read deadline: %s", err)
		return
	}

	// Read first byte from connection to determine handler.
	var typ [1]byte
	if _, err := io.ReadFull(conn, typ[:]); err != nil {
		conn.Close()
		mux.Logger.Printf("tcp.Mux: cannot read header byte: %s", err)
		return
	}

	// Reset read deadline and let the listener handle that.
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		conn.Close()
		mux.Logger.Printf("tcp.Mux: cannot reset set read deadline: %s", err)
		return
	}

	// Retrieve handler based on first byte.
	handler := mux.m[typ[0]]
	if handler == nil {
		conn.Close()
		stats.Add(numUnregisteredHandlers, 1)
		mux.Logger.Printf("tcp.Mux: handler not registered for request from %s: %d (unsupported protocol?)",
			conn.RemoteAddr().String(), typ[0])
		return
	}

	// Send connection to handler.  The handler is responsible for closing the connection.
	handler.c <- conn
}

// Listen returns a Layer associated with the given header. Any connection
// accepted by mux is multiplexed based on the initial header byte.
func (mux *Mux) Listen(header byte) *Layer {
	// Ensure two listeners are not created for the same header byte.
	if _, ok := mux.m[header]; ok {
		panic(fmt.Sprintf("listener already registered under header byte: %d", header))
	}

	// Create a new listener and assign it.
	ln := &listener{
		c: make(chan net.Conn),
	}
	mux.m[header] = ln

	layer := &Layer{
		ln:             ln,
		addr:           mux.addr,
		nodeX509CACert: mux.x509CACert,
		tlsConfig:      mux.tlsConfig,
	}
	layer.dialer = NewDialer(header, mux.remoteEncrypted, mux.InsecureSkipVerify)

	return layer
}

// listener is a receiver for connections received by Mux.
type listener struct {
	c chan net.Conn
}

// Accept waits for and returns the next connection to the listener.
func (ln *listener) Accept() (c net.Conn, err error) {
	conn, ok := <-ln.c
	if !ok {
		return nil, errors.New("network connection closed")
	}
	return conn, nil
}

// Close is a no-op. The mux's listener should be closed instead.
func (ln *listener) Close() error { return nil }

// Addr always returns nil
func (ln *listener) Addr() net.Addr { return nil }

// newTLSListener returns a net listener which encrypts the traffic using TLS.
func newTLSListener(ln net.Listener, certFile, keyFile, caCertFile string) (net.Listener, error) {
	config, err := createTLSConfig(certFile, keyFile, caCertFile)
	if err != nil {
		return nil, err
	}

	return tls.NewListener(ln, config), nil
}

// createTLSConfig returns a TLS config from the given cert, key and optionally
// Certificate Authority cert.
func createTLSConfig(certFile, keyFile, caCertFile string) (*tls.Config, error) {
	var err error
	config := &tls.Config{}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	if caCertFile != "" {
		asn1Data, err := ioutil.ReadFile(caCertFile)
		if err != nil {
			return nil, err
		}
		config.RootCAs = x509.NewCertPool()
		ok := config.RootCAs.AppendCertsFromPEM([]byte(asn1Data))
		if !ok {
			return nil, fmt.Errorf("failed to parse root certificate(s) in %q", caCertFile)
		}
	}

	return config, nil
}
