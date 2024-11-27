package tcp

import (
	"crypto/tls"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v8/rtls"
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
// make connections to other nodes (client), and receive connections from other
// nodes (server)
type Layer struct {
	ln     net.Listener
	addr   net.Addr
	dialer *Dialer
}

// NewLayer returns a new instance of Layer.
func NewLayer(ln net.Listener, dialer *Dialer) *Layer {
	return &Layer{
		ln:     ln,
		addr:   ln.Addr(),
		dialer: dialer,
	}
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

	// The amount of time to wait for the first header byte.
	Timeout time.Duration

	// Out-of-band error logger
	Logger *log.Logger

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
// using TLS. If adv is nil, then the addr of ln is used. If insecure is true,
// then the server will not verify the client's certificate. If mutual is true,
// then the server will require the client to present a trusted certificate.
func NewTLSMux(ln net.Listener, adv net.Addr, cert, key, caCert string, insecure, mutual bool) (*Mux, error) {
	return newTLSMux(ln, adv, cert, key, caCert, false)
}

// NewMutualTLSMux returns a new instance of Mux for ln, and encrypts all traffic
// using TLS. The server will also verify the client's certificate.
func NewMutualTLSMux(ln net.Listener, adv net.Addr, cert, key, caCert string) (*Mux, error) {
	return newTLSMux(ln, adv, cert, key, caCert, true)
}

func newTLSMux(ln net.Listener, adv net.Addr, cert, key, caCert string, mutual bool) (*Mux, error) {
	mux, err := NewMux(ln, adv)
	if err != nil {
		return nil, err
	}

	mtlsState := rtls.MTLSStateDisabled
	if mutual {
		mtlsState = rtls.MTLSStateEnabled
	}
	mux.tlsConfig, err = rtls.CreateServerConfig(cert, key, caCert, mtlsState)
	if err != nil {
		return nil, fmt.Errorf("cannot create TLS config: %s", err)
	}

	mux.ln = tls.NewListener(ln, mux.tlsConfig)
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
func (mux *Mux) Stats() (map[string]interface{}, error) {
	e := "disabled"
	if mux.tlsConfig != nil {
		e = "enabled"
	}
	handlers := []byte{}
	for k := range mux.m {
		handlers = append(handlers, k)
	}
	return map[string]interface{}{
		"addr":     mux.addr.String(),
		"timeout":  mux.Timeout.String(),
		"tls":      e,
		"handlers": string(handlers),
	}, nil
}

// Listen returns a Listener associated with the given header. Any connection
// accepted by mux is multiplexed based on the initial header byte.
func (mux *Mux) Listen(header byte) net.Listener {
	// Ensure two listeners are not created for the same header byte.
	if _, ok := mux.m[header]; ok {
		panic(fmt.Sprintf("listener already registered under header byte: %d", header))
	}

	// Create a new listener and assign it.
	ln := &listener{
		c:    make(chan net.Conn),
		addr: mux.addr,
	}
	mux.m[header] = ln
	return ln
}

func (mux *Mux) handleConn(conn net.Conn) {
	stats.Add(numConnectionsHandled, 1)

	defer mux.wg.Done()
	// Set a read deadline so connections with no data don't timeout.
	if err := conn.SetReadDeadline(time.Now().Add(mux.Timeout)); err != nil {
		conn.Close()
		mux.Logger.Printf("cannot set read deadline: %s", err)
		return
	}

	// Read first byte from connection to determine handler.
	var typ [1]byte
	if _, err := io.ReadFull(conn, typ[:]); err != nil {
		conn.Close()
		mux.Logger.Printf("cannot read header byte: %s", err)
		return
	}

	// Reset read deadline and let the listener handle that.
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		conn.Close()
		mux.Logger.Printf("cannot reset set read deadline: %s", err)
		return
	}

	// Retrieve handler based on first byte.
	handler := mux.m[typ[0]]
	if handler == nil {
		conn.Close()
		stats.Add(numUnregisteredHandlers, 1)
		return
	}

	// Send connection to handler.  The handler is responsible for closing the connection.
	handler.c <- conn
}

// listener is a receiver for connections received by Mux.
type listener struct {
	c    chan net.Conn
	addr net.Addr
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
func (ln *listener) Addr() net.Addr { return ln.addr }
