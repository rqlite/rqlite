package tcp

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

// Transport is the network layer for internode communications.
type Transport struct {
	ln net.Listener

	certFile        string // Path to local X.509 cert.
	certKey         string // Path to corresponding X.509 key.
	remoteEncrypted bool   // Remote nodes use encrypted communication.
	skipVerify      bool   // Skip verification of remote node certs.
	srcIP           string // The specified source IP is optional
}

// NewTransport returns an initialized unencrypted Transport.
func NewTransport() *Transport {
	return &Transport{}
}

// NewTLSTransport returns an initialized TLS-encrypted Transport.
func NewTLSTransport(certFile, keyPath string, skipVerify bool) *Transport {
	return &Transport{
		certFile:        certFile,
		certKey:         keyPath,
		remoteEncrypted: true,
		skipVerify:      skipVerify,
	}
}

// Open opens the transport, binding to the supplied address.
func (t *Transport) Open(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	if t.certFile != "" {
		config, err := createTLSConfig(t.certFile, t.certKey)
		if err != nil {
			return err
		}
		ln = tls.NewListener(ln, config)
	}

	t.ln = ln
	return nil
}

// Dial opens a network connection.
func (t *Transport) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	var dialer *net.Dialer
	dialer = &net.Dialer{Timeout: timeout}
	if t.srcIP != "" {
		netAddr := &net.TCPAddr{
			IP:   net.ParseIP(t.srcIP),
			Port: 0,
		}
		dialer = &net.Dialer{Timeout: timeout, LocalAddr: netAddr}
	}

	var err error
	var conn net.Conn
	if t.remoteEncrypted {
		conf := &tls.Config{
			InsecureSkipVerify: t.skipVerify,
		}
		fmt.Println("doing a TLS dial")
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, conf)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}

	return conn, err
}

// Accept waits for the next connection.
func (t *Transport) Accept() (net.Conn, error) {
	c, err := t.ln.Accept()
	if err != nil {
		fmt.Println("error accepting: ", err.Error())
	}
	return c, err
}

// Close closes the transport
func (t *Transport) Close() error {
	if t.ln != nil {
		return t.ln.Close()
	}
	return nil
}

// Addr returns the binding address of the transport.
func (t *Transport) Addr() net.Addr {
	return t.ln.Addr()
}

// createTLSConfig returns a TLS config from the given cert and key.
func createTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	var err error
	config := &tls.Config{}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return config, nil
}
