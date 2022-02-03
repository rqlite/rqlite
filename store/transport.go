package store

import (
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type hAddr struct {
	addr string
}

func (h *hAddr) Network() string {
	return "tcp"
}

func (h *hAddr) String() string {
	return h.addr
}

// Listener is the interface expected by the Store for Transports.
type Listener interface {
	net.Listener
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// Transport is the network service provided to Raft, and wraps a Listener.
type Transport struct {
	ln Listener
}

// NewTransport returns an initialized Transport.
func NewTransport(ln Listener) *Transport {
	return &Transport{
		ln: ln,
	}
}

// Dial creates a new network connection.
func (t *Transport) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return t.ln.Dial(string(addr), timeout)
}

// Accept waits for the next connection.
func (t *Transport) Accept() (net.Conn, error) {
	return t.ln.Accept()
}

// Close closes the transport
func (t *Transport) Close() error {
	return t.ln.Close()
}

// Addr returns the binding address of the transport.
func (t *Transport) Addr() net.Addr {
	_, port, _ := net.SplitHostPort(t.ln.Addr().String())
	return &hAddr{fmt.Sprintf("localhost:%s", port)}
}
