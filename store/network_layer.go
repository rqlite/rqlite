package store

import (
	"net"
	"time"
)

// networkLayer represents the connection between nodes.
type networkLayer struct {
	ln   net.Listener
	addr net.Addr
}

// newNetworkLayer returns a new instance of networkLayer.
func newNetworkLayer(ln net.Listener, addr net.Addr) *networkLayer {
	return &networkLayer{
		ln:   ln,
		addr: addr,
	}
}

// Addr returns the local address for the layer.
func (l *networkLayer) Addr() net.Addr {
	return l.addr
}

// Dial creates a new network connection.
func (l *networkLayer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}

	// Write a marker byte for raft messages.
	_, err = conn.Write([]byte{muxRaftHeader})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, err
}

// Accept waits for the next connection.
func (l *networkLayer) Accept() (net.Conn, error) { return l.ln.Accept() }

// Close closes the layer.
func (l *networkLayer) Close() error { return l.ln.Close() }
