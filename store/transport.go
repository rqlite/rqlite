package store

import (
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// Transport is the interface the network service must provide.
type Transport interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// raftTransport takes a Transport and makes it suitable for use by the Raft
// networking system.
type raftTransport struct {
	tn Transport
}

func (r *raftTransport) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return r.tn.Dial(string(address), timeout)
}

func (r *raftTransport) Accept() (net.Conn, error) {
	return r.tn.Accept()
}

func (r *raftTransport) Addr() net.Addr {
	return r.tn.Addr()
}

func (r *raftTransport) Close() error {
	return r.tn.Close()
}
