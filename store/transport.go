package store

import (
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

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
	return t.ln.Addr()
}

// NodeTransport is a wrapper around the Raft NetworkTransport, which allows
// custom configuration of the InstallSnapshot method.
type NodeTransport struct {
	*raft.NetworkTransport
}

// NewNodeTransport returns an initialized NodeTransport.
func NewNodeTransport(transport *raft.NetworkTransport) *NodeTransport {
	return &NodeTransport{
		NetworkTransport: transport,
	}
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (n *NodeTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest,
	resp *raft.InstallSnapshotResponse, data io.Reader) error {
	return n.NetworkTransport.InstallSnapshot(id, target, args, resp, data)
}

// Consumer returns a channel of RPC requests to be consumed.
func (n *NodeTransport) Consumer() <-chan raft.RPC {
	return n.NetworkTransport.Consumer()
}
