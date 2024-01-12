package store

import (
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/store/gzip"
)

// Layer is the interface expected by the Store for network communication
// between nodes, which is used for Raft distributed consensus.
type Layer interface {
	net.Listener
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// Transport is the network service provided to Raft, and wraps a Listener.
type Transport struct {
	ly Layer
}

// NewTransport returns an initialized Transport.
func NewTransport(ly Layer) *Transport {
	return &Transport{
		ly: ly,
	}
}

// Dial creates a new network connection.
func (t *Transport) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return t.ly.Dial(string(addr), timeout)
}

// Accept waits for the next connection.
func (t *Transport) Accept() (net.Conn, error) {
	return t.ly.Accept()
}

// Close closes the transport
func (t *Transport) Close() error {
	return t.ly.Close()
}

// Addr returns the binding address of the transport.
func (t *Transport) Addr() net.Addr {
	return t.ly.Addr()
}

// NodeTransport is a wrapper around the Raft NetworkTransport, which allows
// custom configuration of the InstallSnapshot method.
type NodeTransport struct {
	*raft.NetworkTransport
	done   chan struct{}
	closed bool
}

// NewNodeTransport returns an initialized NodeTransport.
func NewNodeTransport(transport *raft.NetworkTransport) *NodeTransport {
	return &NodeTransport{
		NetworkTransport: transport,
		done:             make(chan struct{}),
	}
}

// Close closes the transport
func (n *NodeTransport) Close() error {
	if n.closed {
		return nil
	}
	n.closed = true

	close(n.done)
	if n.NetworkTransport == nil {
		return nil
	}
	return n.NetworkTransport.Close()
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (n *NodeTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest,
	resp *raft.InstallSnapshotResponse, data io.Reader) error {
	gzipData, err := gzip.NewCompressor(data, gzip.DefaultBufferSize)
	if err != nil {
		return err
	}
	defer gzipData.Close()
	return n.NetworkTransport.InstallSnapshot(id, target, args, resp, gzipData)
}

// Consumer returns a channel of RPC requests to be consumed.
func (n *NodeTransport) Consumer() <-chan raft.RPC {
	ch := make(chan raft.RPC)
	srcCh := n.NetworkTransport.Consumer()
	go func() {
		for {
			select {
			case <-n.done:
				return
			case rpc := <-srcCh:
				if rpc.Reader != nil {
					rpc.Reader = gzip.NewDecompressor(rpc.Reader)
				}
				ch <- rpc
			}
		}
	}()
	return ch
}
