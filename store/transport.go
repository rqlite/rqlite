package store

import (
	"io"
	"net"
	"sync/atomic"
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
	commandCommitIndex *atomic.Uint64
	leaderCommitIndex  *atomic.Uint64
	done               chan struct{}
	closed             bool
}

// NewNodeTransport returns an initialized NodeTransport.
func NewNodeTransport(transport *raft.NetworkTransport) *NodeTransport {
	return &NodeTransport{
		NetworkTransport:   transport,
		commandCommitIndex: &atomic.Uint64{},
		leaderCommitIndex:  &atomic.Uint64{},
		done:               make(chan struct{}),
	}
}

// CommandCommitIndex returns the index of the latest committed log entry
// which is applied to the FSM.
func (n *NodeTransport) CommandCommitIndex() uint64 {
	return n.commandCommitIndex.Load()
}

// LeaderCommitIndex returns the index of the latest committed log entry
// which is known to be replicated to the majority of the cluster.
func (n *NodeTransport) LeaderCommitIndex() uint64 {
	return n.leaderCommitIndex.Load()
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
	n.NetworkTransport.CloseStreams()
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
				switch cmd := rpc.Command.(type) {
				case *raft.InstallSnapshotRequest:
					if rpc.Reader != nil {
						rpc.Reader = gzip.NewDecompressor(rpc.Reader)
					}
				case *raft.AppendEntriesRequest:
					for _, e := range cmd.Entries {
						if e.Type == raft.LogCommand {
							n.commandCommitIndex.Store(e.Index)
						}
					}
					n.leaderCommitIndex.Store(cmd.LeaderCommitIndex)
				}
				ch <- rpc
			}
		}
	}()
	return ch
}

// Stats returns the current stats of the transport.
func (n *NodeTransport) Stats() map[string]interface{} {
	return map[string]interface{}{
		"command_commit_index": n.CommandCommitIndex(),
		"leader_commit_index":  n.LeaderCommitIndex(),
	}
}
