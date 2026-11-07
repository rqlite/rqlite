package store

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/internal/rarchive/zstd"
	"github.com/rqlite/rqlite/v10/internal/rsync"
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
	compressSnap bool
	scratchDir   string // Directory for scratch files used when compressing snapshots.

	aeMu                   sync.RWMutex
	appendEntriesTxHandler func(req *raft.AppendEntriesRequest) error
	appendEntriesRxHandler func(req *raft.AppendEntriesRequest) error

	lastAppendEntriesRxTime rsync.AtomicTime
	commandCommitIndex      atomic.Uint64
	leaderCommitIndex       atomic.Uint64
	done                    chan struct{}
	closeOnce               sync.Once
	logger                  *log.Logger
}

// NewNodeTransport returns an initialized NodeTransport. If compressSnap is
// true, snapshots are compressed before being sent to other nodes, and are
// expected to be compressed when received. scratchDir is where the compressed
// form of an outgoing snapshot is written before it is sent.
func NewNodeTransport(transport *raft.NetworkTransport, compressSnap bool, scratchDir string) *NodeTransport {
	return &NodeTransport{
		NetworkTransport: transport,
		compressSnap:     compressSnap,
		scratchDir:       scratchDir,
		done:             make(chan struct{}),
		logger:           log.New(os.Stderr, "[transport] ", log.LstdFlags),
	}
}

// LastAppendEntriesRxTime returns the last time we received an AppendEntriesRPC.
func (n *NodeTransport) LastAppendEntriesRxTime() time.Time {
	return n.lastAppendEntriesRxTime.Load()
}

// CommandCommitIndex returns the index of the latest committed log entry
// which is applied to the FSM.
func (n *NodeTransport) CommandCommitIndex() uint64 {
	return n.commandCommitIndex.Load()
}

// LeaderCommitIndex returns the index of the latest committed log entry
// which is known to be replicated to the majority of the cluster. This value
// is monotonically increasing. It will never return a value lower than a value
// it returned previously.
func (n *NodeTransport) LeaderCommitIndex() uint64 {
	return n.leaderCommitIndex.Load()
}

// SetAppendEntriesTxHandler sets a callback that is called anytime the NodeTransport
// is used to send an AppendEntries request. The handler is called *before* the request
// is handed off to the Raft library.
func (n *NodeTransport) SetAppendEntriesTxHandler(cb func(req *raft.AppendEntriesRequest) error) {
	n.aeMu.Lock()
	defer n.aeMu.Unlock()
	n.appendEntriesTxHandler = cb
}

// SetAppendEntriesRxHandler sets a callback that is called anytime the NodeTransport
// receives an AppendEntries request. The handler is called *before* the request
// is handed off to the Raft library.
func (n *NodeTransport) SetAppendEntriesRxHandler(cb func(req *raft.AppendEntriesRequest) error) {
	n.aeMu.Lock()
	defer n.aeMu.Unlock()
	n.appendEntriesRxHandler = cb
}

// Close closes the transport
func (n *NodeTransport) Close() error {
	var err error
	n.closeOnce.Do(func() {
		close(n.done)
		if n.NetworkTransport == nil {
			return
		}
		n.NetworkTransport.CloseStreams()
		err = n.NetworkTransport.Close()
	})
	return err
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
//
// When compression is enabled the size sent in the request is that of the
// compressed stream, not of the snapshot. Raft on the receiving side reads
// exactly that many bytes off the connection, so it must describe what is
// actually on the wire. The compressed size is only known once compression is
// complete, so the compressed stream is written to a scratch file first. The
// receiving NodeTransport restores the size of the snapshot itself before
// Raft checks it against the number of bytes it received.
func (n *NodeTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest,
	resp *raft.InstallSnapshotResponse, data io.Reader) error {
	if !n.compressSnap {
		return n.NetworkTransport.InstallSnapshot(id, target, args, resp, data)
	}

	fd, err := createTemp(n.scratchDir, snapTransportScratchPattern)
	if err != nil {
		return err
	}
	defer fsutil.Remove(fd.Name())
	defer fd.Close()

	zstdData, err := zstd.NewCompressor(data, args.Size, zstd.DefaultBufferSize)
	if err != nil {
		return err
	}
	compressedSize, err := io.Copy(fd, zstdData)
	if err != nil {
		return err
	}
	if err := zstdData.Close(); err != nil {
		return err
	}
	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return err
	}

	compressedArgs := *args
	compressedArgs.Size = compressedSize
	return n.NetworkTransport.InstallSnapshot(id, target, &compressedArgs, resp, fd)
}

// AppendEntries sends the appropriate RPC to the target node.
func (n *NodeTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	n.aeMu.RLock()
	defer n.aeMu.RUnlock()
	if n.appendEntriesTxHandler != nil {
		if err := n.appendEntriesTxHandler(args); err != nil {
			return err
		}
	}
	return n.NetworkTransport.AppendEntries(id, target, args, resp)
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
					if rpc.Reader != nil && n.compressSnap {
						// The size in the request is that of the compressed
						// stream, which Raft has used to bound its read of the
						// connection. Raft now needs the size of the snapshot
						// itself, to check that it received all of it.
						dec := zstd.NewDecompressor(rpc.Reader)
						sz, err := dec.UncompressedSize()
						if err != nil {
							rpc.Respond(nil, fmt.Errorf("failed to read compressed snapshot header: %w", err))
							continue
						}
						cmd.Size = sz
						rpc.Reader = &snapshotStream{snapshot: dec, compressed: rpc.Reader}
					}
				case *raft.AppendEntriesRequest:
					n.lastAppendEntriesRxTime.Store(time.Now())

					n.aeMu.RLock()
					handler := n.appendEntriesRxHandler
					n.aeMu.RUnlock()
					if handler != nil {
						if err := handler(cmd); err != nil {
							n.logger.Printf("AppendEntriesRxHandler error: %v", err)
						}
					}
					for _, e := range cmd.Entries {
						if e.Type == raft.LogCommand {
							n.commandCommitIndex.Store(e.Index)
						}
					}
					if cmd.LeaderCommitIndex > n.leaderCommitIndex.Load() {
						// There is a small window where a newly elected Leader's internal state can make the commit
						// index it sends be less than a commit index this node received from the previous leader.
						// It's not a safety issue as logs are always correct underneath. The Raft library has
						// the same checks internally.
						n.leaderCommitIndex.Store(cmd.LeaderCommitIndex)
					}
				case *raft.TimeoutNowRequest:
					n.logger.Printf("TimeoutNowRequest received on this node (%s) from node %s at %s",
						n.LocalAddr(), cmd.ID, cmd.Addr)
				}
				select {
				case ch <- rpc:
				case <-n.done:
					return
				}
			}
		}
	}()
	return ch
}

// snapshotStream is the reader handed to Raft for a compressed snapshot. Raft
// reads the snapshot from it, and then drains it so that the connection is
// left clean for the next RPC. The zstd decoder returns the last of the
// snapshot without reading the trailing bytes of its frame, so once the
// snapshot is exhausted, draining must consume what remains of the compressed
// stream. Raft bounds that stream to the compressed size, so this cannot block.
type snapshotStream struct {
	snapshot   io.Reader // The decompressed snapshot.
	compressed io.Reader // The compressed stream the snapshot is read from.
}

// Read reads the snapshot. Once the snapshot is exhausted, the remainder of
// the compressed stream is consumed and discarded.
func (s *snapshotStream) Read(p []byte) (int, error) {
	n, err := s.snapshot.Read(p)
	if err != io.EOF {
		return n, err
	}
	if _, err := io.Copy(io.Discard, s.compressed); err != nil {
		return n, err
	}
	return n, io.EOF
}

// Stats returns the current stats of the transport.
func (n *NodeTransport) Stats() map[string]any {
	return map[string]any{
		"last_append_entries_rx_time": n.LastAppendEntriesRxTime(),
		"command_commit_index":        n.CommandCommitIndex(),
		"leader_commit_index":         n.LeaderCommitIndex(),
		"compress_snap":               n.compressSnap,
	}
}
