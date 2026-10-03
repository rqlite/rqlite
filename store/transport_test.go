package store

import (
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func Test_NewTransport(t *testing.T) {
	if NewTransport(nil) == nil {
		t.Fatal("failed to create new Transport")
	}
}

func Test_NewNodeTransport(t *testing.T) {
	nt := NewNodeTransport(nil, false)
	if nt == nil {
		t.Fatal("failed to create new NodeTransport")
	}
	if err := nt.Close(); err != nil {
		t.Fatalf("failed to close NodeTransport: %s", err.Error())
	}
	if err := nt.Close(); err != nil {
		t.Fatalf("failed to double-close NodeTransport: %s", err.Error())
	}
}

func Test_NodeTransport_RecordAppendEntries(t *testing.T) {
	nt := NewNodeTransport(nil, false)
	defer nt.Close()

	if !nt.LastAppendEntriesRxTime().IsZero() {
		t.Fatalf("expected zero AppendEntries rx time")
	}
	if got := nt.LeaderCommitIndex(); got != 0 {
		t.Fatalf("expected Leader commit index of 0, got %d", got)
	}
	if got := nt.CommandCommitIndex(); got != 0 {
		t.Fatalf("expected command commit index of 0, got %d", got)
	}

	// Entries, with a non-command entry at the highest index.
	t1 := time.Now()
	nt.recordAppendEntries(&raft.AppendEntriesRequest{
		LeaderCommitIndex: 10,
		Entries: []*raft.Log{
			{Index: 11, Type: raft.LogCommand},
			{Index: 12, Type: raft.LogCommand},
			{Index: 13, Type: raft.LogNoop},
		},
	}, t1)
	if got := nt.LastAppendEntriesRxTime(); !got.Equal(t1) {
		t.Fatalf("expected AppendEntries rx time of %v, got %v", t1, got)
	}
	if got := nt.LeaderCommitIndex(); got != 10 {
		t.Fatalf("expected Leader commit index of 10, got %d", got)
	}
	if got := nt.CommandCommitIndex(); got != 12 {
		t.Fatalf("expected command commit index of 12, got %d", got)
	}

	// No entries, but the commit index advances. This is what the Leader sends
	// every CommitTimeout when there is nothing new to replicate.
	t2 := t1.Add(time.Second)
	nt.recordAppendEntries(&raft.AppendEntriesRequest{
		LeaderCommitIndex: 13,
	}, t2)
	if got := nt.LastAppendEntriesRxTime(); !got.Equal(t2) {
		t.Fatalf("expected AppendEntries rx time of %v, got %v", t2, got)
	}
	if got := nt.LeaderCommitIndex(); got != 13 {
		t.Fatalf("expected Leader commit index of 13, got %d", got)
	}
	if got := nt.CommandCommitIndex(); got != 12 {
		t.Fatalf("expected command commit index of 12, got %d", got)
	}

	// A lower commit index, as a newly elected Leader may send, is ignored
	// but still counts as an AppendEntries request.
	t3 := t2.Add(time.Second)
	nt.recordAppendEntries(&raft.AppendEntriesRequest{
		LeaderCommitIndex: 11,
	}, t3)
	if got := nt.LastAppendEntriesRxTime(); !got.Equal(t3) {
		t.Fatalf("expected AppendEntries rx time of %v, got %v", t3, got)
	}
	if got := nt.LeaderCommitIndex(); got != 13 {
		t.Fatalf("expected Leader commit index to remain 13, got %d", got)
	}
}

// Test_NodeTransport_HeartbeatFastPath checks that heartbeats are dispatched
// via the heartbeat handler and never reach Consumer(), while AppendEntries
// requests carrying a commit index do. The stale-read check relies on this:
// LastAppendEntriesRxTime() must not be refreshed by heartbeats.
func Test_NodeTransport_HeartbeatFastPath(t *testing.T) {
	recvNT, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, io.Discard)
	if err != nil {
		t.Fatalf("failed to create receiving transport: %s", err)
	}
	recv := NewNodeTransport(recvNT, false)
	defer recv.Close()

	send, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, io.Discard)
	if err != nil {
		t.Fatalf("failed to create sending transport: %s", err)
	}
	defer send.Close()

	heartbeatCh := make(chan struct{}, 1)
	recv.SetHeartbeatHandler(func(rpc raft.RPC) {
		rpc.Respond(&raft.AppendEntriesResponse{Success: true}, nil)
		heartbeatCh <- struct{}{}
	})

	consumeCh := make(chan *raft.AppendEntriesRequest, 1)
	ch := recv.Consumer()
	go func() {
		for rpc := range ch {
			rpc.Respond(&raft.AppendEntriesResponse{Success: true}, nil)
			if req, ok := rpc.Command.(*raft.AppendEntriesRequest); ok {
				consumeCh <- req
			}
		}
	}()

	header := raft.RPCHeader{
		ID:   []byte("leader"),
		Addr: []byte(send.LocalAddr()),
	}

	// A heartbeat, exactly as hashicorp/raft constructs one.
	var resp raft.AppendEntriesResponse
	if err := send.AppendEntries("follower", recv.LocalAddr(), &raft.AppendEntriesRequest{
		RPCHeader: header,
		Term:      1,
	}, &resp); err != nil {
		t.Fatalf("failed to send heartbeat: %s", err)
	}
	select {
	case <-heartbeatCh:
	case req := <-consumeCh:
		t.Fatalf("heartbeat reached Consumer: %+v", req)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for heartbeat")
	}
	if !recv.LastAppendEntriesRxTime().IsZero() {
		t.Fatalf("heartbeat updated AppendEntries rx time")
	}

	// An AppendEntries request with no entries but a commit index, as the Leader
	// sends every CommitTimeout.
	if err := send.AppendEntries("follower", recv.LocalAddr(), &raft.AppendEntriesRequest{
		RPCHeader:         header,
		Term:              1,
		PrevLogEntry:      5,
		PrevLogTerm:       1,
		LeaderCommitIndex: 5,
	}, &resp); err != nil {
		t.Fatalf("failed to send AppendEntries: %s", err)
	}
	select {
	case req := <-consumeCh:
		if req.LeaderCommitIndex != 5 {
			t.Fatalf("expected Leader commit index of 5, got %d", req.LeaderCommitIndex)
		}
	case <-heartbeatCh:
		t.Fatal("AppendEntries request was handled as a heartbeat")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for AppendEntries request")
	}
	if recv.LastAppendEntriesRxTime().IsZero() {
		t.Fatalf("AppendEntries request did not update AppendEntries rx time")
	}
	if got := recv.LeaderCommitIndex(); got != 5 {
		t.Fatalf("expected Leader commit index of 5, got %d", got)
	}
}
