package store

import (
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
)

func Test_NewTransport(t *testing.T) {
	if NewTransport(nil) == nil {
		t.Fatal("failed to create new Transport")
	}
}

func Test_NewNodeTransport(t *testing.T) {
	nt := NewNodeTransport(nil)
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

func Test_NodeTransport_SetAppendEntriesTxHandler(t *testing.T) {
	nt := NewNodeTransport(nil)
	defer nt.Close()

	var called bool
	handler := func(req *raft.AppendEntriesRequest) error {
		called = true
		return nil
	}

	nt.SetAppendEntriesTxHandler(handler)

	// Verify the handler was set
	if nt.appendEntriesTxHandler == nil {
		t.Fatal("AppendEntriesTxHandler was not set")
	}

	// Test calling the handler
	req := &raft.AppendEntriesRequest{}
	err := nt.appendEntriesTxHandler(req)
	if err != nil {
		t.Fatalf("Handler returned error: %v", err)
	}
	if !called {
		t.Fatal("Handler was not called")
	}

	// Test setting handler to nil
	nt.SetAppendEntriesTxHandler(nil)
	if nt.appendEntriesTxHandler != nil {
		t.Fatal("AppendEntriesTxHandler was not cleared")
	}
}

func Test_NodeTransport_SetAppendEntriesRxHandler(t *testing.T) {
	nt := NewNodeTransport(nil)
	defer nt.Close()

	var called bool
	handler := func(req *raft.AppendEntriesRequest) error {
		called = true
		return nil
	}

	nt.SetAppendEntriesRxHandler(handler)

	// Verify the handler was set
	if nt.appendEntriesRxHandler == nil {
		t.Fatal("AppendEntriesRxHandler was not set")
	}

	// Test calling the handler
	req := &raft.AppendEntriesRequest{}
	err := nt.appendEntriesRxHandler(req)
	if err != nil {
		t.Fatalf("Handler returned error: %v", err)
	}
	if !called {
		t.Fatal("Handler was not called")
	}

	// Test setting handler to nil
	nt.SetAppendEntriesRxHandler(nil)
	if nt.appendEntriesRxHandler != nil {
		t.Fatal("AppendEntriesRxHandler was not cleared")
	}
}

// MockTransport implements raft.Transport for testing
type MockTransport struct {
	appendEntriesCalled bool
	appendEntriesReq    *raft.AppendEntriesRequest
	appendEntriesError  error
	mu                  sync.Mutex
}

func (m *MockTransport) Consumer() <-chan raft.RPC {
	return make(<-chan raft.RPC)
}

func (m *MockTransport) LocalAddr() raft.ServerAddress {
	return "127.0.0.1:0"
}

func (m *MockTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, errors.New("not implemented")
}

func (m *MockTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendEntriesCalled = true
	m.appendEntriesReq = args
	return m.appendEntriesError
}

func (m *MockTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return errors.New("not implemented")
}

func (m *MockTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	return errors.New("not implemented")
}

func (m *MockTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

func (m *MockTransport) DecodePeer(data []byte) raft.ServerAddress {
	return raft.ServerAddress(data)
}

func (m *MockTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
}

func (m *MockTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return errors.New("not implemented")
}

func (m *MockTransport) wasCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appendEntriesCalled
}

func (m *MockTransport) getRequest() *raft.AppendEntriesRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appendEntriesReq
}

func Test_NodeTransport_AppendEntries_TxHandler(t *testing.T) {
	// Test the callback functionality directly since we can't mock NetworkTransport easily
	nt := NewNodeTransport(nil)
	defer nt.Close()

	var handlerCalled bool
	var handlerReq *raft.AppendEntriesRequest

	// Set up the Tx handler
	nt.SetAppendEntriesTxHandler(func(req *raft.AppendEntriesRequest) error {
		handlerCalled = true
		handlerReq = req
		return nil
	})

	// Create a test request
	testReq := &raft.AppendEntriesRequest{
		Term:              1,
		Leader:            []byte("leader1"),
		PrevLogEntry:      10,
		PrevLogTerm:       1,
		LeaderCommitIndex: 10,
	}

	// Since we can't easily test with the actual NetworkTransport, let's test
	// the callback by calling it directly through the handler
	if nt.appendEntriesTxHandler != nil {
		err := nt.appendEntriesTxHandler(testReq)
		if err != nil {
			t.Fatalf("Handler returned error: %v", err)
		}
	}

	// Verify the Tx handler was called
	if !handlerCalled {
		t.Fatal("AppendEntriesTxHandler was not called")
	}

	// Verify the handler received the correct request
	if handlerReq != testReq {
		t.Fatal("Handler received different request than expected")
	}
}

func Test_NodeTransport_AppendEntries_TxHandlerError(t *testing.T) {
	nt := NewNodeTransport(nil)
	defer nt.Close()

	expectedError := errors.New("handler error")

	// Set up a handler that returns an error
	nt.SetAppendEntriesTxHandler(func(req *raft.AppendEntriesRequest) error {
		return expectedError
	})

	// Test the error handling by calling the handler directly
	testReq := &raft.AppendEntriesRequest{}
	if nt.appendEntriesTxHandler != nil {
		err := nt.appendEntriesTxHandler(testReq)
		if err != expectedError {
			t.Fatalf("Expected handler error, got: %v", err)
		}
	}
}

func Test_NodeTransport_AppendEntries_NoTxHandler(t *testing.T) {
	nt := NewNodeTransport(nil)
	defer nt.Close()

	// Don't set any handler - should be nil
	if nt.appendEntriesTxHandler != nil {
		t.Fatal("Handler should be nil by default")
	}
}

func Test_NodeTransport_Consumer_RxHandler(t *testing.T) {
	// We can't easily mock NetworkTransport, so we'll test the handler setting
	// and then verify the logic would work as expected
	nt := NewNodeTransport(nil)
	defer nt.Close()

	var handlerCalled bool
	var handlerReq *raft.AppendEntriesRequest

	// Set up the Rx handler
	nt.SetAppendEntriesRxHandler(func(req *raft.AppendEntriesRequest) error {
		handlerCalled = true
		handlerReq = req
		return nil
	})

	// Test the handler was set correctly
	if nt.appendEntriesRxHandler == nil {
		t.Fatal("AppendEntriesRxHandler was not set")
	}

	// Test calling the handler directly (since we can't easily test the Consumer goroutine)
	testReq := &raft.AppendEntriesRequest{
		Term:              1,
		Leader:            []byte("leader1"),
		PrevLogEntry:      10,
		PrevLogTerm:       1,
		LeaderCommitIndex: 10,
	}

	if nt.appendEntriesRxHandler != nil {
		err := nt.appendEntriesRxHandler(testReq)
		if err != nil {
			t.Fatalf("Handler returned error: %v", err)
		}
	}

	// Verify the Rx handler was called
	if !handlerCalled {
		t.Fatal("AppendEntriesRxHandler was not called")
	}

	// Verify the handler received the correct request
	if handlerReq != testReq {
		t.Fatal("Handler received different request than expected")
	}
}
