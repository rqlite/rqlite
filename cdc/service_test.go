package cdc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
)

func Test_ServiceSingleEvent(t *testing.T) {
	// Channel for the service to receive events.
	eventsCh := make(chan *proto.CDCEvents, 1)

	// Capture the POST body sent by the service.
	bodyCh := make(chan []byte, 1)
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		bodyCh <- b
		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	// Mock cluster – this node is always leader.
	cl := &mockCluster{}
	cl.leader.Store(true)

	// Construct and start the service.
	svc := NewService(
		cl,
		&mockStore{},
		eventsCh,
		testSrv.URL,
		nil,                 // no TLS
		1,                   // maxBatchSz – flush immediately
		50*time.Millisecond, // maxBatchDelay – short for test
	)
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()

	// Send one dummy event to the service.
	ev := &proto.CDCEvent{
		Op:       proto.CDCEvent_INSERT,
		Table:    "foo",
		NewRowId: 2,
	}
	evs := &proto.CDCEvents{
		K:      1,
		Events: []*proto.CDCEvent{ev},
	}
	eventsCh <- evs

	// Wait for the service to forward the batch.
	select {
	case got := <-bodyCh:
		var batch []*proto.CDCEvents
		if err := json.Unmarshal(got, &batch); err != nil {
			t.Fatalf("invalid JSON received: %v", err)
		}
		if len(batch) != 1 || batch[0].K != evs.K {
			t.Fatalf("unexpected payload: %v", batch)
		}
		fmt.Println(string(got))
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for HTTP POST")
	}
}

type mockCluster struct {
	leader atomic.Bool
}

func (m *mockCluster) IsLeader() bool { return m.leader.Load() }

func (m *mockCluster) RegisterLeaderChange(chan<- struct{}) {
	// Not needed for this simple test.
}

type mockStore struct{}

func (m *mockStore) Execute(*proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error) {
	return nil, nil
}
func (m *mockStore) Query(*proto.QueryRequest) ([]*proto.QueryRows, error) { return nil, nil }
