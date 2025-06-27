package cdc

import (
	"expvar"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

func Test_ServiceSingleEvent(t *testing.T) {
	ResetStats()

	// Channel for the service to receive events.
	eventsCh := make(chan *proto.CDCEvents, 1)

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
		Index:  1,
		Events: []*proto.CDCEvent{ev},
	}
	eventsCh <- evs

	// Wait for the service to forward the batch.
	select {
	case got := <-bodyCh:
		var batch proto.CDCEventsBatch
		if err := protojson.Unmarshal(got, &batch); err != nil {
			t.Fatalf("invalid JSON received: %v", err)
		}
		if len(batch.Payload) != 1 || batch.Payload[0].Index != evs.Index {
			t.Fatalf("unexpected payload: %v", batch.Payload)
		}
		if len(batch.Payload[0].Events) != 1 {
			t.Fatalf("unexpected number of events in payload: %d", len(batch.Payload[0].Events))
		}
		if reflect.DeepEqual(batch.Payload[0].Events[0], evs.Events[0]) == false {
			t.Fatalf("unexpected events in payload: %v", batch.Payload[0].Events)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for HTTP POST")
	}

	// Next emulate CDC not running on the Leader.
	cl.leader.Store(false)
	eventsCh <- evs
	pollExpvarUntil(t, numDroppedNotLeader, 1, 2*time.Second)
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

func pollExpvarUntil(t *testing.T, name string, expected int64, timeout time.Duration) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			val := stats.Get(name)
			if val == nil {
				t.Fatalf("expvar %s not found", name)
			}
			if i, ok := val.(*expvar.Int); ok && i.Value() == expected {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for expvar %s to reach %d", name, expected)
		}

	}
}
