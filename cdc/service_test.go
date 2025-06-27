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

	testPoll(t, func() bool {
		return svc.HighWatermark() == evs.Index
	}, 2*time.Second)

	// Next emulate CDC not running on the Leader.
	cl.leader.Store(false)
	eventsCh <- evs
	pollExpvarUntil(t, numDroppedNotLeader, 1, 2*time.Second)
}

func Test_ServiceMultiEvent(t *testing.T) {
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
		nil, // no TLS
		2,   // maxBatchSz – flush after 2 events
		1*time.Second,
	)
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()

	// Create the Events and send them.
	ev1 := &proto.CDCEvent{
		Op:       proto.CDCEvent_INSERT,
		Table:    "foo",
		NewRowId: 10,
	}
	evs1 := &proto.CDCEvents{
		Index:  1,
		Events: []*proto.CDCEvent{ev1},
	}
	ev2 := &proto.CDCEvent{
		Op:       proto.CDCEvent_UPDATE,
		Table:    "baz",
		OldRowId: 20,
		NewRowId: 30,
	}
	evs2 := &proto.CDCEvents{
		Index:  2,
		Events: []*proto.CDCEvent{ev2},
	}
	eventsCh <- evs1
	eventsCh <- evs2

	// Wait for the service to forward the batch.
	select {
	case got := <-bodyCh:
		var batch proto.CDCEventsBatch
		if err := protojson.Unmarshal(got, &batch); err != nil {
			t.Fatalf("invalid JSON received: %v", err)
		}
		if len(batch.Payload) != 2 {
			t.Fatalf("unexpected payload length: %d", len(batch.Payload))
		}
		if batch.Payload[0].Index != evs1.Index || batch.Payload[1].Index != evs2.Index {
			t.Fatalf("unexpected payload indices: %d, %d", batch.Payload[0].Index, batch.Payload[1].Index)
		}
		if len(batch.Payload[0].Events) != 1 || len(batch.Payload[1].Events) != 1 {
			t.Fatalf("unexpected number of events in payload: %d, %d", len(batch.Payload[0].Events), len(batch.Payload[1].Events))
		}
		if !reflect.DeepEqual(batch.Payload[0].Events[0], evs1.Events[0]) || !reflect.DeepEqual(batch.Payload[1].Events[0], evs2.Events[0]) {
			t.Fatalf("unexpected events in payload: %v, %v", batch.Payload[0].Events[0], batch.Payload[1].Events[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for HTTP POST")
	}

	testPoll(t, func() bool {
		return svc.HighWatermark() == evs2.Index
	}, 2*time.Second)
}

func Test_ServiceMultiEvent_Batch(t *testing.T) {
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
		nil, // no TLS
		2,   // maxBatchSz – flush after 2 events
		100*time.Millisecond,
	)
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()

	// Create the Events and send them.
	ev1 := &proto.CDCEvent{
		Op:       proto.CDCEvent_INSERT,
		Table:    "foo",
		NewRowId: 10,
	}
	evs1 := &proto.CDCEvents{
		Index:  1,
		Events: []*proto.CDCEvent{ev1},
	}
	ev2 := &proto.CDCEvent{
		Op:       proto.CDCEvent_UPDATE,
		Table:    "baz",
		OldRowId: 20,
		NewRowId: 30,
	}
	evs2 := &proto.CDCEvents{
		Index:  2,
		Events: []*proto.CDCEvent{ev2},
	}
	ev2 = &proto.CDCEvent{
		Op:       proto.CDCEvent_DELETE,
		Table:    "qux",
		OldRowId: 40,
	}
	evs3 := &proto.CDCEvents{
		Index:  3,
		Events: []*proto.CDCEvent{ev2},
	}
	eventsCh <- evs1
	eventsCh <- evs2
	eventsCh <- evs3

	// Wait for the service to forward the first batch.
	select {
	case got := <-bodyCh:
		var batch proto.CDCEventsBatch
		if err := protojson.Unmarshal(got, &batch); err != nil {
			t.Fatalf("invalid JSON received: %v", err)
		}
		if len(batch.Payload) != 2 {
			t.Fatalf("unexpected payload length: %d", len(batch.Payload))
		}
		if batch.Payload[0].Index != evs1.Index || batch.Payload[1].Index != evs2.Index {
			t.Fatalf("unexpected payload indices: %d, %d", batch.Payload[0].Index, batch.Payload[1].Index)
		}
		if len(batch.Payload[0].Events) != 1 || len(batch.Payload[1].Events) != 1 {
			t.Fatalf("unexpected number of events in payload: %d, %d", len(batch.Payload[0].Events), len(batch.Payload[1].Events))
		}
		if !reflect.DeepEqual(batch.Payload[0].Events[0], evs1.Events[0]) || !reflect.DeepEqual(batch.Payload[1].Events[0], evs2.Events[0]) {
			t.Fatalf("unexpected events in payload: %v, %v", batch.Payload[0].Events[0], batch.Payload[1].Events[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for HTTP POST")
	}

	// Wait for the service to forward the second batch, which will be kicked out due to a timeout.
	select {
	case got := <-bodyCh:
		var batch proto.CDCEventsBatch
		if err := protojson.Unmarshal(got, &batch); err != nil {
			t.Fatalf("invalid JSON received: %v", err)
		}
		if len(batch.Payload) != 1 {
			t.Fatalf("unexpected payload length: %d", len(batch.Payload))
		}
		if batch.Payload[0].Index != evs3.Index {
			t.Fatalf("unexpected payload index: %d", batch.Payload[0].Index)
		}
		if len(batch.Payload[0].Events) != 1 {
			t.Fatalf("unexpected number of events in payload: %d", len(batch.Payload[0].Events))
		}
		if !reflect.DeepEqual(batch.Payload[0].Events[0], evs3.Events[0]) {
			t.Fatalf("unexpected events in payload: %v", batch.Payload[0].Events[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for HTTP POST")
	}

	testPoll(t, func() bool {
		return svc.HighWatermark() == evs3.Index
	}, 2*time.Second)
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

func testPoll(t *testing.T, condition func() bool, timeout time.Duration) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			if condition() {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for condition")
		}
	}
}
