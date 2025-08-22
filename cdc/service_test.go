package cdc

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
)

func Test_ServiceSingleEvent(t *testing.T) {
	ResetStats()

	// Channel for the service to receive events.
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 1)

	bodyCh := make(chan []byte, 1)
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		bodyCh <- b
		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	cl := &mockCluster{}

	cfg := DefaultConfig()
	cfg.Endpoint = testSrv.URL
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond
	svc, err := NewService(
		"node1",
		t.TempDir(),
		cl,
		eventsCh,
		cfg,
	)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()

	// Make it the leader.
	cl.SetLeader(0)

	// Send one dummy event to the service.
	ev := &proto.CDCEvent{
		Op:       proto.CDCEvent_INSERT,
		Table:    "foo",
		NewRowId: 2,
	}
	evs := &proto.CDCIndexedEventGroup{
		Index:  66,
		Events: []*proto.CDCEvent{ev},
	}

	// Test function which waits for the service to forward events. If duration is zero
	// then the test will fail if any events are forwarded within the duration.
	waitFn := func(dur time.Duration, expCount int) {
		n := 0
		select {
		case got := <-bodyCh:
			if expCount == 0 {
				t.Fatalf("unexpected HTTP POST received: %s", got)
			}
			n++
			exp := &CDCMessagesEnvelope{
				NodeID: "node1",
				Payload: []*CDCMessage{
					{
						Index: evs.Index,
						Events: []*CDCMessageEvent{
							{
								Op:       ev.Op.String(),
								Table:    ev.Table,
								NewRowId: ev.NewRowId,
								OldRowId: ev.OldRowId,
							},
						},
					},
				},
			}
			msg := &CDCMessagesEnvelope{}
			if err := UnmarshalFromEnvelopeJSON(got, msg); err != nil {
				t.Fatalf("invalid JSON received: %v", err)
			}
			if reflect.DeepEqual(msg, exp) == false {
				t.Fatalf("unexpected payload: got %v, want %v", msg, exp)
			}
			if n == expCount {
				return // Expected number of events received.
			}
		case <-time.After(dur):
			if expCount > 0 {
				t.Fatalf("timeout waiting for HTTP POST")
			}
		}
	}

	eventsCh <- evs
	waitFn(1*time.Second, 1)

	testPoll(t, func() bool {
		return svc.HighWatermark() == evs.Index
	}, 2*time.Second)

	// Next emulate CDC not running on the Leader.
	cl.SetLeader(-1)
	testPoll(t, func() bool { return !svc.IsLeader() }, 2*time.Second)

	// Send events, and make sure they are ignored.
	evs.Index = 67
	eventsCh <- evs
	waitFn(1*time.Second, 0)

	cl.SetLeader(0)
	waitFn(2*time.Second, 1)
}

func Test_ServiceSingleEvent_LogOnly(t *testing.T) {
	ResetStats()

	// Channel for the service to receive events.
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 1)

	cl := &mockCluster{}

	cfg := DefaultConfig()
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond
	cfg.LogOnly = true
	svc, err := NewService(
		"node1",
		t.TempDir(),
		cl,
		eventsCh,
		cfg,
	)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()
	cl.SetLeader(0)

	// Send one dummy event to the service.
	ev := &proto.CDCEvent{
		Op:       proto.CDCEvent_INSERT,
		Table:    "foo",
		NewRowId: 2,
	}
	evs := &proto.CDCIndexedEventGroup{
		Index:  1,
		Events: []*proto.CDCEvent{ev},
	}
	eventsCh <- evs

	testPoll(t, func() bool {
		return svc.HighWatermark() == evs.Index
	}, 2*time.Second)
}

func Test_ServiceSingleEvent_Retry(t *testing.T) {
	ResetStats()

	// Channel for the service to receive events.
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 1)
	bodyCh := make(chan []byte, 1)
	firstErrSent := false
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if !firstErrSent {
			w.WriteHeader(http.StatusInternalServerError)
			firstErrSent = true
			return
		}
		b, _ := io.ReadAll(r.Body)
		bodyCh <- b
		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	cl := &mockCluster{}

	cfg := DefaultConfig()
	cfg.Endpoint = testSrv.URL
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond
	svc, err := NewService(
		"node1",
		t.TempDir(),
		cl,
		eventsCh,
		cfg,
	)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()
	cl.SetLeader(0)

	// Send one dummy event to the service.
	ev := &proto.CDCEvent{
		Op:       proto.CDCEvent_INSERT,
		Table:    "foo",
		NewRowId: 2,
	}
	evs := &proto.CDCIndexedEventGroup{
		Index:  1,
		Events: []*proto.CDCEvent{ev},
	}
	eventsCh <- evs

	// Wait for the service to forward the batch.
	select {
	case got := <-bodyCh:
		exp := &CDCMessagesEnvelope{
			NodeID: "node1",
			Payload: []*CDCMessage{
				{
					Index: evs.Index,
					Events: []*CDCMessageEvent{
						{
							Op:       ev.Op.String(),
							Table:    ev.Table,
							NewRowId: ev.NewRowId,
							OldRowId: ev.OldRowId,
						},
					},
				},
			},
		}
		msg := &CDCMessagesEnvelope{}
		if err := UnmarshalFromEnvelopeJSON(got, msg); err != nil {
			t.Fatalf("invalid JSON received: %v", err)
		}
		if reflect.DeepEqual(msg, exp) == false {
			t.Fatalf("unexpected payload: got %v, want %v", msg, exp)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for HTTP POST")
	}

	testPoll(t, func() bool {
		return svc.HighWatermark() == evs.Index
	}, 2*time.Second)
}

func Test_ServiceMultiEvent(t *testing.T) {
	ResetStats()

	// Channel for the service to receive events.
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 1)

	bodyCh := make(chan []byte, 1)
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		bodyCh <- b
		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	cl := &mockCluster{}

	cfg := DefaultConfig()
	cfg.Endpoint = testSrv.URL
	cfg.MaxBatchSz = 2
	cfg.MaxBatchDelay = time.Second
	svc, err := NewService(
		"node1",
		t.TempDir(),
		cl,
		eventsCh,
		cfg,
	)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()
	cl.SetLeader(0)

	// Create the Events and send them.
	ev1 := &proto.CDCEvent{
		Op:       proto.CDCEvent_INSERT,
		Table:    "foo",
		NewRowId: 10,
	}
	evs1 := &proto.CDCIndexedEventGroup{
		Index:  1,
		Events: []*proto.CDCEvent{ev1},
	}
	ev2 := &proto.CDCEvent{
		Op:       proto.CDCEvent_UPDATE,
		Table:    "baz",
		OldRowId: 20,
		NewRowId: 30,
	}
	evs2 := &proto.CDCIndexedEventGroup{
		Index:  2,
		Events: []*proto.CDCEvent{ev2},
	}
	eventsCh <- evs1
	eventsCh <- evs2

	// Wait for the service to forward the batch.
	select {
	case got := <-bodyCh:
		exp := &CDCMessagesEnvelope{
			NodeID: "node1",
			Payload: []*CDCMessage{
				{
					Index: evs1.Index,
					Events: []*CDCMessageEvent{
						{
							Op:       ev1.Op.String(),
							Table:    ev1.Table,
							NewRowId: ev1.NewRowId,
							OldRowId: ev1.OldRowId,
						},
					},
				},
				{
					Index: evs2.Index,
					Events: []*CDCMessageEvent{
						{
							Op:       ev2.Op.String(),
							Table:    ev2.Table,
							NewRowId: ev2.NewRowId,
							OldRowId: ev2.OldRowId,
						},
					},
				},
			},
		}
		msg := &CDCMessagesEnvelope{}
		if err := UnmarshalFromEnvelopeJSON(got, msg); err != nil {
			t.Fatalf("invalid JSON received: %v", err)
		}
		if reflect.DeepEqual(msg, exp) == false {
			t.Fatalf("unexpected payload: got %v, want %v", msg, exp)
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
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 1)

	bodyCh := make(chan []byte, 1)
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		bodyCh <- b
		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	cl := &mockCluster{}

	cfg := DefaultConfig()
	cfg.ServiceID = "service1" // Test service ID inclusion.
	cfg.Endpoint = testSrv.URL
	cfg.MaxBatchSz = 2
	cfg.MaxBatchDelay = 100 * time.Millisecond
	svc, err := NewService(
		"node1",
		t.TempDir(),
		cl,
		eventsCh,
		cfg,
	)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()
	cl.SetLeader(0)

	// Create the Events and send them.
	ev1 := &proto.CDCEvent{
		Op:       proto.CDCEvent_INSERT,
		Table:    "foo",
		NewRowId: 10,
	}
	evs1 := &proto.CDCIndexedEventGroup{
		Index:  1,
		Events: []*proto.CDCEvent{ev1},
	}
	ev2 := &proto.CDCEvent{
		Op:       proto.CDCEvent_UPDATE,
		Table:    "baz",
		OldRowId: 20,
		NewRowId: 30,
	}
	evs2 := &proto.CDCIndexedEventGroup{
		Index:  2,
		Events: []*proto.CDCEvent{ev2},
	}
	ev3 := &proto.CDCEvent{
		Op:       proto.CDCEvent_DELETE,
		Table:    "qux",
		OldRowId: 40,
	}
	evs3 := &proto.CDCIndexedEventGroup{
		Index:  3,
		Events: []*proto.CDCEvent{ev3},
	}
	eventsCh <- evs1
	eventsCh <- evs2
	eventsCh <- evs3

	// Wait for the service to forward the first batch.
	select {
	case got := <-bodyCh:
		exp := &CDCMessagesEnvelope{
			ServiceID: "service1",
			NodeID:    "node1",
			Payload: []*CDCMessage{
				{
					Index: evs1.Index,
					Events: []*CDCMessageEvent{
						{
							Op:       ev1.Op.String(),
							Table:    ev1.Table,
							NewRowId: ev1.NewRowId,
						},
					},
				},
				{
					Index: evs2.Index,
					Events: []*CDCMessageEvent{
						{
							Op:       ev2.Op.String(),
							Table:    ev2.Table,
							NewRowId: ev2.NewRowId,
							OldRowId: ev2.OldRowId,
						},
					},
				},
			},
		}
		msg := &CDCMessagesEnvelope{}
		if err := UnmarshalFromEnvelopeJSON(got, msg); err != nil {
			t.Fatalf("invalid JSON received: %v", err)
		}
		if reflect.DeepEqual(msg, exp) == false {
			t.Fatalf("unexpected payload: got %v, want %v", msg, exp)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for HTTP POST")
	}

	// Wait for the service to forward the second batch, which will be kicked out due to a timeout.
	select {
	case got := <-bodyCh:
		exp := &CDCMessagesEnvelope{
			ServiceID: "service1",
			NodeID:    "node1",
			Payload: []*CDCMessage{
				{
					Index: evs3.Index,
					Events: []*CDCMessageEvent{
						{
							Op:       ev3.Op.String(),
							Table:    ev3.Table,
							OldRowId: ev3.OldRowId,
						},
					},
				},
			},
		}
		msg := &CDCMessagesEnvelope{}
		if err := UnmarshalFromEnvelopeJSON(got, msg); err != nil {
			t.Fatalf("invalid JSON received: %v", err)
		}
		if reflect.DeepEqual(msg, exp) == false {
			t.Fatalf("unexpected payload: got %v, want %v", msg, exp)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for HTTP POST")
	}

	testPoll(t, func() bool {
		return svc.HighWatermark() == evs3.Index
	}, 2*time.Second)
}

func Test_ServiceHWMUpdate_Leader(t *testing.T) {
	ResetStats()

	// Channel to send events to the CDC Service.
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)

	cl := &mockCluster{}

	cfg := DefaultConfig()
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond
	cfg.LogOnly = true // Use log-only mode to avoid HTTP complexity
	svc, err := NewService(
		"node1",
		t.TempDir(),
		cl,
		eventsCh,
		cfg,
	)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()

	// Make it the leader.
	cl.SetLeader(0)
	testPoll(t, func() bool { return svc.IsLeader() }, 2*time.Second)

	// Add some events to the FIFO queue
	events := []*proto.CDCIndexedEventGroup{
		{
			Index: 10,
			Events: []*proto.CDCEvent{
				{
					Op:       proto.CDCEvent_INSERT,
					Table:    "foo",
					NewRowId: 1,
				},
			},
		},
		{
			Index: 20,
			Events: []*proto.CDCEvent{
				{
					Op:       proto.CDCEvent_INSERT,
					Table:    "foo",
					NewRowId: 2,
				},
			},
		},
		{
			Index: 30,
			Events: []*proto.CDCEvent{
				{
					Op:       proto.CDCEvent_INSERT,
					Table:    "foo",
					NewRowId: 3,
				},
			},
		},
	}

	// Send events to the service
	for _, ev := range events {
		eventsCh <- ev
	}

	// Wait for events to be processed and high watermark updated
	testPoll(t, func() bool {
		return svc.HighWatermark() == 30
	}, 2*time.Second)
}

func Test_ServiceHWMUpdate_Follow(t *testing.T) {
	ResetStats()

	// Channel to send events to the CDC Service.
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)

	cl := &mockCluster{}

	cfg := DefaultConfig()
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond
	cfg.LogOnly = true // Use log-only mode to avoid HTTP complexity
	svc, err := NewService(
		"node1",
		t.TempDir(),
		cl,
		eventsCh,
		cfg,
	)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()

	// Make it the leader.
	testPoll(t, func() bool { return !svc.IsLeader() }, 2*time.Second)

	// Add some events to the FIFO queue
	events := []*proto.CDCIndexedEventGroup{
		{
			Index: 10,
			Events: []*proto.CDCEvent{
				{
					Op:       proto.CDCEvent_INSERT,
					Table:    "foo",
					NewRowId: 1,
				},
			},
		},
	}

	// Send events to the service
	for _, ev := range events {
		eventsCh <- ev
	}

	// Confirm FIFO has the events
	testPoll(t, func() bool {
		return svc.fifo.Len() == 1
	}, 2*time.Second)

	// Simulate a high watermark update from the cluster, which should
	// prune FIFO.
	cl.BroadcastHighWatermark(10)

	// Wait for events to be processed and high watermark updated
	testPoll(t, func() bool {
		return svc.hwmFollowerUpdated.Load() == 1 && svc.fifo.Len() == 0 && svc.HighWatermark() == 10
	}, 2*time.Second)
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

// mockCluster manages multiple CDC services for comprehensive testing
type mockCluster struct {
	mu             sync.Mutex
	leaderChannels []chan<- bool
	hwmChannels    []chan<- uint64
	currentLeader  int // index of current leader, -1 if none
}

func newMockCluster() *mockCluster {
	return &mockCluster{
		currentLeader: -1,
	}
}

func (tc *mockCluster) RegisterLeaderChange(ch chan<- bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.leaderChannels = append(tc.leaderChannels, ch)
}

func (tc *mockCluster) RegisterHWMUpdate(ch chan<- uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.hwmChannels = append(tc.hwmChannels, ch)
}

func (tc *mockCluster) BroadcastHighWatermark(value uint64) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Default behavior: broadcast to all registered HWM channels
	for _, ch := range tc.hwmChannels {
		select {
		case ch <- value:
		default:
			// Non-blocking send to avoid deadlocks
		}
	}
	return nil
}

// SetLeader send a True to channel at leaderIndex and False to all others.
// Indexes are zero-based. Send -1 to mark no leader.
func (tc *mockCluster) SetLeader(leaderIndex int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.currentLeader = leaderIndex
	for i, ch := range tc.leaderChannels {
		isLeader := (i == leaderIndex)
		select {
		case ch <- isLeader:
		default:
			// Non-blocking send to avoid deadlocks
		}
	}
}

// BroadcastHWM sends HWM update to all registered channels
func (tc *mockCluster) BroadcastHWM(hwm uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for _, ch := range tc.hwmChannels {
		select {
		case ch <- hwm:
		default:
			// Non-blocking send to avoid deadlocks
		}
	}
}

// GetCurrentLeader returns the current leader index
func (tc *mockCluster) GetCurrentLeader() int {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.currentLeader
}
