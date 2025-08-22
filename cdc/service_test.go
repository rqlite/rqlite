package cdc

import (
	"encoding/json"
	"fmt"
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
	cl.SignalLeaderChange(true)

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
	cl.SignalLeaderChange(false)
	testPoll(t, func() bool { return !svc.IsLeader() }, 2*time.Second)

	// Send events, and make sure they are ignored.
	evs.Index = 67
	eventsCh <- evs
	waitFn(1*time.Second, 0)

	cl.SignalLeaderChange(true)
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
	cl.SignalLeaderChange(true)

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
	cl.SignalLeaderChange(true)

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
	cl.SignalLeaderChange(true)

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
	cl.SignalLeaderChange(true)

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
	cl.SignalLeaderChange(true)
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

type mockCluster struct {
	obCh    chan<- bool
	hwmObCh chan<- uint64
}

func (m *mockCluster) RegisterLeaderChange(ch chan<- bool) {
	m.obCh = ch
}

func (m *mockCluster) RegisterHWMUpdate(ch chan<- uint64) {
	m.hwmObCh = ch
}

func (m *mockCluster) SignalLeaderChange(leader bool) {
	if m.obCh != nil {
		m.obCh <- leader
	}
}

func (m *mockCluster) SignalHWMUpdate(hwm uint64) {
	if m.hwmObCh != nil {
		m.hwmObCh <- hwm
	}
}

func (m *mockCluster) BroadcastHighWatermark(value uint64) error {
	if m.hwmObCh != nil {
		m.hwmObCh <- value
		return nil
	}
	return nil // No observer, nothing to do.
}

// TestCluster manages multiple CDC services for comprehensive testing
type TestCluster struct {
	mu             sync.Mutex
	leaderChannels []chan<- bool
	hwmChannels    []chan<- uint64
	currentLeader  int // index of current leader, -1 if none
	broadcastHWMFn func(uint64) error
}

func NewTestCluster() *TestCluster {
	return &TestCluster{
		currentLeader: -1,
	}
}

func (tc *TestCluster) RegisterLeaderChange(ch chan<- bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.leaderChannels = append(tc.leaderChannels, ch)
}

func (tc *TestCluster) RegisterHWMUpdate(ch chan<- uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.hwmChannels = append(tc.hwmChannels, ch)
}

func (tc *TestCluster) BroadcastHighWatermark(value uint64) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.broadcastHWMFn != nil {
		return tc.broadcastHWMFn(value)
	}

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

// SetLeader makes the node at the given index the leader, others become followers
func (tc *TestCluster) SetLeader(leaderIndex int) {
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
func (tc *TestCluster) BroadcastHWM(hwm uint64) {
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
func (tc *TestCluster) GetCurrentLeader() int {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.currentLeader
}

// SetBroadcastHWMFunc allows customizing the BroadcastHighWatermark behavior
func (tc *TestCluster) SetBroadcastHWMFunc(fn func(uint64) error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.broadcastHWMFn = fn
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

// Test helper to create an HTTP test server that records requests
type HTTPTestServer struct {
	*httptest.Server
	requests [][]byte
	mu       sync.Mutex
}

func NewHTTPTestServer() *HTTPTestServer {
	hts := &HTTPTestServer{}
	hts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		hts.mu.Lock()
		hts.requests = append(hts.requests, body)
		hts.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	return hts
}

func (h *HTTPTestServer) GetRequests() [][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([][]byte, len(h.requests))
	copy(result, h.requests)
	return result
}

func (h *HTTPTestServer) GetRequestCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.requests)
}

func (h *HTTPTestServer) ClearRequests() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.requests = nil
}

// Test_ClusterBasicDelivery tests that only the leader sends events to HTTP endpoint
func Test_ClusterBasicDelivery(t *testing.T) {
	ResetStats()

	// Setup HTTP test server
	httpServer := NewHTTPTestServer()
	defer httpServer.Close()

	// Setup test cluster
	cluster := NewTestCluster()

	// Create three services
	services := make([]*Service, 3)
	eventChannels := make([]chan *proto.CDCIndexedEventGroup, 3)

	for i := 0; i < 3; i++ {
		eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)
		eventChannels[i] = eventsCh

		cfg := DefaultConfig()
		cfg.Endpoint = httpServer.URL
		cfg.MaxBatchSz = 1
		cfg.MaxBatchDelay = 50 * time.Millisecond

		svc, err := NewService(
			fmt.Sprintf("node%d", i+1),
			t.TempDir(),
			cluster,
			eventsCh,
			cfg,
		)
		if err != nil {
			t.Fatalf("failed to create service %d: %v", i, err)
		}

		if err := svc.Start(); err != nil {
			t.Fatalf("failed to start service %d: %v", i, err)
		}
		defer svc.Stop()

		services[i] = svc
	}

	// Make service 0 the leader
	cluster.SetLeader(0)
	testPoll(t, func() bool {
		return services[0].IsLeader() && !services[1].IsLeader() && !services[2].IsLeader()
	}, 2*time.Second)

	// Send events to the leader
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
	}

	for _, ev := range events {
		eventChannels[0] <- ev
	}

	// Wait for events to be sent
	testPoll(t, func() bool {
		return httpServer.GetRequestCount() == 2
	}, 2*time.Second)

	// Verify the leader's high watermark is updated
	testPoll(t, func() bool {
		return services[0].HighWatermark() == 20
	}, 2*time.Second)

	// Verify HTTP requests contain expected events
	requests := httpServer.GetRequests()
	if len(requests) != 2 {
		t.Fatalf("expected 2 HTTP requests, got %d", len(requests))
	}

	// Verify non-leaders don't send HTTP requests
	httpServer.ClearRequests()

	// Send events to non-leaders
	eventChannels[1] <- events[0]
	eventChannels[2] <- events[1]

	// Wait a bit to ensure no HTTP requests are made
	time.Sleep(200 * time.Millisecond)

	if httpServer.GetRequestCount() != 0 {
		t.Fatalf("non-leaders should not send HTTP requests, got %d", httpServer.GetRequestCount())
	}
}

// Test_ClusterSimpleHWM tests basic HWM functionality with TestCluster
func Test_ClusterSimpleHWM(t *testing.T) {
	ResetStats()

	// Setup test cluster
	cluster := NewTestCluster()

	// Create one service
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)

	cfg := DefaultConfig()
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond
	cfg.LogOnly = true // Use log-only mode to avoid HTTP complexity
	cfg.HighWatermarkInterval = 100 * time.Millisecond

	svc, err := NewService(
		"node1",
		t.TempDir(),
		cluster,
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

	// Make it the leader
	cluster.SetLeader(0)
	testPoll(t, func() bool { return svc.IsLeader() }, 2*time.Second)

	// Send an event
	event := &proto.CDCIndexedEventGroup{
		Index: 10,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "foo",
				NewRowId: 1,
			},
		},
	}
	eventsCh <- event

	// Wait for event to be processed and local HWM updated
	testPoll(t, func() bool {
		return svc.HighWatermark() == 10
	}, 2*time.Second)

	t.Logf("Service HWM after processing: %d", svc.HighWatermark())
}

// Test_ClusterHWMPropagation tests high watermark propagation across cluster
func Test_ClusterHWMPropagation(t *testing.T) {
	ResetStats()

	// Setup HTTP test server
	httpServer := NewHTTPTestServer()
	defer httpServer.Close()

	// Setup test cluster
	cluster := NewTestCluster()

	// Track HWM broadcasts
	var broadcastedHWM uint64
	var broadcastMutex sync.Mutex
	cluster.SetBroadcastHWMFunc(func(hwm uint64) error {
		broadcastMutex.Lock()
		broadcastedHWM = hwm
		broadcastMutex.Unlock()
		t.Logf("Broadcasting HWM: %d", hwm)
		// Directly broadcast to channels instead of calling BroadcastHWM to avoid deadlock
		for _, ch := range cluster.hwmChannels {
			select {
			case ch <- hwm:
			default:
				// Non-blocking send to avoid deadlocks
			}
		}
		return nil
	})

	// Create three services
	services := make([]*Service, 3)
	eventChannels := make([]chan *proto.CDCIndexedEventGroup, 3)

	for i := 0; i < 3; i++ {
		eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)
		eventChannels[i] = eventsCh

		cfg := DefaultConfig()
		cfg.Endpoint = httpServer.URL
		cfg.MaxBatchSz = 1
		cfg.MaxBatchDelay = 50 * time.Millisecond
		cfg.HighWatermarkInterval = 100 * time.Millisecond // Short interval for testing

		svc, err := NewService(
			fmt.Sprintf("node%d", i+1),
			t.TempDir(),
			cluster,
			eventsCh,
			cfg,
		)
		if err != nil {
			t.Fatalf("failed to create service %d: %v", i, err)
		}

		if err := svc.Start(); err != nil {
			t.Fatalf("failed to start service %d: %v", i, err)
		}
		defer svc.Stop()

		services[i] = svc
	}

	// Make service 0 the leader
	cluster.SetLeader(0)
	testPoll(t, func() bool {
		return services[0].IsLeader() && !services[1].IsLeader() && !services[2].IsLeader()
	}, 2*time.Second)

	// Send events to the leader
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
	}

	for _, ev := range events {
		eventChannels[0] <- ev
	}

	// Wait for events to be sent
	testPoll(t, func() bool {
		return httpServer.GetRequestCount() == 2
	}, 2*time.Second)

	// Wait for leader's HWM to be updated (happens immediately after sending)
	testPoll(t, func() bool {
		return services[0].HighWatermark() == 20
	}, 2*time.Second)

	t.Logf("Leader HWM after sending: %d", services[0].HighWatermark())

	// Wait for HWM to be broadcast periodically
	testPoll(t, func() bool {
		broadcastMutex.Lock()
		hwm := broadcastedHWM
		broadcastMutex.Unlock()
		t.Logf("Checking broadcast HWM: %d", hwm)
		return hwm == 20
	}, 1*time.Second)

	// Wait for HWM propagation to followers
	testPoll(t, func() bool {
		hwm1 := services[1].HighWatermark()
		hwm2 := services[2].HighWatermark()
		t.Logf("Follower HWMs: node2=%d, node3=%d", hwm1, hwm2)
		return hwm1 == 20 && hwm2 == 20
	}, 2*time.Second)
}

// Test_ClusterLeadershipChange tests leadership changes and catch-up behavior
func Test_ClusterLeadershipChange(t *testing.T) {
	ResetStats()

	// Setup HTTP test server
	httpServer := NewHTTPTestServer()
	defer httpServer.Close()

	// Setup test cluster
	cluster := NewTestCluster()

	// Create three services
	services := make([]*Service, 3)
	eventChannels := make([]chan *proto.CDCIndexedEventGroup, 3)

	for i := 0; i < 3; i++ {
		eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)
		eventChannels[i] = eventsCh

		cfg := DefaultConfig()
		cfg.Endpoint = httpServer.URL
		cfg.MaxBatchSz = 1
		cfg.MaxBatchDelay = 50 * time.Millisecond

		svc, err := NewService(
			fmt.Sprintf("node%d", i+1),
			t.TempDir(),
			cluster,
			eventsCh,
			cfg,
		)
		if err != nil {
			t.Fatalf("failed to create service %d: %v", i, err)
		}

		if err := svc.Start(); err != nil {
			t.Fatalf("failed to start service %d: %v", i, err)
		}
		defer svc.Stop()

		services[i] = svc
	}

	// Initially, make service 0 the leader
	cluster.SetLeader(0)
	testPoll(t, func() bool {
		return services[0].IsLeader() && !services[1].IsLeader() && !services[2].IsLeader()
	}, 2*time.Second)

	// Send an event to service 1 (non-leader) - it should queue but not send
	event1 := &proto.CDCIndexedEventGroup{
		Index: 10,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "foo",
				NewRowId: 1,
			},
		},
	}
	eventChannels[1] <- event1

	// Wait a bit to ensure no HTTP request is made by non-leader
	time.Sleep(200 * time.Millisecond)
	if httpServer.GetRequestCount() != 0 {
		t.Fatalf("non-leader should not send HTTP requests, got %d", httpServer.GetRequestCount())
	}

	// Now make service 1 the leader (service 0 becomes follower)
	cluster.SetLeader(1)
	testPoll(t, func() bool {
		return !services[0].IsLeader() && services[1].IsLeader() && !services[2].IsLeader()
	}, 2*time.Second)

	// Service 1 should now send the queued event
	testPoll(t, func() bool {
		return httpServer.GetRequestCount() == 1
	}, 2*time.Second)

	// Verify service 1's high watermark is updated
	testPoll(t, func() bool {
		return services[1].HighWatermark() == 10
	}, 2*time.Second)

	// Send another event to the new leader
	event2 := &proto.CDCIndexedEventGroup{
		Index: 20,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "foo",
				NewRowId: 2,
			},
		},
	}
	eventChannels[1] <- event2

	// Wait for the second event to be sent
	testPoll(t, func() bool {
		return httpServer.GetRequestCount() == 2
	}, 2*time.Second)

	// Verify service 1's high watermark is updated to the latest
	testPoll(t, func() bool {
		return services[1].HighWatermark() == 20
	}, 2*time.Second)

	// Verify that the old leader (service 0) doesn't send any new events
	eventChannels[0] <- event2
	time.Sleep(200 * time.Millisecond)
	if httpServer.GetRequestCount() != 2 {
		t.Fatalf("old leader should not send new events after demotion, got %d requests", httpServer.GetRequestCount())
	}
}

// Test_ClusterHWMDeletion tests that events are deleted from FIFO after HWM updates
func Test_ClusterHWMDeletion(t *testing.T) {
	ResetStats()

	// Setup HTTP test server
	httpServer := NewHTTPTestServer()
	defer httpServer.Close()

	// Setup test cluster
	cluster := NewTestCluster()

	// Create a service
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)
	cfg := DefaultConfig()
	cfg.Endpoint = httpServer.URL
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond

	svc, err := NewService(
		"node1",
		t.TempDir(),
		cluster,
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

	// Send some events while NOT leader (they should queue in FIFO)
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
	}

	for _, ev := range events {
		eventsCh <- ev
	}

	// Wait for events to be queued in FIFO
	testPoll(t, func() bool {
		return svc.fifo.Len() == 2
	}, 2*time.Second)

	// Simulate HWM update from cluster (as if another node sent these events)
	cluster.BroadcastHWM(15) // This should delete events up to index 15

	// Wait for FIFO to be pruned
	testPoll(t, func() bool {
		return svc.fifo.Len() == 1 && svc.HighWatermark() == 15
	}, 2*time.Second)

	// Send another HWM update that covers all events
	cluster.BroadcastHWM(25)

	// Wait for all events to be deleted from FIFO
	testPoll(t, func() bool {
		return svc.fifo.Len() == 0 && svc.HighWatermark() == 25
	}, 2*time.Second)
}

// Test_ClusterBatchingBehavior tests that multiple events are batched together
func Test_ClusterBatchingBehavior(t *testing.T) {
	ResetStats()

	// Setup HTTP test server
	httpServer := NewHTTPTestServer()
	defer httpServer.Close()

	// Setup test cluster
	cluster := NewTestCluster()

	// Create a service with larger batch size
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)
	cfg := DefaultConfig()
	cfg.Endpoint = httpServer.URL
	cfg.MaxBatchSz = 3                  // Batch up to 3 events
	cfg.MaxBatchDelay = 1 * time.Second // Long delay to test batching by size

	svc, err := NewService(
		"node1",
		t.TempDir(),
		cluster,
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

	// Make it the leader
	cluster.SetLeader(0)
	testPoll(t, func() bool { return svc.IsLeader() }, 2*time.Second)

	// Send 3 events quickly - they should be batched together
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

	for _, ev := range events {
		eventsCh <- ev
	}

	// Wait for events to be batched and sent as one request
	testPoll(t, func() bool {
		return httpServer.GetRequestCount() == 1
	}, 2*time.Second)

	// Verify the high watermark is set to the last event in the batch
	testPoll(t, func() bool {
		return svc.HighWatermark() == 30
	}, 2*time.Second)

	// Verify the request contains all 3 events
	requests := httpServer.GetRequests()
	if len(requests) != 1 {
		t.Fatalf("expected 1 HTTP request with batched events, got %d", len(requests))
	}

	// The request body should contain all 3 events in the payload
	// We can verify this by checking the JSON contains the expected structure
	var envelope struct {
		NodeID  string `json:"node_id"`
		Payload []struct {
			Index uint64 `json:"index"`
		} `json:"payload"`
	}

	if err := json.Unmarshal(requests[0], &envelope); err != nil {
		t.Fatalf("failed to unmarshal request: %v", err)
	}

	if len(envelope.Payload) != 3 {
		t.Fatalf("expected 3 events in batch, got %d", len(envelope.Payload))
	}

	expectedIndexes := []uint64{10, 20, 30}
	for i, event := range envelope.Payload {
		if event.Index != expectedIndexes[i] {
			t.Fatalf("expected event %d to have index %d, got %d", i, expectedIndexes[i], event.Index)
		}
	}
}
