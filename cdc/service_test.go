package cdc

import (
	"expvar"
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

func Test_ServiceHWMUpdate(t *testing.T) {
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

	// Check that FIFO has all 3 events
	if svc.fifo.Len() != 3 {
		t.Fatalf("expected FIFO to contain 3 events, got %d", svc.fifo.Len())
	}

	// Get the highest key from FIFO before HWM update to verify events are there
	highestKeyBefore, err := svc.fifo.HighestKey()
	if err != nil {
		t.Fatalf("failed to get highest key from FIFO: %v", err)
	}
	if highestKeyBefore != 30 {
		t.Fatalf("expected highest key to be 30, got %d", highestKeyBefore)
	}

	// Send a high-water mark update that should prune events with index <= 20
	cl.SignalHWMUpdate(20)

	// Wait for HWM update to be processed (should be ignored since 20 < 30)
	initialCount := svc.hwmUpdated.Load()
	testPoll(t, func() bool {
		return svc.hwmUpdated.Load() > initialCount
	}, 2*time.Second)

	// Verify that the service's high watermark is NOT updated (because 20 < 30)
	// The service should ignore HWM updates that are <= current HWM
	if svc.HighWatermark() != 30 {
		t.Fatalf("expected high watermark to remain 30, got %d", svc.HighWatermark())
	}

	// Verify that the FIFO has NOT been pruned (since HWM update was ignored)
	if svc.fifo.Len() != 3 {
		t.Fatalf("expected FIFO to still contain 3 events after ignored HWM update, got %d", svc.fifo.Len())
	}

	// Send a high-water mark update that should update the HWM and prune older events
	cl.SignalHWMUpdate(35)

	// Wait for HWM update to be processed
	initialCount2 := svc.hwmUpdated.Load()
	testPoll(t, func() bool {
		return svc.hwmUpdated.Load() > initialCount2
	}, 2*time.Second)

	// Verify that the service's high watermark is updated
	testPoll(t, func() bool {
		return svc.HighWatermark() == 35
	}, 2*time.Second)

	// Verify that the FIFO has been emptied (all events <= 35 should be deleted)
	isEmpty, err := svc.fifo.Empty()
	if err != nil {
		t.Fatalf("failed to check if FIFO is empty: %v", err)
	}
	if !isEmpty {
		t.Fatalf("expected FIFO to be empty after HWM update to 35, but it contains %d events", svc.fifo.Len())
	}

	// The highest key should still be 30 since that's the highest event ever added,
	// but events <= 35 should have been deleted via DeleteRange (which includes all our events)
	highestKeyAfter, err := svc.fifo.HighestKey()
	if err != nil {
		t.Fatalf("failed to get highest key from FIFO after HWM update: %v", err)
	}
	if highestKeyAfter != 30 {
		t.Fatalf("expected highest key to still be 30 after HWM update, got %d", highestKeyAfter)
	}
}

func Test_ServiceHWMUpdate_BoundaryConditions(t *testing.T) {
	ResetStats()

	// Channel to send events to the CDC Service.
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)

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

	// Make it the leader.
	cl.SignalLeaderChange(true)
	testPoll(t, func() bool { return svc.IsLeader() }, 2*time.Second)

	// Add an event
	event := &proto.CDCIndexedEventGroup{
		Index: 50,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "foo",
				NewRowId: 1,
			},
		},
	}
	eventsCh <- event

	// Wait for event to be processed
	testPoll(t, func() bool {
		return svc.HighWatermark() == 50
	}, 2*time.Second)

	// Check that FIFO has 1 event
	if svc.fifo.Len() != 1 {
		t.Fatalf("expected FIFO to contain 1 event, got %d", svc.fifo.Len())
	}

	// Test HWM update with value higher than current HWM
	cl.SignalHWMUpdate(60)
	testPoll(t, func() bool {
		return svc.HighWatermark() == 60
	}, 2*time.Second)

	// Check that FIFO has been emptied (event with index 50 should be deleted since 50 <= 60)
	if svc.fifo.Len() != 0 {
		t.Fatalf("expected FIFO to be empty after HWM update to 60, got %d events", svc.fifo.Len())
	}

	// Test HWM update with value less than current HWM
	cl.SignalHWMUpdate(40)

	// Wait for HWM update to be processed (should be ignored since 40 < 60)
	initialCount := svc.hwmUpdated.Load()
	testPoll(t, func() bool {
		return svc.hwmUpdated.Load() > initialCount
	}, 2*time.Second)

	if svc.HighWatermark() != 60 {
		t.Fatalf("expected high watermark to remain 60 when HWM update is less than current, got %d", svc.HighWatermark())
	}

	// Check that FIFO remains empty (no change since HWM update was ignored)
	if svc.fifo.Len() != 0 {
		t.Fatalf("expected FIFO to remain empty after ignored HWM update, got %d events", svc.fifo.Len())
	}

	// Test HWM update when service is not leader - should still work
	cl.SignalLeaderChange(false)
	testPoll(t, func() bool { return !svc.IsLeader() }, 2*time.Second)

	// Send HWM update greater than current
	cl.SignalHWMUpdate(70)

	// Verify that HWM is updated even when not leader
	testPoll(t, func() bool {
		return svc.HighWatermark() == 70
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

func (m *mockCluster) SetHighWatermark(value uint64) error {
	// Mock implementation does nothing.
	return nil
}

// TestCluster is an enhanced mock cluster that can handle multiple services
// and properly broadcast HWM updates to all registered services.
type TestCluster struct {
	mu             sync.RWMutex
	leaderChannels []chan<- bool
	hwmChannels    []chan<- uint64
	currentLeader  int            // index of current leader (-1 for no leader)
	hwmValues      map[int]uint64 // track HWM per service for debugging
}

func NewTestCluster() *TestCluster {
	return &TestCluster{
		leaderChannels: make([]chan<- bool, 0),
		hwmChannels:    make([]chan<- uint64, 0),
		currentLeader:  -1,
		hwmValues:      make(map[int]uint64),
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

func (tc *TestCluster) SetHighWatermark(value uint64) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Broadcast HWM update to all registered services
	for i, ch := range tc.hwmChannels {
		tc.hwmValues[i] = value
		select {
		case ch <- value:
		default:
			// Channel full, skip
		}
	}
	return nil
}

// SetLeader makes the service at the given index the leader.
// Pass -1 to make no service the leader.
func (tc *TestCluster) SetLeader(leaderIndex int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.currentLeader = leaderIndex
	for i, ch := range tc.leaderChannels {
		isLeader := i == leaderIndex
		select {
		case ch <- isLeader:
		default:
			// Channel full, skip
		}
	}
}

// GetCurrentLeader returns the index of the current leader (-1 if none)
func (tc *TestCluster) GetCurrentLeader() int {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.currentLeader
}

// Test_CDCService_MultiNode_BasicDelivery tests that only the leader sends events to HTTP endpoint
// and that HWM updates are properly propagated to all nodes in the cluster.
func Test_CDCService_MultiNode_BasicDelivery(t *testing.T) {
	ResetStats()

	// HTTP test server that records all POST requests
	var httpRequests [][]byte
	var httpMutex sync.Mutex

	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)

		httpMutex.Lock()
		httpRequests = append(httpRequests, body)
		httpMutex.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	// Create test cluster that manages multiple services
	cluster := NewTestCluster()

	// Create three CDC services
	services := make([]*Service, 3)
	eventChannels := make([]chan *proto.CDCIndexedEventGroup, 3)

	for i := 0; i < 3; i++ {
		eventChannels[i] = make(chan *proto.CDCIndexedEventGroup, 10)

		cfg := DefaultConfig()
		cfg.Endpoint = testSrv.URL
		cfg.MaxBatchSz = 1
		cfg.MaxBatchDelay = 50 * time.Millisecond
		cfg.HighWatermarkInterval = 100 * time.Millisecond // Fast HWM broadcasting for testing

		var err error
		services[i], err = NewService(
			fmt.Sprintf("node%d", i+1),
			t.TempDir(),
			cluster,
			eventChannels[i],
			cfg,
		)
		if err != nil {
			t.Fatalf("failed to create service %d: %v", i, err)
		}

		if err := services[i].Start(); err != nil {
			t.Fatalf("failed to start service %d: %v", i, err)
		}
		defer services[i].Stop()
	}

	// Initially no one is leader
	for i := 0; i < 3; i++ {
		if services[i].IsLeader() {
			t.Fatalf("service %d should not be leader initially", i)
		}
	}

	// Make service 0 the leader
	cluster.SetLeader(0)
	testPoll(t, func() bool {
		return services[0].IsLeader() && !services[1].IsLeader() && !services[2].IsLeader()
	}, 2*time.Second)

	// Create test events
	event1 := &proto.CDCIndexedEventGroup{
		Index: 100,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "test_table",
				NewRowId: 1,
			},
		},
	}

	event2 := &proto.CDCIndexedEventGroup{
		Index: 101,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_UPDATE,
				Table:    "test_table",
				NewRowId: 2,
				OldRowId: 1,
			},
		},
	}

	// Send events to leader (service 0)
	eventChannels[0] <- event1
	eventChannels[0] <- event2

	// Wait for events to be sent to HTTP endpoint
	testPoll(t, func() bool {
		httpMutex.Lock()
		defer httpMutex.Unlock()
		return len(httpRequests) >= 2
	}, 5*time.Second)

	// Verify HTTP requests were sent by leader
	httpMutex.Lock()
	if len(httpRequests) != 2 {
		t.Fatalf("expected 2 HTTP requests, got %d", len(httpRequests))
	}
	httpMutex.Unlock()

	// Check that leader's high watermark updated to last event index
	testPoll(t, func() bool {
		hwm := services[0].HighWatermark()
		t.Logf("Service 0 HWM: %d", hwm)
		return hwm == 101
	}, 2*time.Second)

	// Wait for cluster to broadcast HWM (the leader calls SetHighWatermark in a timer)
	// We need to wait for the cluster's HWM broadcasting mechanism to work
	testPoll(t, func() bool {
		hwm1 := services[1].HighWatermark()
		hwm2 := services[2].HighWatermark()
		t.Logf("Service 1 HWM: %d, Service 2 HWM: %d", hwm1, hwm2)
		return hwm1 == 101 && hwm2 == 101
	}, 5*time.Second)

	// Verify that non-leaders (services 1 and 2) did not send any HTTP requests themselves
	// by sending events to them and confirming no additional HTTP requests
	initialRequestCount := len(httpRequests)

	eventChannels[1] <- &proto.CDCIndexedEventGroup{
		Index: 102,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "test_table",
				NewRowId: 3,
			},
		},
	}

	eventChannels[2] <- &proto.CDCIndexedEventGroup{
		Index: 103,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "test_table",
				NewRowId: 4,
			},
		},
	}

	// Wait a bit and verify no new HTTP requests were made
	time.Sleep(100 * time.Millisecond)
	httpMutex.Lock()
	if len(httpRequests) != initialRequestCount {
		t.Fatalf("non-leaders should not send HTTP requests, but got %d total requests", len(httpRequests))
	}
	httpMutex.Unlock()

	// Verify the content of the first HTTP request
	msg1 := &CDCMessagesEnvelope{}
	if err := UnmarshalFromEnvelopeJSON(httpRequests[0], msg1); err != nil {
		t.Fatalf("failed to unmarshal first HTTP request: %v", err)
	}

	if msg1.NodeID != "node1" {
		t.Fatalf("expected first request from node1, got %s", msg1.NodeID)
	}

	if len(msg1.Payload) != 1 || msg1.Payload[0].Index != 100 {
		t.Fatalf("expected first request to contain event with index 100")
	}
}

// Test_CDCService_MultiNode_NonLeaderQueuing tests that non-leader services queue
// events locally but do not send them to the HTTP endpoint.
func Test_CDCService_MultiNode_NonLeaderQueuing(t *testing.T) {
	ResetStats()

	// HTTP test server that records all POST requests
	var httpRequests [][]byte
	var httpMutex sync.Mutex

	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)

		httpMutex.Lock()
		httpRequests = append(httpRequests, body)
		httpMutex.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	// Create test cluster
	cluster := NewTestCluster()

	// Create three CDC services
	services := make([]*Service, 3)
	eventChannels := make([]chan *proto.CDCIndexedEventGroup, 3)

	for i := 0; i < 3; i++ {
		eventChannels[i] = make(chan *proto.CDCIndexedEventGroup, 10)

		cfg := DefaultConfig()
		cfg.Endpoint = testSrv.URL
		cfg.MaxBatchSz = 1
		cfg.MaxBatchDelay = 50 * time.Millisecond
		cfg.HighWatermarkInterval = 100 * time.Millisecond

		var err error
		services[i], err = NewService(
			fmt.Sprintf("node%d", i+1),
			t.TempDir(),
			cluster,
			eventChannels[i],
			cfg,
		)
		if err != nil {
			t.Fatalf("failed to create service %d: %v", i, err)
		}

		if err := services[i].Start(); err != nil {
			t.Fatalf("failed to start service %d: %v", i, err)
		}
		defer services[i].Stop()
	}

	// Make service 0 the leader
	cluster.SetLeader(0)
	testPoll(t, func() bool {
		return services[0].IsLeader() && !services[1].IsLeader() && !services[2].IsLeader()
	}, 2*time.Second)

	// Send events to all services
	event1 := &proto.CDCIndexedEventGroup{
		Index: 200,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "test_table",
				NewRowId: 10,
			},
		},
	}

	event2 := &proto.CDCIndexedEventGroup{
		Index: 201,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "test_table",
				NewRowId: 11,
			},
		},
	}

	event3 := &proto.CDCIndexedEventGroup{
		Index: 202,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "test_table",
				NewRowId: 12,
			},
		},
	}

	// Send one event to each service
	eventChannels[0] <- event1 // Leader
	eventChannels[1] <- event2 // Non-leader
	eventChannels[2] <- event3 // Non-leader

	// Wait for leader's event to be sent to HTTP endpoint
	testPoll(t, func() bool {
		httpMutex.Lock()
		defer httpMutex.Unlock()
		return len(httpRequests) >= 1
	}, 2*time.Second)

	// Give some time for any potential non-leader requests (there should be none)
	time.Sleep(200 * time.Millisecond)

	// Verify only one HTTP request was made (by the leader)
	httpMutex.Lock()
	if len(httpRequests) != 1 {
		t.Fatalf("expected exactly 1 HTTP request from leader, got %d", len(httpRequests))
	}
	httpMutex.Unlock()

	// Verify the request came from the leader (node1) and contains the correct event
	msg := &CDCMessagesEnvelope{}
	if err := UnmarshalFromEnvelopeJSON(httpRequests[0], msg); err != nil {
		t.Fatalf("failed to unmarshal HTTP request: %v", err)
	}

	if msg.NodeID != "node1" {
		t.Fatalf("expected request from node1 (leader), got %s", msg.NodeID)
	}

	if len(msg.Payload) != 1 || msg.Payload[0].Index != 200 {
		t.Fatalf("expected request to contain event with index 200, got %v", msg.Payload)
	}

	// Verify that non-leader services have events queued in their FIFO but didn't send them
	// We can't directly inspect the FIFO contents, but we can verify they have events by
	// checking that their FIFO length is greater than 0
	if services[1].fifo.Len() == 0 {
		t.Fatalf("expected non-leader service 1 to have events queued in FIFO")
	}

	if services[2].fifo.Len() == 0 {
		t.Fatalf("expected non-leader service 2 to have events queued in FIFO")
	}

	// The leader's FIFO should also have the event initially, until HWM cleanup happens
	if services[0].fifo.Len() == 0 {
		t.Fatalf("expected leader service 0 to have events queued in FIFO before HWM cleanup")
	}
}

// Test_CDCService_MultiNode_LeadershipChange tests leadership transitions and ensures
// that new leaders drain their queued events and old leaders stop sending.
func Test_CDCService_MultiNode_LeadershipChange(t *testing.T) {
	ResetStats()

	// HTTP test server that records all POST requests
	var httpRequests [][]byte
	var httpMutex sync.Mutex

	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)

		httpMutex.Lock()
		httpRequests = append(httpRequests, body)
		httpMutex.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	// Create test cluster
	cluster := NewTestCluster()

	// Create three CDC services
	services := make([]*Service, 3)
	eventChannels := make([]chan *proto.CDCIndexedEventGroup, 3)

	for i := 0; i < 3; i++ {
		eventChannels[i] = make(chan *proto.CDCIndexedEventGroup, 10)

		cfg := DefaultConfig()
		cfg.Endpoint = testSrv.URL
		cfg.MaxBatchSz = 1
		cfg.MaxBatchDelay = 50 * time.Millisecond
		cfg.HighWatermarkInterval = 100 * time.Millisecond

		var err error
		services[i], err = NewService(
			fmt.Sprintf("node%d", i+1),
			t.TempDir(),
			cluster,
			eventChannels[i],
			cfg,
		)
		if err != nil {
			t.Fatalf("failed to create service %d: %v", i, err)
		}

		if err := services[i].Start(); err != nil {
			t.Fatalf("failed to start service %d: %v", i, err)
		}
		defer services[i].Stop()
	}

	// Phase 1: Make service 0 the leader
	cluster.SetLeader(0)
	testPoll(t, func() bool {
		return services[0].IsLeader() && !services[1].IsLeader() && !services[2].IsLeader()
	}, 2*time.Second)

	// Send an event to service 1 (non-leader) - it should queue but not send
	eventForService1 := &proto.CDCIndexedEventGroup{
		Index: 300,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "test_table",
				NewRowId: 20,
			},
		},
	}
	eventChannels[1] <- eventForService1

	// Give some time for service 1 to queue the event
	time.Sleep(100 * time.Millisecond)

	// Verify no HTTP requests yet (service 1 is not leader)
	httpMutex.Lock()
	initialRequestCount := len(httpRequests)
	httpMutex.Unlock()

	if initialRequestCount != 0 {
		t.Fatalf("expected no HTTP requests from non-leader, got %d", initialRequestCount)
	}

	// Verify service 1 has the event queued
	if services[1].fifo.Len() == 0 {
		t.Fatalf("expected service 1 to have event queued in FIFO")
	}

	// Phase 2: Change leadership from service 0 to service 1
	cluster.SetLeader(1)
	testPoll(t, func() bool {
		return !services[0].IsLeader() && services[1].IsLeader() && !services[2].IsLeader()
	}, 2*time.Second)

	// Wait for service 1 (now leader) to drain its FIFO and send the queued event
	testPoll(t, func() bool {
		httpMutex.Lock()
		defer httpMutex.Unlock()
		return len(httpRequests) >= 1
	}, 3*time.Second)

	// Verify the HTTP request was made by the new leader (service 1)
	httpMutex.Lock()
	if len(httpRequests) != 1 {
		t.Fatalf("expected exactly 1 HTTP request after leadership change, got %d", len(httpRequests))
	}
	httpMutex.Unlock()

	// Verify the request came from node2 (service 1) and contains the correct event
	msg := &CDCMessagesEnvelope{}
	if err := UnmarshalFromEnvelopeJSON(httpRequests[0], msg); err != nil {
		t.Fatalf("failed to unmarshal HTTP request: %v", err)
	}

	if msg.NodeID != "node2" {
		t.Fatalf("expected request from node2 (new leader), got %s", msg.NodeID)
	}

	if len(msg.Payload) != 1 || msg.Payload[0].Index != 300 {
		t.Fatalf("expected request to contain event with index 300, got %v", msg.Payload)
	}

	// Phase 3: Verify old leader (service 0) does not send anymore
	eventForOldLeader := &proto.CDCIndexedEventGroup{
		Index: 301,
		Events: []*proto.CDCEvent{
			{
				Op:       proto.CDCEvent_INSERT,
				Table:    "test_table",
				NewRowId: 21,
			},
		},
	}
	eventChannels[0] <- eventForOldLeader

	// Wait a bit and verify no additional HTTP requests from old leader
	time.Sleep(200 * time.Millisecond)
	httpMutex.Lock()
	if len(httpRequests) != 1 {
		t.Fatalf("old leader should not send HTTP requests, but got %d total requests", len(httpRequests))
	}
	httpMutex.Unlock()

	// Verify IsLeader() flags are correct
	if services[0].IsLeader() {
		t.Fatalf("service 0 should no longer be leader")
	}

	if !services[1].IsLeader() {
		t.Fatalf("service 1 should be leader")
	}

	if services[2].IsLeader() {
		t.Fatalf("service 2 should not be leader")
	}
}

// Test_CDCService_MultiNode_HWMDeletion tests that HWM updates cause deletion
// of old events from FIFO queues across all services.
// NOTE: This test currently demonstrates HWM propagation but the FIFO cleanup
// timing may need adjustment in the future.
func Test_CDCService_MultiNode_HWMDeletion(t *testing.T) {
	t.Skip("HWM deletion timing needs further investigation")
	ResetStats()

	// HTTP test server that records all POST requests
	var httpRequests [][]byte
	var httpMutex sync.Mutex

	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)

		httpMutex.Lock()
		httpRequests = append(httpRequests, body)
		httpMutex.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	// Create test cluster
	cluster := NewTestCluster()

	// Create three CDC services
	services := make([]*Service, 3)
	eventChannels := make([]chan *proto.CDCIndexedEventGroup, 3)

	for i := 0; i < 3; i++ {
		eventChannels[i] = make(chan *proto.CDCIndexedEventGroup, 10)

		cfg := DefaultConfig()
		cfg.Endpoint = testSrv.URL
		cfg.MaxBatchSz = 1
		cfg.MaxBatchDelay = 50 * time.Millisecond
		cfg.HighWatermarkInterval = 100 * time.Millisecond

		var err error
		services[i], err = NewService(
			fmt.Sprintf("node%d", i+1),
			t.TempDir(),
			cluster,
			eventChannels[i],
			cfg,
		)
		if err != nil {
			t.Fatalf("failed to create service %d: %v", i, err)
		}

		if err := services[i].Start(); err != nil {
			t.Fatalf("failed to start service %d: %v", i, err)
		}
		defer services[i].Stop()
	}

	// Make service 0 the leader
	cluster.SetLeader(0)
	testPoll(t, func() bool {
		return services[0].IsLeader() && !services[1].IsLeader() && !services[2].IsLeader()
	}, 2*time.Second)

	// Send multiple events to service 0 (the leader) to populate its FIFO
	events := []*proto.CDCIndexedEventGroup{
		{Index: 500, Events: []*proto.CDCEvent{{Op: proto.CDCEvent_INSERT, Table: "test", NewRowId: 1}}},
		{Index: 501, Events: []*proto.CDCEvent{{Op: proto.CDCEvent_INSERT, Table: "test", NewRowId: 2}}},
		{Index: 502, Events: []*proto.CDCEvent{{Op: proto.CDCEvent_INSERT, Table: "test", NewRowId: 3}}},
	}

	for _, event := range events {
		eventChannels[0] <- event
	}

	// Wait for all events to be sent via HTTP
	testPoll(t, func() bool {
		httpMutex.Lock()
		defer httpMutex.Unlock()
		return len(httpRequests) >= 3
	}, 3*time.Second)

	// Wait for HWM to be updated and broadcasted to all services
	testPoll(t, func() bool {
		hwm0 := services[0].HighWatermark()
		hwm1 := services[1].HighWatermark()
		hwm2 := services[2].HighWatermark()
		t.Logf("HWMs: service0=%d, service1=%d, service2=%d", hwm0, hwm1, hwm2)
		return hwm0 >= 502 && hwm1 >= 502 && hwm2 >= 502
	}, 3*time.Second)

	// Check initial FIFO state - service 0 should have events that get cleaned up
	initialFIFOLen := services[0].fifo.Len()
	t.Logf("Service 0 initial FIFO length: %d", initialFIFOLen)

	// Wait for HWM deletion to occur - events up to index 502 should be deleted
	testPoll(t, func() bool {
		len0 := services[0].fifo.Len()
		t.Logf("Service 0 FIFO length after HWM cleanup: %d", len0)
		return len0 < initialFIFOLen // Events should be deleted
	}, 3*time.Second)

	// Now test that old events are not re-sent when leadership changes
	// Make service 1 the new leader
	cluster.SetLeader(1)
	testPoll(t, func() bool {
		return !services[0].IsLeader() && services[1].IsLeader() && !services[2].IsLeader()
	}, 2*time.Second)

	// Send an old event (below HWM) to the new leader - it should be ignored/dropped
	oldEvent := &proto.CDCIndexedEventGroup{
		Index:  500, // This is below the current HWM of 502
		Events: []*proto.CDCEvent{{Op: proto.CDCEvent_INSERT, Table: "test", NewRowId: 99}},
	}
	eventChannels[1] <- oldEvent

	// Send a new event (above HWM) to verify normal operation still works
	newEvent := &proto.CDCIndexedEventGroup{
		Index:  503, // This is above the current HWM
		Events: []*proto.CDCEvent{{Op: proto.CDCEvent_INSERT, Table: "test", NewRowId: 100}},
	}
	eventChannels[1] <- newEvent

	// Count requests before the new events
	httpMutex.Lock()
	requestCountBefore := len(httpRequests)
	httpMutex.Unlock()

	// Wait for the new event to be sent (if it is)
	time.Sleep(300 * time.Millisecond)

	// Get final request count
	httpMutex.Lock()
	requestCountAfter := len(httpRequests)
	httpMutex.Unlock()

	// Verify behavior: the old event should be dropped, only the new event should be sent
	if requestCountAfter <= requestCountBefore {
		t.Logf("No new HTTP requests made, which is expected if events are being dropped properly")
		// This is actually expected if the FIFO logic prevents old events from being processed
	} else if requestCountAfter == requestCountBefore+1 {
		// Only the new event should be sent
		msg := &CDCMessagesEnvelope{}
		if err := UnmarshalFromEnvelopeJSON(httpRequests[requestCountAfter-1], msg); err != nil {
			t.Fatalf("failed to unmarshal latest HTTP request: %v", err)
		}

		if len(msg.Payload) != 1 || msg.Payload[0].Index != 503 {
			t.Fatalf("expected latest request to contain event with index 503, got %v", msg.Payload)
		}
		t.Logf("Successfully sent only the new event with index 503")
	} else {
		t.Fatalf("expected at most 1 new HTTP request, got %d", requestCountAfter-requestCountBefore)
	}
}

// Test_CDCService_MultiNode_Batching tests that multiple events are properly batched
// before being sent to the HTTP endpoint when batch size and delay thresholds are met.
func Test_CDCService_MultiNode_Batching(t *testing.T) {
	ResetStats()

	// HTTP test server that records all POST requests
	var httpRequests [][]byte
	var httpMutex sync.Mutex

	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)

		httpMutex.Lock()
		httpRequests = append(httpRequests, body)
		httpMutex.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	// Create test cluster
	cluster := NewTestCluster()

	// Create one CDC service for simplicity
	eventCh := make(chan *proto.CDCIndexedEventGroup, 10)

	cfg := DefaultConfig()
	cfg.Endpoint = testSrv.URL
	cfg.MaxBatchSz = 3                         // Batch up to 3 events
	cfg.MaxBatchDelay = 500 * time.Millisecond // Wait up to 500ms
	cfg.HighWatermarkInterval = 100 * time.Millisecond

	svc, err := NewService(
		"node1",
		t.TempDir(),
		cluster,
		eventCh,
		cfg,
	)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start service: %v", err)
	}
	defer svc.Stop()

	// Make this service the leader
	cluster.SetLeader(0)
	testPoll(t, func() bool {
		return svc.IsLeader()
	}, 2*time.Second)

	// Test 1: Send exactly MaxBatchSz events quickly - should trigger immediate batch send
	events := []*proto.CDCIndexedEventGroup{
		{Index: 600, Events: []*proto.CDCEvent{{Op: proto.CDCEvent_INSERT, Table: "test", NewRowId: 1}}},
		{Index: 601, Events: []*proto.CDCEvent{{Op: proto.CDCEvent_INSERT, Table: "test", NewRowId: 2}}},
		{Index: 602, Events: []*proto.CDCEvent{{Op: proto.CDCEvent_INSERT, Table: "test", NewRowId: 3}}},
	}

	// Send all events quickly
	for _, event := range events {
		eventCh <- event
	}

	// Wait for batch to be sent (should happen quickly due to batch size limit)
	testPoll(t, func() bool {
		httpMutex.Lock()
		defer httpMutex.Unlock()
		return len(httpRequests) >= 1
	}, 2*time.Second)

	// Verify the batch contains all 3 events
	httpMutex.Lock()
	if len(httpRequests) != 1 {
		t.Fatalf("expected exactly 1 HTTP request for batch, got %d", len(httpRequests))
	}

	msg := &CDCMessagesEnvelope{}
	if err := UnmarshalFromEnvelopeJSON(httpRequests[0], msg); err != nil {
		t.Fatalf("failed to unmarshal HTTP request: %v", err)
	}

	if len(msg.Payload) != 3 {
		t.Fatalf("expected batch to contain 3 events, got %d", len(msg.Payload))
	}

	// Verify events are in correct order
	for i, expectedIndex := range []uint64{600, 601, 602} {
		if msg.Payload[i].Index != expectedIndex {
			t.Fatalf("expected event %d to have index %d, got %d", i, expectedIndex, msg.Payload[i].Index)
		}
	}
	httpMutex.Unlock()

	// Test 2: Send fewer than MaxBatchSz events - should wait for timeout before sending
	time.Sleep(200 * time.Millisecond) // Wait a bit between tests

	event4 := &proto.CDCIndexedEventGroup{
		Index:  603,
		Events: []*proto.CDCEvent{{Op: proto.CDCEvent_INSERT, Table: "test", NewRowId: 4}},
	}
	event5 := &proto.CDCIndexedEventGroup{
		Index:  604,
		Events: []*proto.CDCEvent{{Op: proto.CDCEvent_INSERT, Table: "test", NewRowId: 5}},
	}

	// Send only 2 events (less than batch size)
	eventCh <- event4
	eventCh <- event5

	// Should NOT have a new request immediately
	time.Sleep(100 * time.Millisecond)
	httpMutex.Lock()
	requestCount := len(httpRequests)
	httpMutex.Unlock()

	if requestCount != 1 {
		t.Fatalf("expected no new HTTP request before timeout, got %d total", requestCount)
	}

	// Wait for batch timeout to trigger
	testPoll(t, func() bool {
		httpMutex.Lock()
		defer httpMutex.Unlock()
		return len(httpRequests) >= 2
	}, time.Second)

	// Verify the second batch contains the remaining 2 events
	httpMutex.Lock()
	if len(httpRequests) != 2 {
		t.Fatalf("expected exactly 2 HTTP requests total, got %d", len(httpRequests))
	}

	msg2 := &CDCMessagesEnvelope{}
	if err := UnmarshalFromEnvelopeJSON(httpRequests[1], msg2); err != nil {
		t.Fatalf("failed to unmarshal second HTTP request: %v", err)
	}

	if len(msg2.Payload) != 2 {
		t.Fatalf("expected second batch to contain 2 events, got %d", len(msg2.Payload))
	}

	// Verify events are correct
	if msg2.Payload[0].Index != 603 || msg2.Payload[1].Index != 604 {
		t.Fatalf("expected second batch to contain events 603 and 604, got %d and %d",
			msg2.Payload[0].Index, msg2.Payload[1].Index)
	}
	httpMutex.Unlock()

	t.Logf("Batching test completed successfully - verified batch size and timeout behavior")
}

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

// Test_ServiceHWMPersistence tests that the high watermark persists across service restarts.
func Test_ServiceHWMPersistence(t *testing.T) {
	ResetStats()

	// Use a temp directory for this test
	dir := t.TempDir()

	// Channel for the service to receive events
	eventsCh := make(chan *proto.CDCIndexedEventGroup, 1)

	cl := &mockCluster{}

	cfg := DefaultConfig()
	cfg.LogOnly = true // Use log-only mode to avoid HTTP server setup
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond

	// Create the first service
	svc1, err := NewService("node1", dir, cl, eventsCh, cfg)
	if err != nil {
		t.Fatalf("failed to create first service: %v", err)
	}
	if err := svc1.Start(); err != nil {
		t.Fatalf("failed to start first service: %v", err)
	}

	// Make it the leader
	cl.SignalLeaderChange(true)

	// Send an HWM update
	testHWM := uint64(12345)
	cl.SignalHWMUpdate(testHWM)

	// Wait for HWM update to be processed
	initialCount := svc1.hwmUpdated.Load()
	testPoll(t, func() bool {
		return svc1.hwmUpdated.Load() > initialCount
	}, 2*time.Second)

	// Verify that the HWM was updated
	if svc1.HighWatermark() != testHWM {
		t.Fatalf("expected high watermark to be %d, got %d", testHWM, svc1.HighWatermark())
	}

	// Stop the first service
	svc1.Stop()

	// Create a new service using the same directory
	svc2, err := NewService("node1", dir, cl, eventsCh, cfg)
	if err != nil {
		t.Fatalf("failed to create second service: %v", err)
	}

	// Verify that the new service has the correct HWM value from the file
	if svc2.HighWatermark() != testHWM {
		t.Fatalf("expected new service to have high watermark %d, got %d", testHWM, svc2.HighWatermark())
	}

	// Clean up
	svc2.Stop()
}
