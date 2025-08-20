package cdc

import (
	"expvar"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
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
