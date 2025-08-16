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

type mockCluster struct {
	obCh chan<- bool
}

func (m *mockCluster) RegisterLeaderChange(ch chan<- bool) {
	m.obCh = ch
}

func (m *mockCluster) SignalLeaderChange(leader bool) {
	if m.obCh != nil {
		m.obCh <- leader
	}
}

func (m *mockCluster) SetHighWatermark(value uint64) error {
	// Mock implementation does nothing.
	return nil
}

func (m *mockCluster) GetHighWatermark() (uint64, error) {
	// Mock implementation does nothing.
	return 0, nil
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
