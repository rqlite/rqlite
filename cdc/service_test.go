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

	cl := &mockCluster{}
	cl.leader.Store(true)

	cfg := DefaultConfig()
	cfg.Endpoint = testSrv.URL
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond
	svc, err := NewService(
		t.TempDir(),
		cl,
		&mockStore{},
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
		exp := &CDCMessagesEnvelope{
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

	// Next emulate CDC not running on the Leader.
	cl.leader.Store(false)
	eventsCh <- evs
	pollExpvarUntil(t, numDroppedNotLeader, 1, 2*time.Second)
}

func Test_ServiceSingleEvent_LogOnly(t *testing.T) {
	ResetStats()

	// Channel for the service to receive events.
	eventsCh := make(chan *proto.CDCEvents, 1)

	cl := &mockCluster{}
	cl.leader.Store(true)

	cfg := DefaultConfig()
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond
	cfg.LogOnly = true
	svc, err := NewService(
		t.TempDir(),
		cl,
		&mockStore{},
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

	testPoll(t, func() bool {
		return svc.HighWatermark() == evs.Index
	}, 2*time.Second)
}

func Test_ServiceSingleEvent_Retry(t *testing.T) {
	ResetStats()

	// Channel for the service to receive events.
	eventsCh := make(chan *proto.CDCEvents, 1)
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
	cl.leader.Store(true)

	cfg := DefaultConfig()
	cfg.Endpoint = testSrv.URL
	cfg.MaxBatchSz = 1
	cfg.MaxBatchDelay = 50 * time.Millisecond
	svc, err := NewService(
		t.TempDir(),
		cl,
		&mockStore{},
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
		exp := &CDCMessagesEnvelope{
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
	eventsCh := make(chan *proto.CDCEvents, 1)

	bodyCh := make(chan []byte, 1)
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		bodyCh <- b
		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	cl := &mockCluster{}
	cl.leader.Store(true)

	cfg := DefaultConfig()
	cfg.Endpoint = testSrv.URL
	cfg.MaxBatchSz = 2
	cfg.MaxBatchDelay = time.Second
	svc, err := NewService(
		t.TempDir(),
		cl,
		&mockStore{},
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
		exp := &CDCMessagesEnvelope{
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
	eventsCh := make(chan *proto.CDCEvents, 1)

	bodyCh := make(chan []byte, 1)
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		bodyCh <- b
		w.WriteHeader(http.StatusOK)
	}))
	defer testSrv.Close()

	cl := &mockCluster{}
	cl.leader.Store(true)

	cfg := DefaultConfig()
	cfg.Endpoint = testSrv.URL
	cfg.MaxBatchSz = 2
	cfg.MaxBatchDelay = 100 * time.Millisecond
	svc, err := NewService(
		t.TempDir(),
		cl,
		&mockStore{},
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
	ev3 := &proto.CDCEvent{
		Op:       proto.CDCEvent_DELETE,
		Table:    "qux",
		OldRowId: 40,
	}
	evs3 := &proto.CDCEvents{
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
	leader atomic.Bool
	obCh   chan<- bool
}

func (m *mockCluster) IsLeader() bool { return m.leader.Load() }

func (m *mockCluster) RegisterLeaderChange(ch chan<- bool) {
	m.obCh = ch
}

func (m *mockCluster) SignalLeaderChange(leader bool) {
	m.leader.Store(leader)
	if m.obCh != nil {
		m.obCh <- leader
	}
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
