package cdc

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
)

// Test_ClusterBasicDelivery tests that only the leader sends events to HTTP endpoint
func Test_ClusterBasicDelivery(t *testing.T) {
	ResetStats()

	// Setup HTTP test server
	httpServer := NewhttpTestServer()
	defer httpServer.Close()

	// Setup test cluster
	cluster := newMockCluster()

	// Create three services
	services := make([]*Service, 3)
	eventChannels := make([]chan *proto.CDCIndexedEventGroup, 3)

	for i := range 3 {
		eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)
		eventChannels[i] = eventsCh

		cfg := DefaultConfig()
		cfg.Endpoint = httpServer.URL
		cfg.MaxBatchSz = 1
		cfg.MaxBatchDelay = 50 * time.Millisecond
		cfg.HighWatermarkInterval = 100 * time.Millisecond // Short interval for testing

		svc, err := NewService(
			fmt.Sprintf("node%d", i),
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

	// Verify followers don't send HTTP requests
	httpServer.ClearRequests()

	// Send events to followers
	for _, ev := range events {
		eventChannels[1] <- ev
		eventChannels[2] <- ev
	}

	// Wait a bit to ensure no HTTP requests are made
	time.Sleep(200 * time.Millisecond)

	if httpServer.GetRequestCount() != 0 {
		t.Fatalf("followers should not send CDC HTTP requests, got %d", httpServer.GetRequestCount())
	}

	testPoll(t, func() bool {
		return services[1].HighWatermark() == 20
	}, 1*time.Second)

}

// Test_ClusterSimpleHWM tests basic HWM functionality with mockCluster
func Test_ClusterSimpleHWM(t *testing.T) {
	ResetStats()

	// Setup test cluster
	cluster := newMockCluster()

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

	httpServer := NewhttpTestServer()
	defer httpServer.Close()

	cluster := newMockCluster()

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

	// Send all events to each Service's channel, in parallel simulating
	// the database layer sending events to the CDC service.
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
	for _, eCh := range eventChannels {
		go func(ch chan *proto.CDCIndexedEventGroup) {
			for _, ev := range events {
				ch <- ev
			}
		}(eCh)
	}

	// Wait for events to be sent by the Leader
	testPoll(t, func() bool {
		return httpServer.GetRequestCount() == 2
	}, time.Second)

	// Confirm that HWM is set correctly across cluster.
	testPoll(t, func() bool {
		return services[0].HighWatermark() == 20 && services[1].HighWatermark() == 20 && services[2].HighWatermark() == 20
	}, time.Second)

	// Check internals
	testPoll(t, func() bool {
		return services[1].hwmFollowerUpdated.Load() != 0 && services[2].hwmFollowerUpdated.Load() != 0
	}, time.Second)
	if services[0].hwmLeaderUpdated.Load() == 0 {
		t.Fatalf("expected leader to have updated HWM, got 0")
	}
	if services[0].hwmFollowerUpdated.Load() != 0 {
		t.Fatalf("expected leader to node have follower-updated HWM")
	}
}

// // Test_ClusterLeadershipChange tests leadership changes and catch-up behavior
// func Test_ClusterLeadershipChange(t *testing.T) {
// 	ResetStats()

// 	// Setup HTTP test server
// 	httpServer := NewhttpTestServer()
// 	defer httpServer.Close()

// 	// Setup test cluster
// 	cluster := NewTestCluster()

// 	// Create three services
// 	services := make([]*Service, 3)
// 	eventChannels := make([]chan *proto.CDCIndexedEventGroup, 3)

// 	for i := 0; i < 3; i++ {
// 		eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)
// 		eventChannels[i] = eventsCh

// 		cfg := DefaultConfig()
// 		cfg.Endpoint = httpServer.URL
// 		cfg.MaxBatchSz = 1
// 		cfg.MaxBatchDelay = 50 * time.Millisecond

// 		svc, err := NewService(
// 			fmt.Sprintf("node%d", i+1),
// 			t.TempDir(),
// 			cluster,
// 			eventsCh,
// 			cfg,
// 		)
// 		if err != nil {
// 			t.Fatalf("failed to create service %d: %v", i, err)
// 		}

// 		if err := svc.Start(); err != nil {
// 			t.Fatalf("failed to start service %d: %v", i, err)
// 		}
// 		defer svc.Stop()

// 		services[i] = svc
// 	}

// 	// Initially, make service 0 the leader
// 	cluster.SetLeader(0)
// 	testPoll(t, func() bool {
// 		return services[0].IsLeader() && !services[1].IsLeader() && !services[2].IsLeader()
// 	}, 2*time.Second)

// 	// Send an event to service 1 (non-leader) - it should queue but not send
// 	event1 := &proto.CDCIndexedEventGroup{
// 		Index: 10,
// 		Events: []*proto.CDCEvent{
// 			{
// 				Op:       proto.CDCEvent_INSERT,
// 				Table:    "foo",
// 				NewRowId: 1,
// 			},
// 		},
// 	}
// 	eventChannels[1] <- event1

// 	// Wait a bit to ensure no HTTP request is made by non-leader
// 	time.Sleep(200 * time.Millisecond)
// 	if httpServer.GetRequestCount() != 0 {
// 		t.Fatalf("non-leader should not send HTTP requests, got %d", httpServer.GetRequestCount())
// 	}

// 	// Now make service 1 the leader (service 0 becomes follower)
// 	cluster.SetLeader(1)
// 	testPoll(t, func() bool {
// 		return !services[0].IsLeader() && services[1].IsLeader() && !services[2].IsLeader()
// 	}, 2*time.Second)

// 	// Service 1 should now send the queued event
// 	testPoll(t, func() bool {
// 		return httpServer.GetRequestCount() == 1
// 	}, 2*time.Second)

// 	// Verify service 1's high watermark is updated
// 	testPoll(t, func() bool {
// 		return services[1].HighWatermark() == 10
// 	}, 2*time.Second)

// 	// Send another event to the new leader
// 	event2 := &proto.CDCIndexedEventGroup{
// 		Index: 20,
// 		Events: []*proto.CDCEvent{
// 			{
// 				Op:       proto.CDCEvent_INSERT,
// 				Table:    "foo",
// 				NewRowId: 2,
// 			},
// 		},
// 	}
// 	eventChannels[1] <- event2

// 	// Wait for the second event to be sent
// 	testPoll(t, func() bool {
// 		return httpServer.GetRequestCount() == 2
// 	}, 2*time.Second)

// 	// Verify service 1's high watermark is updated to the latest
// 	testPoll(t, func() bool {
// 		return services[1].HighWatermark() == 20
// 	}, 2*time.Second)

// 	// Verify that the old leader (service 0) doesn't send any new events
// 	eventChannels[0] <- event2
// 	time.Sleep(200 * time.Millisecond)
// 	if httpServer.GetRequestCount() != 2 {
// 		t.Fatalf("old leader should not send new events after demotion, got %d requests", httpServer.GetRequestCount())
// 	}
// }

// // Test_ClusterHWMDeletion tests that events are deleted from FIFO after HWM updates
// func Test_ClusterHWMDeletion(t *testing.T) {
// 	ResetStats()

// 	// Setup HTTP test server
// 	httpServer := NewhttpTestServer()
// 	defer httpServer.Close()

// 	// Setup test cluster
// 	cluster := NewTestCluster()

// 	// Create a service
// 	eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)
// 	cfg := DefaultConfig()
// 	cfg.Endpoint = httpServer.URL
// 	cfg.MaxBatchSz = 1
// 	cfg.MaxBatchDelay = 50 * time.Millisecond

// 	svc, err := NewService(
// 		"node1",
// 		t.TempDir(),
// 		cluster,
// 		eventsCh,
// 		cfg,
// 	)
// 	if err != nil {
// 		t.Fatalf("failed to create service: %v", err)
// 	}

// 	if err := svc.Start(); err != nil {
// 		t.Fatalf("failed to start service: %v", err)
// 	}
// 	defer svc.Stop()

// 	// Send some events while NOT leader (they should queue in FIFO)
// 	events := []*proto.CDCIndexedEventGroup{
// 		{
// 			Index: 10,
// 			Events: []*proto.CDCEvent{
// 				{
// 					Op:       proto.CDCEvent_INSERT,
// 					Table:    "foo",
// 					NewRowId: 1,
// 				},
// 			},
// 		},
// 		{
// 			Index: 20,
// 			Events: []*proto.CDCEvent{
// 				{
// 					Op:       proto.CDCEvent_INSERT,
// 					Table:    "foo",
// 					NewRowId: 2,
// 				},
// 			},
// 		},
// 	}

// 	for _, ev := range events {
// 		eventsCh <- ev
// 	}

// 	// Wait for events to be queued in FIFO
// 	testPoll(t, func() bool {
// 		return svc.fifo.Len() == 2
// 	}, 2*time.Second)

// 	// Simulate HWM update from cluster (as if another node sent these events)
// 	cluster.BroadcastHWM(15) // This should delete events up to index 15

// 	// Wait for FIFO to be pruned
// 	testPoll(t, func() bool {
// 		return svc.fifo.Len() == 1 && svc.HighWatermark() == 15
// 	}, 2*time.Second)

// 	// Send another HWM update that covers all events
// 	cluster.BroadcastHWM(25)

// 	// Wait for all events to be deleted from FIFO
// 	testPoll(t, func() bool {
// 		return svc.fifo.Len() == 0 && svc.HighWatermark() == 25
// 	}, 2*time.Second)
// }

// // Test_ClusterBatchingBehavior tests that multiple events are batched together
// func Test_ClusterBatchingBehavior(t *testing.T) {
// 	ResetStats()

// 	// Setup HTTP test server
// 	httpServer := NewhttpTestServer()
// 	defer httpServer.Close()

// 	// Setup test cluster
// 	cluster := NewTestCluster()

// 	// Create a service with larger batch size
// 	eventsCh := make(chan *proto.CDCIndexedEventGroup, 10)
// 	cfg := DefaultConfig()
// 	cfg.Endpoint = httpServer.URL
// 	cfg.MaxBatchSz = 3                  // Batch up to 3 events
// 	cfg.MaxBatchDelay = 1 * time.Second // Long delay to test batching by size

// 	svc, err := NewService(
// 		"node1",
// 		t.TempDir(),
// 		cluster,
// 		eventsCh,
// 		cfg,
// 	)
// 	if err != nil {
// 		t.Fatalf("failed to create service: %v", err)
// 	}

// 	if err := svc.Start(); err != nil {
// 		t.Fatalf("failed to start service: %v", err)
// 	}
// 	defer svc.Stop()

// 	// Make it the leader
// 	cluster.SetLeader(0)
// 	testPoll(t, func() bool { return svc.IsLeader() }, 2*time.Second)

// 	// Send 3 events quickly - they should be batched together
// 	events := []*proto.CDCIndexedEventGroup{
// 		{
// 			Index: 10,
// 			Events: []*proto.CDCEvent{
// 				{
// 					Op:       proto.CDCEvent_INSERT,
// 					Table:    "foo",
// 					NewRowId: 1,
// 				},
// 			},
// 		},
// 		{
// 			Index: 20,
// 			Events: []*proto.CDCEvent{
// 				{
// 					Op:       proto.CDCEvent_INSERT,
// 					Table:    "foo",
// 					NewRowId: 2,
// 				},
// 			},
// 		},
// 		{
// 			Index: 30,
// 			Events: []*proto.CDCEvent{
// 				{
// 					Op:       proto.CDCEvent_INSERT,
// 					Table:    "foo",
// 					NewRowId: 3,
// 				},
// 			},
// 		},
// 	}

// 	for _, ev := range events {
// 		eventsCh <- ev
// 	}

// 	// Wait for events to be batched and sent as one request
// 	testPoll(t, func() bool {
// 		return httpServer.GetRequestCount() == 1
// 	}, 2*time.Second)

// 	// Verify the high watermark is set to the last event in the batch
// 	testPoll(t, func() bool {
// 		return svc.HighWatermark() == 30
// 	}, 2*time.Second)

// 	// Verify the request contains all 3 events
// 	requests := httpServer.GetRequests()
// 	if len(requests) != 1 {
// 		t.Fatalf("expected 1 HTTP request with batched events, got %d", len(requests))
// 	}

// 	// The request body should contain all 3 events in the payload
// 	// We can verify this by checking the JSON contains the expected structure
// 	var envelope struct {
// 		NodeID  string `json:"node_id"`
// 		Payload []struct {
// 			Index uint64 `json:"index"`
// 		} `json:"payload"`
// 	}

// 	if err := json.Unmarshal(requests[0], &envelope); err != nil {
// 		t.Fatalf("failed to unmarshal request: %v", err)
// 	}

// 	if len(envelope.Payload) != 3 {
// 		t.Fatalf("expected 3 events in batch, got %d", len(envelope.Payload))
// 	}

// 	expectedIndexes := []uint64{10, 20, 30}
// 	for i, event := range envelope.Payload {
// 		if event.Index != expectedIndexes[i] {
// 			t.Fatalf("expected event %d to have index %d, got %d", i, expectedIndexes[i], event.Index)
// 		}
// 	}
// }

// Test helper to create an HTTP test server that records requests
type httpTestServer struct {
	*httptest.Server
	requests [][]byte
	mu       sync.Mutex
}

func NewhttpTestServer() *httpTestServer {
	hts := &httpTestServer{}
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

func (h *httpTestServer) GetRequests() [][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([][]byte, len(h.requests))
	copy(result, h.requests)
	return result
}

func (h *httpTestServer) GetRequestCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.requests)
}

func (h *httpTestServer) ClearRequests() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.requests = nil
}
