package store

import (
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
)

// Test_StoreEnableCDC tests that CDC can be initialized via configuration.
func Test_StoreEnableCDC(t *testing.T) {
	// Create a channel for CDC events
	ch := make(chan *proto.CDCIndexedEventGroup, 10)

	// Create CDC config
	cdcConfig := &CDCConfig{
		Ch:         ch,
		RowIDsOnly: false,
	}

	cfg := NewDBConfig()
	ly := mustMockLayer("localhost:0")
	s := New(&Config{
		DBConf: cfg,
		Dir:    t.TempDir(),
		ID:     "test-node",
	}, cdcConfig, ly)

	defer s.Close(true)
	defer ly.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	// CDC should be initialized
	if s.cdcStreamer == nil {
		t.Fatalf("expected CDC streamer to be created when CDCConfig is provided")
	}
}

// Test_StoreWithoutCDCConfig tests that Store works correctly without CDC config.
func Test_StoreWithoutCDCConfig(t *testing.T) {
	cfg := NewDBConfig()
	ly := mustMockLayer("localhost:0")
	s := New(&Config{
		DBConf: cfg,
		Dir:    t.TempDir(),
		ID:     "test-node",
	}, nil, ly) // No CDC config

	defer s.Close(true)
	defer ly.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	// CDC should not be initialized
	if s.cdcStreamer != nil {
		t.Fatalf("expected CDC streamer to be nil when no CDCConfig is provided")
	}
}

// Test_StoreCDC_Events_Single tests that CDC events are actually sent when database changes occur.
func Test_StoreCDC_Events_Single(t *testing.T) {
	// Create a channel for CDC events
	cdcChannel := make(chan *proto.CDCIndexedEventGroup, 100)

	// Create CDC config
	cdcConfig := &CDCConfig{
		Ch:         cdcChannel,
		RowIDsOnly: false,
	}

	cfg := NewDBConfig()
	ly := mustMockLayer("localhost:0")
	s := New(&Config{
		DBConf: cfg,
		Dir:    t.TempDir(),
		ID:     "test-node",
	}, cdcConfig, ly)

	defer ly.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)
	_, err := s.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	er := executeRequestFromString(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`, false, false)
	_, _, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute CREATE TABLE on single node: %s", err.Error())
	}

	er = executeRequestFromString(`INSERT INTO foo(id, name) VALUES(101, "fiona")`, false, false)
	_, _, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	timeout := time.After(5 * time.Second)
	var allEvents []*proto.CDCEvent

	// Collect all CDC events
	for {
		select {
		case events := <-cdcChannel:
			if events != nil {
				allEvents = append(allEvents, events.Events...)
			}
		case <-timeout:
			break
		}

		// Look for the INSERT event specifically
		for _, ev := range allEvents {
			if ev.Table == "foo" && ev.Op == proto.CDCEvent_INSERT {
				if ev.NewRowId != 101 {
					t.Fatalf("expected new row ID to be 101, got %d", ev.NewRowId)
				}
				if ev.NewRow.Values[0].GetI() != 101 {
					t.Fatalf("expected new row ID value to be 101, got %d", ev.NewRow.Values[0].GetI())
				}
				if ev.NewRow.Values[1].GetS() != "fiona" {
					t.Fatalf("expected new row name value to be 'fiona', got %s", ev.NewRow.Values[1].GetS())
				}
				return // Test passed
			}
		}
	}

	// If we get here, we didn't find the INSERT event
	t.Fatalf("timeout waiting for CDC INSERT event for table 'foo', got %d total events", len(allEvents))
}

// Test_StoreNewWithCDCConfig tests that a Store can be created with CDC configuration.
func Test_StoreNewWithCDCConfig(t *testing.T) {
	// Create a channel for CDC events
	ch := make(chan *proto.CDCIndexedEventGroup, 10)

	// Create CDC config
	cdcConfig := &CDCConfig{
		Ch:         ch,
		RowIDsOnly: true,
	}

	// Create store with CDC config
	cfg := NewDBConfig()
	ly := mustMockLayer("localhost:0")
	s := New(&Config{
		DBConf: cfg,
		Dir:    t.TempDir(),
		ID:     "test-node",
	}, cdcConfig, ly)

	defer s.Close(true)
	defer ly.Close()

	// Verify that the CDC config was stored
	if s.cdcConfig == nil {
		t.Fatalf("expected CDC config to be stored in the store")
	}
	if s.cdcConfig.Ch == nil {
		t.Fatalf("expected CDC config channel to be set")
	}

	// Test that store can still be created with nil CDC config (backward compatibility)
	s2 := New(&Config{
		DBConf: cfg,
		Dir:    t.TempDir(),
		ID:     "test-node-2",
	}, nil, ly)

	defer s2.Close(true)

	// Verify that nil CDC config is handled correctly
	if s2.cdcConfig != nil {
		t.Fatalf("expected CDC config to be nil when not provided")
	}
}

// Test_StoreWithCDCConfigOpen tests that CDC is properly initialized when Store is opened
func Test_StoreWithCDCConfigOpen(t *testing.T) {
	// Create a channel for CDC events
	ch := make(chan *proto.CDCIndexedEventGroup, 10)

	// Create CDC config
	cdcConfig := &CDCConfig{
		Ch:         ch,
		RowIDsOnly: false,
	}

	// Create and open store with CDC config
	cfg := NewDBConfig()
	ly := mustMockLayer("localhost:0")
	s := New(&Config{
		DBConf: cfg,
		Dir:    t.TempDir(),
		ID:     "test-node",
	}, cdcConfig, ly)

	defer s.Close(true)
	defer ly.Close()

	// Open the store - this should initialize CDC
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	// Verify that CDC streamer was created
	if s.cdcStreamer == nil {
		t.Fatalf("expected CDC streamer to be initialized")
	}

	// Bootstrap the store to enable writes
	if err := s.Bootstrap(NewServer("test-node", ly.Addr().String(), true)); err != nil {
		t.Fatalf("failed to bootstrap store: %v", err)
	}

	// Wait for store to be ready
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to wait for leader: %v", err)
	}
}
