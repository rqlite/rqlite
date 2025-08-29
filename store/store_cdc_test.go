package store

import (
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
)

// Test_StoreEnableCDC tests that CDC can be enabled and disabled on the Store.
func Test_StoreEnableCDC(t *testing.T) {
	s, ln := mustNewStore(t)
	defer s.Close(true)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	// Initially CDC should be nil
	if s.cdcStreamer != nil {
		t.Fatalf("expected CDC streamer to be nil initially")
	}

	// Create a channel for CDC events
	ch := make(chan *proto.CDCIndexedEventGroup, 10)

	// Enable CDC
	if err := s.EnableCDC(ch, false); err != nil {
		t.Fatalf("failed to enable CDC: %v", err)
	}
	if s.cdcStreamer == nil {
		t.Fatalf("expected CDC streamer to be created after EnableCDC")
	}

	// Disable CDC
	if err := s.DisableCDC(); err != nil {
		t.Fatalf("failed to disable CDC: %v", err)
	}
	if s.cdcStreamer != nil {
		t.Fatalf("expected CDC streamer to be nil after DisableCDC")
	}
}

// Test_StoreEnableDisableCDC tests that CDC can be enabled and disabled multiple times.
func Test_StoreEnableDisableCDC(t *testing.T) {
	s, ln := mustNewStore(t)
	defer s.Close(true)
	defer ln.Close()

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	ch1 := make(chan *proto.CDCIndexedEventGroup, 10)
	ch2 := make(chan *proto.CDCIndexedEventGroup, 10)

	// Enable CDC with first channel
	if err := s.EnableCDC(ch1, false); err != nil {
		t.Fatalf("failed to enable CDC: %v", err)
	}
	if s.cdcStreamer == nil {
		t.Fatalf("expected CDC streamer to be created")
	}

	// Enable CDC with second channel (should return error)
	if err := s.EnableCDC(ch2, false); err != ErrCDCEnabled {
		t.Fatalf("expected ErrCDCEnabled, got: %v", err)
	}
	if s.cdcStreamer == nil {
		t.Fatalf("expected CDC streamer to still be created")
	}

	// Disable CDC
	if err := s.DisableCDC(); err != nil {
		t.Fatalf("failed to disable CDC: %v", err)
	}
	if s.cdcStreamer != nil {
		t.Fatalf("expected CDC streamer to be nil after disable")
	}

	// Enable again
	if err := s.EnableCDC(ch1, false); err != nil {
		t.Fatalf("failed to enable CDC again: %v", err)
	}
	if s.cdcStreamer == nil {
		t.Fatalf("expected CDC streamer to be created again")
	}
}

// Test_StoreCDC_Events_Single tests that CDC events are actually sent when database changes occur.
func Test_StoreCDC_Events_Single(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()

	// Create a channel for CDC events
	cdcChannel := make(chan *proto.CDCIndexedEventGroup, 100)

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
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	if err := s.EnableCDC(cdcChannel, false); err != nil {
		t.Fatalf("failed to enable CDC: %v", err)
	}
	er = executeRequestFromString(`INSERT INTO foo(id, name) VALUES(101, "fiona")`, false, false)
	_, _, err = s.Execute(er)
	if err != nil {
		t.Fatalf("failed to execute INSERT on single node: %s", err.Error())
	}

	timeout := time.After(5 * time.Second)
	select {
	case events := <-cdcChannel:
		if events == nil {
			t.Fatalf("received nil CDC events")
		}
		if len(events.Events) != 1 {
			t.Fatalf("expected 1 CDC event, got %d", len(events.Events))
		}
		ev := events.Events[0]

		if ev.Table != "foo" {
			t.Fatalf("expected table name to be 'foo', got %s", ev.Table)
		}
		if ev.Op != proto.CDCEvent_INSERT {
			t.Fatalf("expected CDC event operation to be INSERT, got %s", ev.Op)
		}
		if ev.NewRowId != 101 {
			t.Fatalf("expected new row ID to be 101, got %d", ev.NewRowId)
		}
		if ev.NewRow.Values[0].GetI() != 101 {
			t.Fatalf("expected new row ID value to be 1, got %d", ev.NewRow.Values[0].GetI())
		}
		if ev.NewRow.Values[1].GetS() != "fiona" {
			t.Fatalf("expected new row name value to be 'fiona', got %s", ev.NewRow.Values[1].GetS())
			break
		}
	case <-timeout:
		t.Fatalf("timeout waiting for CDC INSERT event for table 'foo'")
	}
}

// Test_StoreCDCNotOpen tests that EnableCDC returns ErrNotOpen when store is not open.
func Test_StoreCDCNotOpen(t *testing.T) {
	s, ln := mustNewStore(t)
	defer s.Close(true)
	defer ln.Close()

	// Create a channel for CDC events
	ch := make(chan *proto.CDCIndexedEventGroup, 10)

	// Try to enable CDC on a closed store - should return ErrNotOpen
	if err := s.EnableCDC(ch, false); err != ErrNotOpen {
		t.Fatalf("expected ErrNotOpen, got: %v", err)
	}
}
