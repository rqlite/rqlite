package db

import (
	"reflect"
	"testing"

	command "github.com/rqlite/rqlite/v8/command/proto"
)

func Test_NewCDCStreamer(t *testing.T) {
	ch := make(chan *command.CDCEvents, 10)
	streamer := NewCDCStreamer(ch)
	if streamer == nil {
		t.Fatalf("expected CDCStreamer to be created, got nil")
	}
	if len(streamer.pending.Events) != 0 {
		t.Fatalf("expected no pending events after commit, got %d", len(streamer.pending.Events))
	}
}

func Test_NewCDCStreamer_CommitEmpty(t *testing.T) {
	ch := make(chan *command.CDCEvents, 10)
	streamer := NewCDCStreamer(ch)

	streamer.Reset(1234)
	streamer.CommitHook()
	if len(streamer.pending.Events) != 0 {
		t.Fatalf("expected no pending events after commit, got %d", len(streamer.pending.Events))
	}

	events := <-ch
	if events.K != 1234 {
		t.Fatalf("expected K value to be 1234, got %d", events.K)
	}
	if len(events.Events) != 0 {
		t.Fatalf("expected no events, got %d", len(events.Events))
	}
	if len(ch) != 0 {
		t.Fatalf("expected channel to be empty after commit, got %d", len(ch))
	}
}

func Test_NewCDCStreamer_CommitOne(t *testing.T) {
	ch := make(chan *command.CDCEvents, 10)
	streamer := NewCDCStreamer(ch)

	streamer.Reset(5678)
	change := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_INSERT,
		OldRowId: 100,
		NewRowId: 200,
	}
	if err := streamer.PreupdateHook(change); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	streamer.CommitHook()
	if len(streamer.pending.Events) != 0 {
		t.Fatalf("expected no pending events after commit, got %d", len(streamer.pending.Events))
	}

	ev := <-ch
	if ev.K != 5678 {
		t.Fatalf("expected K value to be 5678, got %d", ev.K)
	}
	if len(ev.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(ev.Events))
	}
	if !reflect.DeepEqual(change, ev.Events[0]) {
		t.Fatalf("received event does not match sent event: expected %v, got %v", change, ev.Events[0])
	}
}

func Test_NewCDCStreamer_CommitTwo(t *testing.T) {
	ch := make(chan *command.CDCEvents, 10)
	streamer := NewCDCStreamer(ch)

	streamer.Reset(9012)
	change1 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_UPDATE,
		OldRowId: 300,
		NewRowId: 400,
	}
	change2 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_DELETE,
		OldRowId: 500,
		NewRowId: 0,
	}

	if err := streamer.PreupdateHook(change1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := streamer.PreupdateHook(change2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	streamer.CommitHook()
	if len(streamer.pending.Events) != 0 {
		t.Fatalf("expected no pending events after commit, got %d", len(streamer.pending.Events))
	}

	ev := <-ch
	if ev.K != 9012 {
		t.Fatalf("expected K value to be 9012, got %d", ev.K)
	}
	if len(ev.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(ev.Events))
	}
	if !reflect.DeepEqual(change1, ev.Events[0]) {
		t.Fatalf("received first event does not match sent event: expected %v, got %v", change1, ev.Events[0])
	}
	if !reflect.DeepEqual(change2, ev.Events[1]) {
		t.Fatalf("received second event does not match sent event: expected %v, got %v", change2, ev.Events[1])
	}
}

// Test_NewCDCStreamer_ResetThenPreupdate tests the behavior of the CDCStreamer
// when  predupdate is called followed by a reset. It ensures that the reset
// clears out any pending events.
func Test_NewCDCStreamer_ResetThenPreupdate(t *testing.T) {
	ch := make(chan *command.CDCEvents, 10)
	streamer := NewCDCStreamer(ch)

	streamer.Reset(1234)
	change1 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_INSERT,
		OldRowId: 100,
		NewRowId: 200,
	}
	if err := streamer.PreupdateHook(change1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if streamer.Len() != 1 {
		t.Fatalf("expected 1 pending event, got %d", streamer.Len())
	}

	streamer.Reset(5678)
	if streamer.Len() != 0 {
		t.Fatalf("expected no pending events after reset, got %d", streamer.Len())
	}
	change2 := &command.CDCEvent{
		Table:    "test_table",
		Op:       command.CDCEvent_UPDATE,
		OldRowId: 300,
		NewRowId: 400,
	}
	if err := streamer.PreupdateHook(change2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	streamer.CommitHook()
	if len(streamer.pending.Events) != 0 {
		t.Fatalf("expected no pending events after commit, got %d", len(streamer.pending.Events))
	}

	ev := <-ch
	if ev.K != 5678 {
		t.Fatalf("expected K value to be 5678, got %d", ev.K)
	}
	if len(ev.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(ev.Events))
	}
	if !reflect.DeepEqual(change2, ev.Events[0]) {
		t.Fatalf("received event does not match sent event: expected %v, got %v", change2, ev.Events[0])
	}
}
