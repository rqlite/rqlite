package snapshot2

import (
	"testing"

	"github.com/hashicorp/raft"
)

func Test_NewSinkCancel(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if sink.ID() != "snap-1234" {
		t.Fatalf("Unexpected ID: %s", sink.ID())
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Failed to cancel unopened sink: %v", err)
	}
}

func Test_NewSinkClose(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if sink.ID() != "snap-1234" {
		t.Fatalf("Unexpected ID: %s", sink.ID())
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to cancel unopened sink: %v", err)
	}
}

func Test_NewSinkOpenCancel(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Failed to cancel opened sink: %v", err)
	}
}

func Test_NewSinkOpenCloseFail(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}
	if err := sink.Close(); err == nil {
		t.Fatalf("Expected error closing opened sink without data")
	}
}

func mustStore(t *testing.T) *Store {
	t.Helper()
	str, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	return str
}

func makeRaftMeta(id string, index, term, cfgIndex uint64) *raft.SnapshotMeta {
	return &raft.SnapshotMeta{
		ID:                 id,
		Index:              index,
		Term:               term,
		Configuration:      makeTestConfiguration("1", "localhost:1"),
		ConfigurationIndex: cfgIndex,
		Version:            1,
	}
}
