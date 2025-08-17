package store

import (
	"testing"
	"time"
)

func Test_SingleNode_CDCCluster(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	defer s.Close(true)

	cdcCluster := NewCDCCluster(s)
	if cdcCluster == nil {
		t.Fatalf("expected CDCCluster to be created")
	}

	if _, err := s.Noop("test"); err != nil {
		t.Fatalf("failed to execute Noop: %s", err.Error())
	}
}

func Test_MultiNode_CDCCluster(t *testing.T) {
	s0, ln0 := mustNewStore(t)
	defer ln0.Close()
	if err := s0.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s0.Close(true)

	s1, ln1 := mustNewStore(t)
	defer ln1.Close()
	if err := s1.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s1.Close(true)

	// Join stores
	if err := s0.Bootstrap(NewServer(s0.ID(), s0.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s0.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Join the second node to the cluster
	if err := s0.Join(joinRequest(s1.ID(), s1.Addr(), true)); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	if _, err := s1.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Create CDC cluster objects, this is what we are testing.
	cdcCluster0 := NewCDCCluster(s0)
	if cdcCluster0 == nil {
		t.Fatalf("expected CDCCluster to be created")
	}
	cdcCluster1 := NewCDCCluster(s1)
	if cdcCluster1 == nil {
		t.Fatalf("expected CDCCluster to be created")
	}

	// Set a high watermark on the first CDC cluster, ensure it's propagated.
	if err := cdcCluster0.SetHighWatermark(100); err != nil {
		t.Fatalf("failed to set high watermark on CDCCluster: %s", err.Error())
	}

	hwmCh1 := make(chan uint64, 1)
	cdcCluster1.RegisterHWMChange(hwmCh1)
	select {
	case hwm := <-hwmCh1:
		if hwm != 100 {
			t.Fatalf("expected high watermark to be 100, got %d", hwm)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for high watermark change notification")
	}

	// Check that leadership changes are signalled.
	leaderCh0 := make(chan bool, 5)
	cdcCluster0.RegisterLeaderChange(leaderCh0)
	leaderCh1 := make(chan bool, 5)
	cdcCluster1.RegisterLeaderChange(leaderCh1)

	s0.Stepdown(true, s1.raftID)
	if s0.IsLeader() {
		t.Fatalf("expected node 0 to no longer be leader after stepdown")
	}
	testPoll(t, func() bool {
		return s1.IsLeader()
	}, 100*time.Millisecond, 5*time.Second)

	// Read from both channels to ensure we got the leadership change notifications.
	if readLastBoolFromChannel(leaderCh0) {
		t.Fatalf("expected node 0 to be signalled as no longer leader")
	}

	if !readLastBoolFromChannel(leaderCh1) {
		t.Fatalf("expected node 1 to be signalled as leader")
	}

	// Test HWM propagation back to the node that was leader.
	hwmCh0 := make(chan uint64, 1)
	cdcCluster0.RegisterHWMChange(hwmCh0)

	if err := cdcCluster1.SetHighWatermark(200); err != nil {
		t.Fatalf("failed to set high watermark on CDCCluster: %s", err.Error())
	}
	select {
	case hwm := <-hwmCh0:
		if hwm != 200 {
			t.Fatalf("expected high watermark to be 200, got %d", hwm)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for high watermark change notification")
	}
}

// readLastBoolFromChannel reads all values from the channel until it's empty
// returning the last value read. If the channel closes this function will panic.
func readLastBoolFromChannel(ch <-chan bool) bool {
	var last bool
	for {
		select {
		case val, ok := <-ch:
			if !ok {
				panic("channel closed")
			}
			last = val
		default:
			return last
		}
	}
}
