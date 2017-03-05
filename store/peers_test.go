package store

import (
	"os"
	"sort"
	"testing"
	"time"
)

// Test_NumPeersEnableSingle tests that a single node reports
// itself as capable of joining a cluster.
func Test_NumPeersEnableSingle(t *testing.T) {
	s0 := mustNewStore(true)
	defer os.RemoveAll(s0.Path())
	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open node for num peers test: %s", err.Error())
	}
	s0.WaitForLeader(5 * time.Second)
	s0.Close(true)

	j, err := JoinAllowed(s0.Path())
	if err != nil {
		t.Fatalf("failed to check join status of %s: %s", s0.Path(), err.Error())
	}
	if !j {
		t.Fatalf("config files at %s indicate joining is not allowed", s0.Path())
	}
}

// Test_NumPeersDisableSingle tests that a single node reports
// itself as capable of joining a cluster, when explicitly configured
// as not capable of self-electing.
func Test_NumPeersDisableSingle(t *testing.T) {
	s0 := mustNewStore(true)
	defer os.RemoveAll(s0.Path())
	if err := s0.Open(false); err != nil {
		t.Fatalf("failed to open node for num peers test: %s", err.Error())
	}
	s0.Close(true)

	j, err := JoinAllowed(s0.Path())
	if err != nil {
		t.Fatalf("failed to check join status of %s: %s", s0.Path(), err.Error())
	}
	if !j {
		t.Fatalf("config files at %s indicate joining is not allowed", s0.Path())
	}
}

// Test_NumPeersJoin tests that the correct number of nodes are recorded by
// nodes in a cluster.
func Test_NumPeersJoin(t *testing.T) {
	s0 := mustNewStore(true)
	defer os.RemoveAll(s0.Path())
	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open node for num peers test: %s", err.Error())
	}
	s0.WaitForLeader(5 * time.Second)

	s1 := mustNewStore(true)
	defer os.RemoveAll(s1.Path())
	if err := s1.Open(false); err != nil {
		t.Fatalf("failed to open node for num peers test: %s", err.Error())
	}

	// Get sorted list of cluster nodes.
	storeNodes := []string{s0.Addr().String(), s1.Addr().String()}
	sort.StringSlice(storeNodes).Sort()

	// Join the second node to the first.
	if err := s0.Join(s1.Addr().String()); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr().String(), err.Error())
	}
	s1.WaitForLeader(5 * time.Second)
	s1.Close(true)
	s0.Close(true)

	// Check that peers are set as expected.
	m, _ := NumPeers(s0.Path())
	if m != 2 {
		t.Fatalf("got wrong value for number of peers, exp %d, got %d", 2, m)
	}

	j, err := JoinAllowed(s0.Path())
	if err != nil {
		t.Fatalf("failed to check join status of %s: %s", s0.Path(), err.Error())
	}
	if j {
		t.Fatalf("config files at %s indicate joining is allowed", s0.Path())
	}

	k, err := JoinAllowed(s1.Path())
	if err != nil {
		t.Fatalf("failed to check join status of %s: %s", s1.Path(), err.Error())
	}
	if k {
		t.Fatalf("config files at %s indicate joining is allowed", s1.Path())
	}
}
