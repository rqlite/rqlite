package store

import (
	"testing"
)

func Test_IsReadOnly(t *testing.T) {
	var servers []*Server

	if _, found := IsReadOnly(nil, "1"); found {
		t.Fatalf("found should be false")
	}

	servers = make([]*Server, 1)
	if _, found := IsReadOnly(servers, ""); found {
		t.Fatalf("found should be false")
	}
	if _, found := IsReadOnly(servers, "node1"); found {
		t.Fatalf("found should be false")
	}
	servers[0] = &Server{
		ID:       "node1",
		Addr:     "localhost:4002",
		Suffrage: "Voter",
	}
	if ro, found := IsReadOnly(servers, "node1"); ro || !found {
		t.Fatalf("IsReadOnly returned ro: %t, found: %t", ro, found)
	}
	servers[0] = &Server{
		ID:       "node1",
		Addr:     "localhost:4002",
		Suffrage: "Voter",
	}
	if ro, found := IsReadOnly(servers, "node2"); found {
		t.Fatalf("IsReadOnly returned ro: %t, found: %t", ro, found)
	}
	servers[0] = &Server{
		ID:       "node1",
		Addr:     "localhost:4002",
		Suffrage: "Nonvoter",
	}
	if ro, found := IsReadOnly(servers, "node1"); !ro || !found {
		t.Fatalf("IsReadOnly returned ro: %t, found: %t", ro, found)
	}
}
