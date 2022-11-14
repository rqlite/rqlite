package store

import (
	"testing"
)

func Test_IsReadOnly(t *testing.T) {
	var servers Servers

	if _, found := servers.IsReadOnly("1"); found {
		t.Fatalf("found should be false")
	}

	nodes := make([]*Server, 1)
	servers = Servers(nodes)
	if _, found := servers.IsReadOnly(""); found {
		t.Fatalf("found should be false")
	}
	if _, found := servers.IsReadOnly("node1"); found {
		t.Fatalf("found should be false")
	}
	nodes[0] = &Server{
		ID:       "node1",
		Addr:     "localhost:4002",
		Suffrage: "Voter",
	}
	if ro, found := servers.IsReadOnly("node1"); ro || !found {
		t.Fatalf("IsReadOnly returned ro: %t, found: %t", ro, found)
	}
	nodes[0] = &Server{
		ID:       "node1",
		Addr:     "localhost:4002",
		Suffrage: "Voter",
	}
	if ro, found := servers.IsReadOnly("node2"); found {
		t.Fatalf("IsReadOnly returned ro: %t, found: %t", ro, found)
	}
	nodes[0] = &Server{
		ID:       "node1",
		Addr:     "localhost:4002",
		Suffrage: "Nonvoter",
	}
	if ro, found := servers.IsReadOnly("node1"); !ro || !found {
		t.Fatalf("servers.IsReadOnly returned ro: %t, found: %t", ro, found)
	}
}
