package store

import (
	"sort"
	"testing"
)

func TestServer_Sort(t *testing.T) {
	servers := []*Server{
		{"node3", "addr4"},
		{"node2", "addr3"},
		{"node0", "addr0"},
		{"node1", "addr1"},
	}

	sort.Sort(Servers(servers))

	if servers[0].ID != "node0" ||
		servers[1].ID != "node1" ||
		servers[2].ID != "node2" ||
		servers[3].ID != "node3" {
		t.Fatal("servers sorted incorrectly")
	}
}
