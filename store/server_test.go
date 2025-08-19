package store

import (
	"sort"
	"testing"
)

func Test_Server_IsReadOnly(t *testing.T) {
	testCases := []struct {
		name          string
		servers       Servers
		nodeID        string
		expectedRO    bool
		expectedFound bool
	}{
		{
			name:          "EmptyServers",
			servers:       nil,
			nodeID:        "1",
			expectedRO:    false,
			expectedFound: false,
		},
		{
			name:          "EmptyNodeID",
			servers:       Servers(make([]*Server, 1)),
			nodeID:        "",
			expectedRO:    false,
			expectedFound: false,
		},
		{
			name: "NonExistentNode",
			servers: Servers([]*Server{
				{ID: "node1", Addr: "localhost:4002", Suffrage: "Voter"},
			}),
			nodeID:        "node2",
			expectedRO:    false,
			expectedFound: false,
		},
		{
			name: "ExistingVoterNode",
			servers: Servers([]*Server{
				{ID: "node1", Addr: "localhost:4002", Suffrage: "Voter"},
			}),
			nodeID:        "node1",
			expectedRO:    false,
			expectedFound: true,
		},
		{
			name: "ExistingNonvoterNode",
			servers: Servers([]*Server{
				{ID: "node1", Addr: "localhost:4002", Suffrage: "Nonvoter"},
			}),
			nodeID:        "node1",
			expectedRO:    true,
			expectedFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ro, found := tc.servers.IsReadOnly(tc.nodeID)
			if ro != tc.expectedRO || found != tc.expectedFound {
				t.Fatalf("IsReadOnly for %s returned ro: %t, found: %t, expected ro: %t, expected found: %t", tc.name, ro, found, tc.expectedRO, tc.expectedFound)
			}
		})
	}
}

func Test_Server_Contains(t *testing.T) {
	testCases := []struct {
		name     string
		servers  Servers
		nodeID   string
		expected bool
	}{
		{
			name:     "EmptyServers",
			servers:  nil,
			nodeID:   "1",
			expected: false,
		},
		{
			name:     "EmptyNodeID",
			servers:  Servers(make([]*Server, 1)),
			nodeID:   "",
			expected: false,
		},
		{
			name: "NonExistentNode",
			servers: Servers([]*Server{
				{ID: "node1", Addr: "localhost:4002", Suffrage: "Voter"},
			}),
			nodeID:   "node2",
			expected: false,
		},
		{
			name: "ExistingNode",
			servers: Servers([]*Server{
				{ID: "node1", Addr: "localhost:4002", Suffrage: "Voter"},
			}),
			nodeID:   "node1",
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.servers.Contains(tc.nodeID)
			if actual != tc.expected {
				t.Fatalf("Contains for %s returned %t, expected %t", tc.name, actual, tc.expected)
			}
		})
	}
}

func Test_Server_Sort(t *testing.T) {
	servers := Servers{
		{ID: "3", Addr: "localhost:4003", Suffrage: "Voter"},
		{ID: "1", Addr: "localhost:4001", Suffrage: "Voter"},
		{ID: "2", Addr: "localhost:4002", Suffrage: "Nonvoter"},
	}
	expectedOrder := []string{"1", "2", "3"}

	sort.Sort(Servers(servers))

	for i, server := range servers {
		if server.ID != expectedOrder[i] {
			t.Fatalf("Expected server ID %s at index %d, got %s", expectedOrder[i], i, server.ID)
		}
	}
}

func Test_Server_Voters(t *testing.T) {
	testCases := []struct {
		name     string
		servers  Servers
		expected []string // Expected IDs of voters
	}{
		{
			name:     "EmptyServers",
			servers:  nil,
			expected: nil,
		},
		{
			name:     "EmptySlice",
			servers:  Servers{},
			expected: []string{},
		},
		{
			name: "AllVoters",
			servers: Servers{
				{ID: "node1", Addr: "localhost:4001", Suffrage: "Voter"},
				{ID: "node2", Addr: "localhost:4002", Suffrage: "Voter"},
			},
			expected: []string{"node1", "node2"},
		},
		{
			name: "AllNonVoters",
			servers: Servers{
				{ID: "node1", Addr: "localhost:4001", Suffrage: "Nonvoter"},
				{ID: "node2", Addr: "localhost:4002", Suffrage: "Nonvoter"},
			},
			expected: []string{},
		},
		{
			name: "MixedServers",
			servers: Servers{
				{ID: "node1", Addr: "localhost:4001", Suffrage: "Voter"},
				{ID: "node2", Addr: "localhost:4002", Suffrage: "Nonvoter"},
				{ID: "node3", Addr: "localhost:4003", Suffrage: "Voter"},
			},
			expected: []string{"node1", "node3"},
		},
		{
			name: "WithNilElements",
			servers: Servers{
				{ID: "node1", Addr: "localhost:4001", Suffrage: "Voter"},
				nil,
				{ID: "node2", Addr: "localhost:4002", Suffrage: "Nonvoter"},
			},
			expected: []string{"node1"},
		},
		{
			name: "LowercaseVoter",
			servers: Servers{
				{ID: "node1", Addr: "localhost:4001", Suffrage: "voter"},
				{ID: "node2", Addr: "localhost:4002", Suffrage: "Nonvoter"},
			},
			expected: []string{"node1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			voters := tc.servers.Voters()

			if tc.expected == nil && voters != nil {
				t.Fatalf("Voters for %s returned %+v, expected nil", tc.name, voters)
			}

			if len(voters) != len(tc.expected) {
				t.Fatalf("Voters for %s returned %d servers, expected %d", tc.name, len(voters), len(tc.expected))
			}

			for i, expectedID := range tc.expected {
				if voters[i].ID != expectedID {
					t.Fatalf("Voters for %s returned server ID %s at index %d, expected %s", tc.name, voters[i].ID, i, expectedID)
				}
			}
		})
	}
}

func Test_Server_NonVoters(t *testing.T) {
	testCases := []struct {
		name     string
		servers  Servers
		expected []string // Expected IDs of non-voters
	}{
		{
			name:     "EmptyServers",
			servers:  nil,
			expected: nil,
		},
		{
			name:     "EmptySlice",
			servers:  Servers{},
			expected: []string{},
		},
		{
			name: "AllVoters",
			servers: Servers{
				{ID: "node1", Addr: "localhost:4001", Suffrage: "Voter"},
				{ID: "node2", Addr: "localhost:4002", Suffrage: "Voter"},
			},
			expected: []string{},
		},
		{
			name: "AllNonVoters",
			servers: Servers{
				{ID: "node1", Addr: "localhost:4001", Suffrage: "Nonvoter"},
				{ID: "node2", Addr: "localhost:4002", Suffrage: "Nonvoter"},
			},
			expected: []string{"node1", "node2"},
		},
		{
			name: "MixedServers",
			servers: Servers{
				{ID: "node1", Addr: "localhost:4001", Suffrage: "Voter"},
				{ID: "node2", Addr: "localhost:4002", Suffrage: "Nonvoter"},
				{ID: "node3", Addr: "localhost:4003", Suffrage: "Voter"},
				{ID: "node4", Addr: "localhost:4004", Suffrage: "Nonvoter"},
			},
			expected: []string{"node2", "node4"},
		},
		{
			name: "WithNilElements",
			servers: Servers{
				{ID: "node1", Addr: "localhost:4001", Suffrage: "Voter"},
				nil,
				{ID: "node2", Addr: "localhost:4002", Suffrage: "Nonvoter"},
			},
			expected: []string{"node2"},
		},
		{
			name: "LowercaseVoterIgnored",
			servers: Servers{
				{ID: "node1", Addr: "localhost:4001", Suffrage: "voter"},
				{ID: "node2", Addr: "localhost:4002", Suffrage: "Nonvoter"},
			},
			expected: []string{"node2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nonVoters := tc.servers.NonVoters()

			if tc.expected == nil && nonVoters != nil {
				t.Fatalf("NonVoters for %s returned %+v, expected nil", tc.name, nonVoters)
			}

			if len(nonVoters) != len(tc.expected) {
				t.Fatalf("NonVoters for %s returned %d servers, expected %d", tc.name, len(nonVoters), len(tc.expected))
			}

			for i, expectedID := range tc.expected {
				if nonVoters[i].ID != expectedID {
					t.Fatalf("NonVoters for %s returned server ID %s at index %d, expected %s", tc.name, nonVoters[i].ID, i, expectedID)
				}
			}
		})
	}
}
