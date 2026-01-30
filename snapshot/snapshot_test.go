package snapshot

import (
	"testing"

	"github.com/hashicorp/raft"
)

// Test Snapshot.Less method
func TestSnapshot_Less(t *testing.T) {
	tests := []struct {
		name     string
		s1Term   uint64
		s1Index  uint64
		s1ID     string
		s2Term   uint64
		s2Index  uint64
		s2ID     string
		expected bool
	}{
		{
			name:     "less by term",
			s1Term:   1,
			s1Index:  10,
			s1ID:     "snapshot-a",
			s2Term:   2,
			s2Index:  5,
			s2ID:     "snapshot-b",
			expected: true,
		},
		{
			name:     "greater by term",
			s1Term:   2,
			s1Index:  10,
			s1ID:     "snapshot-a",
			s2Term:   1,
			s2Index:  20,
			s2ID:     "snapshot-b",
			expected: false,
		},
		{
			name:     "less by index",
			s1Term:   1,
			s1Index:  5,
			s1ID:     "snapshot-a",
			s2Term:   1,
			s2Index:  10,
			s2ID:     "snapshot-b",
			expected: true,
		},
		{
			name:     "greater by index",
			s1Term:   1,
			s1Index:  15,
			s1ID:     "snapshot-a",
			s2Term:   1,
			s2Index:  10,
			s2ID:     "snapshot-b",
			expected: false,
		},
		{
			name:     "less by id",
			s1Term:   1,
			s1Index:  10,
			s1ID:     "snapshot-a",
			s2Term:   1,
			s2Index:  10,
			s2ID:     "snapshot-b",
			expected: true,
		},
		{
			name:     "greater by id",
			s1Term:   1,
			s1Index:  10,
			s1ID:     "snapshot-z",
			s2Term:   1,
			s2Index:  10,
			s2ID:     "snapshot-b",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s1 := &Snapshot{
				id: tt.s1ID,
				raftMeta: &raft.SnapshotMeta{
					Term:  tt.s1Term,
					Index: tt.s1Index,
				},
			}
			s2 := &Snapshot{
				id: tt.s2ID,
				raftMeta: &raft.SnapshotMeta{
					Term:  tt.s2Term,
					Index: tt.s2Index,
				},
			}

			result := s1.Less(s2)
			if result != tt.expected {
				t.Errorf("Less() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test Snapshot.Equal method
func TestSnapshot_Equal(t *testing.T) {
	tests := []struct {
		name     string
		s1Term   uint64
		s1Index  uint64
		s1ID     string
		s2Term   uint64
		s2Index  uint64
		s2ID     string
		expected bool
	}{
		{
			name:     "equal snapshots",
			s1Term:   1,
			s1Index:  10,
			s1ID:     "snapshot-a",
			s2Term:   1,
			s2Index:  10,
			s2ID:     "snapshot-a",
			expected: true,
		},
		{
			name:     "different term",
			s1Term:   1,
			s1Index:  10,
			s1ID:     "snapshot-a",
			s2Term:   2,
			s2Index:  10,
			s2ID:     "snapshot-a",
			expected: false,
		},
		{
			name:     "different index",
			s1Term:   1,
			s1Index:  10,
			s1ID:     "snapshot-a",
			s2Term:   1,
			s2Index:  11,
			s2ID:     "snapshot-a",
			expected: false,
		},
		{
			name:     "different id",
			s1Term:   1,
			s1Index:  10,
			s1ID:     "snapshot-a",
			s2Term:   1,
			s2Index:  10,
			s2ID:     "snapshot-b",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s1 := &Snapshot{
				id: tt.s1ID,
				raftMeta: &raft.SnapshotMeta{
					Term:  tt.s1Term,
					Index: tt.s1Index,
				},
			}
			s2 := &Snapshot{
				id: tt.s2ID,
				raftMeta: &raft.SnapshotMeta{
					Term:  tt.s2Term,
					Index: tt.s2Index,
				},
			}

			result := s1.Equal(s2)
			if result != tt.expected {
				t.Errorf("Equal() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test SnapshotSet.Len method
func TestSnapshotSet_Len(t *testing.T) {
	tests := []struct {
		name     string
		itemsLen int
	}{
		{"empty set", 0},
		{"single item", 1},
		{"multiple items", 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items := make([]*Snapshot, tt.itemsLen)
			for i := 0; i < tt.itemsLen; i++ {
				items[i] = &Snapshot{
					id:       "snapshot-" + string(rune('a'+i)),
					raftMeta: &raft.SnapshotMeta{Term: 1, Index: uint64(i)},
				}
			}
			ss := SnapshotSet{items: items}

			if got := ss.Len(); got != tt.itemsLen {
				t.Errorf("Len() = %v, want %v", got, tt.itemsLen)
			}
		})
	}
}

// Test SnapshotSet.All method
func TestSnapshotSet_All(t *testing.T) {
	items := []*Snapshot{
		{id: "snapshot-1", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
		{id: "snapshot-2", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		{id: "snapshot-3", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
	}
	ss := SnapshotSet{items: items}

	result := ss.All()
	if len(result) != 3 {
		t.Fatalf("All() returned %d items, want 3", len(result))
	}
	for i, snap := range result {
		if snap != items[i] {
			t.Errorf("All()[%d] = %v, want %v", i, snap, items[i])
		}
	}
}

// Test SnapshotSet.IDs method
func TestSnapshotSet_IDs(t *testing.T) {
	items := []*Snapshot{
		{id: "snapshot-1", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
		{id: "snapshot-2", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		{id: "snapshot-3", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
	}
	ss := SnapshotSet{items: items}

	result := ss.IDs()
	expected := []string{"snapshot-1", "snapshot-2", "snapshot-3"}

	if len(result) != len(expected) {
		t.Fatalf("IDs() returned %d items, want %d", len(result), len(expected))
	}
	for i, id := range result {
		if id != expected[i] {
			t.Errorf("IDs()[%d] = %q, want %q", i, id, expected[i])
		}
	}
}

// Test SnapshotSet.RaftMetas method
func TestSnapshotSet_RaftMetas(t *testing.T) {
	items := []*Snapshot{
		{id: "snapshot-1", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
		{id: "snapshot-2", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		{id: "snapshot-3", raftMeta: &raft.SnapshotMeta{Term: 2, Index: 3}},
	}
	ss := SnapshotSet{items: items}

	result := ss.RaftMetas()
	if len(result) != 3 {
		t.Fatalf("RaftMetas() returned %d items, want 3", len(result))
	}
	for i, meta := range result {
		if meta != items[i].raftMeta {
			t.Errorf("RaftMetas()[%d] = %v, want %v", i, meta, items[i].raftMeta)
		}
	}
}

// Test SnapshotSet.Oldest method
func TestSnapshotSet_Oldest(t *testing.T) {
	t.Run("empty set", func(t *testing.T) {
		ss := SnapshotSet{items: []*Snapshot{}}
		snap, ok := ss.Oldest()
		if ok {
			t.Error("Oldest() returned ok=true for empty set")
		}
		if snap != nil {
			t.Error("Oldest() returned non-nil snapshot for empty set")
		}
	})

	t.Run("non-empty set", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
			{id: "snapshot-3", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		}
		ss := SnapshotSet{items: items}

		snap, ok := ss.Oldest()
		if !ok {
			t.Error("Oldest() returned ok=false for non-empty set")
		}
		if snap != items[0] {
			t.Errorf("Oldest() = %v, want %v", snap, items[0])
		}
	})
}

// Test SnapshotSet.Newest method
func TestSnapshotSet_Newest(t *testing.T) {
	t.Run("empty set", func(t *testing.T) {
		ss := SnapshotSet{items: []*Snapshot{}}
		snap, ok := ss.Newest()
		if ok {
			t.Error("Newest() returned ok=true for empty set")
		}
		if snap != nil {
			t.Error("Newest() returned non-nil snapshot for empty set")
		}
	})

	t.Run("non-empty set", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
			{id: "snapshot-3", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		}
		ss := SnapshotSet{items: items}

		snap, ok := ss.Newest()
		if !ok {
			t.Error("Newest() returned ok=false for non-empty set")
		}
		if snap != items[2] {
			t.Errorf("Newest() = %v, want %v", snap, items[2])
		}
	})
}

// Test SnapshotSet.WithID method
func TestSnapshotSet_WithID(t *testing.T) {
	items := []*Snapshot{
		{id: "snapshot-1", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
		{id: "snapshot-2", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		{id: "snapshot-3", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
	}
	ss := SnapshotSet{items: items}

	t.Run("existing id", func(t *testing.T) {
		snap, ok := ss.WithID("snapshot-2")
		if !ok {
			t.Error("WithID() returned ok=false for existing id")
		}
		if snap != items[1] {
			t.Errorf("WithID() = %v, want %v", snap, items[1])
		}
	})

	t.Run("non-existing id", func(t *testing.T) {
		snap, ok := ss.WithID("snapshot-999")
		if ok {
			t.Error("WithID() returned ok=true for non-existing id")
		}
		if snap != nil {
			t.Error("WithID() returned non-nil snapshot for non-existing id")
		}
	})
}

// Test SnapshotSet.BeforeID method
func TestSnapshotSet_BeforeID(t *testing.T) {
	items := []*Snapshot{
		{id: "snapshot-1", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
		{id: "snapshot-2", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		{id: "snapshot-3", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		{id: "snapshot-4", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 4}},
	}
	ss := SnapshotSet{dir: "/test", items: items}

	t.Run("middle id", func(t *testing.T) {
		before := ss.BeforeID("snapshot-3")
		if before.Len() != 2 {
			t.Errorf("BeforeID() returned %d items, want 2", before.Len())
		}
		beforeItems := before.All()
		if beforeItems[0] != items[0] || beforeItems[1] != items[1] {
			t.Error("BeforeID() returned incorrect items")
		}
	})

	t.Run("first id", func(t *testing.T) {
		before := ss.BeforeID("snapshot-1")
		if before.Len() != 0 {
			t.Errorf("BeforeID() for first id returned %d items, want 0", before.Len())
		}
	})

	t.Run("non-existing id", func(t *testing.T) {
		before := ss.BeforeID("snapshot-999")
		if before.Len() != 0 {
			t.Errorf("BeforeID() for non-existing id returned %d items, want 0", before.Len())
		}
	})
}

// Test SnapshotSet.AfterID method
func TestSnapshotSet_AfterID(t *testing.T) {
	items := []*Snapshot{
		{id: "snapshot-1", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
		{id: "snapshot-2", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		{id: "snapshot-3", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		{id: "snapshot-4", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 4}},
	}
	ss := SnapshotSet{dir: "/test", items: items}

	t.Run("middle id", func(t *testing.T) {
		after := ss.AfterID("snapshot-2")
		if after.Len() != 2 {
			t.Errorf("AfterID() returned %d items, want 2", after.Len())
		}
		afterItems := after.All()
		if afterItems[0] != items[2] || afterItems[1] != items[3] {
			t.Error("AfterID() returned incorrect items")
		}
	})

	t.Run("last id", func(t *testing.T) {
		after := ss.AfterID("snapshot-4")
		if after.Len() != 0 {
			t.Errorf("AfterID() for last id returned %d items, want 0", after.Len())
		}
	})

	t.Run("non-existing id", func(t *testing.T) {
		after := ss.AfterID("snapshot-999")
		if after.Len() != 0 {
			t.Errorf("AfterID() for non-existing id returned %d items, want 0", after.Len())
		}
	})
}

// Test SnapshotSet.Range method
func TestSnapshotSet_Range(t *testing.T) {
	items := []*Snapshot{
		{id: "snapshot-1", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
		{id: "snapshot-2", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		{id: "snapshot-3", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		{id: "snapshot-4", raftMeta: &raft.SnapshotMeta{Term: 1, Index: 4}},
	}
	ss := SnapshotSet{dir: "/test", items: items}

	t.Run("valid range", func(t *testing.T) {
		result := ss.Range("snapshot-2", "snapshot-4")
		if result.Len() != 2 {
			t.Errorf("Range() returned %d items, want 2", result.Len())
		}
		rangeItems := result.All()
		if rangeItems[0] != items[1] || rangeItems[1] != items[2] {
			t.Error("Range() returned incorrect items")
		}
	})

	t.Run("empty toID means to end", func(t *testing.T) {
		result := ss.Range("snapshot-2", "")
		if result.Len() != 3 {
			t.Errorf("Range() with empty toID returned %d items, want 3", result.Len())
		}
		if result.All()[0] != items[1] || result.All()[1] != items[2] || result.All()[2] != items[3] {
			t.Error("Range() with empty toID returned incorrect items")
		}
	})

	t.Run("fromID not present", func(t *testing.T) {
		result := ss.Range("snapshot-999", "snapshot-4")
		if result.Len() != 0 {
			t.Errorf("Range() with non-existing fromID returned %d items, want 0", result.Len())
		}
	})

	t.Run("toID not present", func(t *testing.T) {
		result := ss.Range("snapshot-2", "snapshot-999")
		if result.Len() != 0 {
			t.Errorf("Range() with non-existing toID returned %d items, want 0", result.Len())
		}
	})

	t.Run("toID at or before fromID", func(t *testing.T) {
		result := ss.Range("snapshot-3", "snapshot-2")
		if result.Len() != 0 {
			t.Errorf("Range() with toID before fromID returned %d items, want 0", result.Len())
		}

		result = ss.Range("snapshot-2", "snapshot-2")
		if result.Len() != 0 {
			t.Errorf("Range() with toID equal to fromID returned %d items, want 0", result.Len())
		}
	})
}
