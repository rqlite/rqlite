package snapshot

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

// Test Snapshot.Less method
func Test_Snapshot_Less(t *testing.T) {
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
				t.Fatalf("Less() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test Snapshot.Equal method
func Test_Snapshot_Equal(t *testing.T) {
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
				t.Fatalf("Equal() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test SnapshotSet.Len method
func Test_SnapshotSet_Len(t *testing.T) {
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
				t.Fatalf("Len() = %v, want %v", got, tt.itemsLen)
			}
		})
	}
}

// Test SnapshotSet.All method
func Test_SnapshotSet_All(t *testing.T) {
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
			t.Fatalf("All()[%d] = %v, want %v", i, snap, items[i])
		}
	}
}

// Test SnapshotSet.IDs method
func Test_SnapshotSet_IDs(t *testing.T) {
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
			t.Fatalf("IDs()[%d] = %q, want %q", i, id, expected[i])
		}
	}
}

// Test SnapshotSet.RaftMetas method
func Test_SnapshotSet_RaftMetas(t *testing.T) {
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
			t.Fatalf("RaftMetas()[%d] = %v, want %v", i, meta, items[i].raftMeta)
		}
	}
}

// Test SnapshotSet.Oldest method
func Test_SnapshotSet_Oldest(t *testing.T) {
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
			t.Fatalf("Oldest() = %v, want %v", snap, items[0])
		}
	})
}

// Test SnapshotSet.Newest method
func Test_SnapshotSet_Newest(t *testing.T) {
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
			t.Fatalf("Newest() = %v, want %v", snap, items[2])
		}
	})
}

// Test SnapshotSet.NewestFull method
func Test_SnapshotSet_NewestFull(t *testing.T) {
	t.Run("no full snapshots", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		}
		ss := SnapshotSet{items: items}

		snap, ok := ss.NewestFull()
		if ok {
			t.Error("NewestFull() returned ok=true when no full snapshots exist")
		}
		if snap != nil {
			t.Error("NewestFull() returned non-nil snapshot when none exist")
		}
	})

	t.Run("single full snapshot", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		}
		ss := SnapshotSet{items: items}

		snap, ok := ss.NewestFull()
		if !ok {
			t.Error("NewestFull() returned ok=false when full snapshot exists")
		}
		if snap != items[0] {
			t.Fatalf("NewestFull() = %v, want %v", snap, items[0])
		}
	})

	t.Run("multiple full snapshots", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
			{id: "snapshot-3", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
			{id: "snapshot-4", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 4}},
		}
		ss := SnapshotSet{items: items}

		snap, ok := ss.NewestFull()
		if !ok {
			t.Error("NewestFull() returned ok=false when full snapshots exist")
		}
		if snap != items[2] {
			t.Fatalf("NewestFull() = %v, want %v", snap, items[2])
		}
	})
}

// Test SnapshotSet.NewestIncremental method
func Test_SnapshotSet_NewestIncremental(t *testing.T) {
	t.Run("no incremental snapshots", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		}
		ss := SnapshotSet{items: items}

		snap, ok := ss.NewestIncremental()
		if ok {
			t.Error("NewestIncremental() returned ok=true when no incremental snapshots exist")
		}
		if snap != nil {
			t.Error("NewestIncremental() returned non-nil snapshot when none exist")
		}
	})

	t.Run("single incremental snapshot", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		}
		ss := SnapshotSet{items: items}

		snap, ok := ss.NewestIncremental()
		if !ok {
			t.Error("NewestIncremental() returned ok=false when incremental snapshot exists")
		}
		if snap != items[1] {
			t.Fatalf("NewestIncremental() = %v, want %v", snap, items[1])
		}
	})

	t.Run("multiple incremental snapshots", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
			{id: "snapshot-3", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
			{id: "snapshot-4", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 4}},
		}
		ss := SnapshotSet{items: items}

		snap, ok := ss.NewestIncremental()
		if !ok {
			t.Error("NewestIncremental() returned ok=false when incremental snapshots exist")
		}
		if snap != items[3] {
			t.Fatalf("NewestIncremental() = %v, want %v", snap, items[3])
		}
	})
}

// Test SnapshotSet.Fulls method
func Test_SnapshotSet_Fulls(t *testing.T) {
	items := []*Snapshot{
		{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
		{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		{id: "snapshot-3", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		{id: "snapshot-4", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 4}},
	}
	ss := SnapshotSet{dir: "/test", items: items}

	fulls := ss.Fulls()
	if fulls.Len() != 2 {
		t.Fatalf("Fulls() returned %d items, want 2", fulls.Len())
	}
	if fulls.dir != "/test" {
		t.Fatalf("Fulls().dir = %q, want %q", fulls.dir, "/test")
	}

	fullItems := fulls.All()
	if fullItems[0] != items[0] {
		t.Fatalf("Fulls()[0] = %v, want %v", fullItems[0], items[0])
	}
	if fullItems[1] != items[2] {
		t.Fatalf("Fulls()[1] = %v, want %v", fullItems[1], items[2])
	}
}

// Test SnapshotSet.Incrementals method
func Test_SnapshotSet_Incrementals(t *testing.T) {
	items := []*Snapshot{
		{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
		{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		{id: "snapshot-3", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		{id: "snapshot-4", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 4}},
	}
	ss := SnapshotSet{dir: "/test", items: items}

	incrementals := ss.Incrementals()
	if incrementals.Len() != 2 {
		t.Fatalf("Incrementals() returned %d items, want 2", incrementals.Len())
	}
	if incrementals.dir != "/test" {
		t.Fatalf("Incrementals().dir = %q, want %q", incrementals.dir, "/test")
	}

	incItems := incrementals.All()
	if incItems[0] != items[1] {
		t.Fatalf("Incrementals()[0] = %v, want %v", incItems[0], items[1])
	}
	if incItems[1] != items[3] {
		t.Fatalf("Incrementals()[1] = %v, want %v", incItems[1], items[3])
	}
}

// Test SnapshotSet.WithID method
func Test_SnapshotSet_WithID(t *testing.T) {
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
			t.Fatalf("WithID() = %v, want %v", snap, items[1])
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
func Test_SnapshotSet_BeforeID(t *testing.T) {
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
			t.Fatalf("BeforeID() returned %d items, want 2", before.Len())
		}
		beforeItems := before.All()
		if beforeItems[0] != items[0] || beforeItems[1] != items[1] {
			t.Error("BeforeID() returned incorrect items")
		}
	})

	t.Run("first id", func(t *testing.T) {
		before := ss.BeforeID("snapshot-1")
		if before.Len() != 0 {
			t.Fatalf("BeforeID() for first id returned %d items, want 0", before.Len())
		}
	})

	t.Run("non-existing id", func(t *testing.T) {
		before := ss.BeforeID("snapshot-999")
		if before.Len() != 0 {
			t.Fatalf("BeforeID() for non-existing id returned %d items, want 0", before.Len())
		}
	})
}

// Test SnapshotSet.AfterID method
func Test_SnapshotSet_AfterID(t *testing.T) {
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
			t.Fatalf("AfterID() returned %d items, want 2", after.Len())
		}
		afterItems := after.All()
		if afterItems[0] != items[2] || afterItems[1] != items[3] {
			t.Error("AfterID() returned incorrect items")
		}
	})

	t.Run("last id", func(t *testing.T) {
		after := ss.AfterID("snapshot-4")
		if after.Len() != 0 {
			t.Fatalf("AfterID() for last id returned %d items, want 0", after.Len())
		}
	})

	t.Run("non-existing id", func(t *testing.T) {
		after := ss.AfterID("snapshot-999")
		if after.Len() != 0 {
			t.Fatalf("AfterID() for non-existing id returned %d items, want 0", after.Len())
		}
	})
}

// Test SnapshotSet.Range method
func Test_SnapshotSet_Range(t *testing.T) {
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
			t.Fatalf("Range() returned %d items, want 2", result.Len())
		}
		rangeItems := result.All()
		if rangeItems[0] != items[1] || rangeItems[1] != items[2] {
			t.Error("Range() returned incorrect items")
		}
	})

	t.Run("empty toID means to end", func(t *testing.T) {
		result := ss.Range("snapshot-2", "")
		if result.Len() != 3 {
			t.Fatalf("Range() with empty toID returned %d items, want 3", result.Len())
		}
		if result.All()[0] != items[1] || result.All()[1] != items[2] || result.All()[2] != items[3] {
			t.Error("Range() with empty toID returned incorrect items")
		}
	})

	t.Run("fromID not present", func(t *testing.T) {
		result := ss.Range("snapshot-999", "snapshot-4")
		if result.Len() != 0 {
			t.Fatalf("Range() with non-existing fromID returned %d items, want 0", result.Len())
		}
	})

	t.Run("toID not present", func(t *testing.T) {
		result := ss.Range("snapshot-2", "snapshot-999")
		if result.Len() != 0 {
			t.Fatalf("Range() with non-existing toID returned %d items, want 0", result.Len())
		}
	})

	t.Run("toID at or before fromID", func(t *testing.T) {
		result := ss.Range("snapshot-3", "snapshot-2")
		if result.Len() != 0 {
			t.Fatalf("Range() with toID before fromID returned %d items, want 0", result.Len())
		}

		result = ss.Range("snapshot-2", "snapshot-2")
		if result.Len() != 0 {
			t.Fatalf("Range() with toID equal to fromID returned %d items, want 0", result.Len())
		}
	})
}

// Test SnapshotSet.PartitionAtFull method
func Test_SnapshotSet_PartitionAtFull(t *testing.T) {
	t.Run("no full snapshots", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		}
		ss := SnapshotSet{dir: "/test", items: items}

		full, newer := ss.PartitionAtFull()
		if full.Len() != 0 {
			t.Fatalf("PartitionAtFull() with no full snapshots returned full.Len()=%d, want 0", full.Len())
		}
		if newer.Len() != 0 {
			t.Fatalf("PartitionAtFull() with no full snapshots returned newer.Len()=%d, want 0", newer.Len())
		}
	})

	t.Run("full snapshot with no newer", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		}
		ss := SnapshotSet{dir: "/test", items: items}

		full, newer := ss.PartitionAtFull()
		if full.Len() != 1 {
			t.Fatalf("PartitionAtFull() returned full.Len()=%d, want 1", full.Len())
		}
		if full.All()[0] != items[1] {
			t.Fatalf("PartitionAtFull() returned incorrect full snapshot")
		}
		if newer.Len() != 0 {
			t.Fatalf("PartitionAtFull() returned newer.Len()=%d, want 0", newer.Len())
		}
	})

	t.Run("full snapshot with newer incrementals", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
			{id: "snapshot-3", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		}
		ss := SnapshotSet{dir: "/test", items: items}

		full, newer := ss.PartitionAtFull()
		if full.Len() != 1 {
			t.Fatalf("PartitionAtFull() returned full.Len()=%d, want 1", full.Len())
		}
		if full.All()[0] != items[0] {
			t.Fatalf("PartitionAtFull() returned incorrect full snapshot")
		}
		if newer.Len() != 2 {
			t.Fatalf("PartitionAtFull() returned newer.Len()=%d, want 2", newer.Len())
		}
		newerItems := newer.All()
		if newerItems[0] != items[1] || newerItems[1] != items[2] {
			t.Error("PartitionAtFull() returned incorrect newer snapshots")
		}
	})

	t.Run("multiple full snapshots", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
			{id: "snapshot-3", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
			{id: "snapshot-4", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 4}},
		}
		ss := SnapshotSet{dir: "/test", items: items}

		full, newer := ss.PartitionAtFull()
		if full.Len() != 1 {
			t.Fatalf("PartitionAtFull() returned full.Len()=%d, want 1", full.Len())
		}
		if full.All()[0] != items[2] {
			t.Fatalf("PartitionAtFull() returned %v, want newest full snapshot %v", full.All()[0], items[2])
		}
		if newer.Len() != 1 {
			t.Fatalf("PartitionAtFull() returned newer.Len()=%d, want 1", newer.Len())
		}
		if newer.All()[0] != items[3] {
			t.Fatalf("PartitionAtFull() returned %v, want newer snapshot %v", newer.All()[0], items[3])
		}
	})
}

// Test SnapshotSet.ValidateIncrementalChain method
func Test_SnapshotSet_ValidateIncrementalChain(t *testing.T) {
	t.Run("no full snapshot", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		}
		ss := SnapshotSet{items: items}

		err := ss.ValidateIncrementalChain()
		if err == nil {
			t.Error("ValidateIncrementalChain() should return error when no full snapshot present")
		}
	})

	t.Run("valid chain", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
			{id: "snapshot-3", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		}
		ss := SnapshotSet{items: items}

		err := ss.ValidateIncrementalChain()
		if err != nil {
			t.Fatalf("ValidateIncrementalChain() returned unexpected error: %v", err)
		}
	})

	t.Run("full snapshot only", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
		}
		ss := SnapshotSet{items: items}

		err := ss.ValidateIncrementalChain()
		if err != nil {
			t.Fatalf("ValidateIncrementalChain() with only full snapshot returned error: %v", err)
		}
	})

	t.Run("multiple full snapshots with incrementals after newest", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
			{id: "snapshot-3", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
			{id: "snapshot-4", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 4}},
		}
		ss := SnapshotSet{items: items}

		err := ss.ValidateIncrementalChain()
		if err != nil {
			t.Fatalf("ValidateIncrementalChain() returned unexpected error: %v", err)
		}
	})
}

// Test SnapshotCatalog.Scan method with filesystem
func Test_SnapshotCatalog_Scan(t *testing.T) {
	t.Run("empty directory", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() on empty directory returned error: %v", err)
		}
		if ss.Len() != 0 {
			t.Fatalf("Scan() on empty directory returned %d snapshots, want 0", ss.Len())
		}
	})

	t.Run("single full snapshot", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snapshot-1", 1, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 1 {
			t.Fatalf("Scan() returned %d snapshots, want 1", ss.Len())
		}

		snap := ss.All()[0]
		if snap.id != "snapshot-1" {
			t.Fatalf("snapshot id = %q, want %q", snap.id, "snapshot-1")
		}
		if snap.typ != SnapshotTypeFull {
			t.Fatalf("snapshot type = %v, want %v", snap.typ, SnapshotTypeFull)
		}
		if snap.raftMeta.Index != 1 || snap.raftMeta.Term != 1 {
			t.Fatalf("snapshot metadata incorrect: term=%d, index=%d", snap.raftMeta.Term, snap.raftMeta.Index)
		}
	})

	t.Run("single incremental snapshot", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotInc(t, rootDir, "snapshot-1", 1, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 1 {
			t.Fatalf("Scan() returned %d snapshots, want 1", ss.Len())
		}

		snap := ss.All()[0]
		if snap.typ != SnapshotTypeIncremental {
			t.Fatalf("snapshot type = %v, want %v", snap.typ, SnapshotTypeIncremental)
		}
	})

	t.Run("multiple snapshots ordered correctly", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}

		// Create snapshots in non-chronological order
		mustCreateSnapshotFull(t, rootDir, "snapshot-3", 3, 2)
		mustCreateSnapshotFull(t, rootDir, "snapshot-1", 1, 1)
		mustCreateSnapshotInc(t, rootDir, "snapshot-2", 2, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 3 {
			t.Fatalf("Scan() returned %d snapshots, want 3", ss.Len())
		}

		// Check ordering: should be sorted by (Term, Index, ID)
		snapshots := ss.All()
		if snapshots[0].id != "snapshot-1" {
			t.Fatalf("snapshots[0].id = %q, want %q", snapshots[0].id, "snapshot-1")
		}
		if snapshots[1].id != "snapshot-2" {
			t.Fatalf("snapshots[1].id = %q, want %q", snapshots[1].id, "snapshot-2")
		}
		if snapshots[2].id != "snapshot-3" {
			t.Fatalf("snapshots[2].id = %q, want %q", snapshots[2].id, "snapshot-3")
		}
	})

	t.Run("skips temporary directories", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}

		mustCreateSnapshotFull(t, rootDir, "snapshot-1", 1, 1)
		// Create a temp directory
		tmpDir := filepath.Join(rootDir, "snapshot-2.tmp")
		if err := os.MkdirAll(tmpDir, 0755); err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 1 {
			t.Fatalf("Scan() returned %d snapshots, want 1 (should skip .tmp)", ss.Len())
		}
	})

	t.Run("skips non-directory entries", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}

		mustCreateSnapshotFull(t, rootDir, "snapshot-1", 1, 1)
		// Create a regular file in the root
		regularFile := filepath.Join(rootDir, "regular-file.txt")
		if err := os.WriteFile(regularFile, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create regular file: %v", err)
		}

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 1 {
			t.Fatalf("Scan() returned %d snapshots, want 1 (should skip regular files)", ss.Len())
		}
	})

	t.Run("error on missing data file", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}

		// Create snapshot directory with meta.json but no data file
		snapshotDir := filepath.Join(rootDir, "snapshot-1")
		if err := os.MkdirAll(snapshotDir, 0755); err != nil {
			t.Fatalf("failed to create snapshot dir: %v", err)
		}
		meta := &raft.SnapshotMeta{
			ID:    "snapshot-1",
			Index: 1,
			Term:  1,
		}
		metaData, _ := json.Marshal(meta)
		if err := os.WriteFile(metaPath(snapshotDir), metaData, 0644); err != nil {
			t.Fatalf("failed to write meta file: %v", err)
		}

		_, err := catalog.Scan(rootDir)
		if err == nil {
			t.Error("Scan() should return error when data file is missing")
		}
	})

	t.Run("error on missing meta.json", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}

		// Create snapshot directory with data file but no meta.json
		snapshotDir := filepath.Join(rootDir, "snapshot-1")
		if err := os.MkdirAll(snapshotDir, 0755); err != nil {
			t.Fatalf("failed to create snapshot dir: %v", err)
		}
		mustCopyFile(t, "testdata/db-and-wals/full2.db", filepath.Join(snapshotDir, dbfileName))

		_, err := catalog.Scan(rootDir)
		if err == nil {
			t.Error("Scan() should return error when meta.json is missing")
		}
	})

	t.Run("error on non-existent directory", func(t *testing.T) {
		catalog := &SnapshotCatalog{}
		_, err := catalog.Scan("/non/existent/directory")
		if err == nil {
			t.Error("Scan() should return error for non-existent directory")
		}
	})
}

// Test SnapshotSet.ResolveFiles method
func TestSnapshotSet_ResolveFiles(t *testing.T) {
	t.Run("non-existent ID", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		_, _, err = ss.ResolveFiles("no-such-id")
		if err != ErrSnapshotNotFound {
			t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
		}
	})

	t.Run("single full snapshot", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		dbFile, walFiles, err := ss.ResolveFiles("snap-1")
		if err != nil {
			t.Fatalf("ResolveFiles() returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 0 {
			t.Fatalf("expected 0 WAL files, got %d", len(walFiles))
		}
	})

	t.Run("full then one incremental", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)
		mustCreateSnapshotInc(t, rootDir, "snap-2", 2, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		// Resolve the full snapshot — no WALs.
		dbFile, walFiles, err := ss.ResolveFiles("snap-1")
		if err != nil {
			t.Fatalf("ResolveFiles(snap-1) returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 0 {
			t.Fatalf("expected 0 WAL files, got %d", len(walFiles))
		}

		// Resolve the incremental — DB from full, one WAL.
		dbFile, walFiles, err = ss.ResolveFiles("snap-2")
		if err != nil {
			t.Fatalf("ResolveFiles(snap-2) returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 1 {
			t.Fatalf("expected 1 WAL file, got %d", len(walFiles))
		}
		if exp := filepath.Join(rootDir, "snap-2", walFileName(1)); walFiles[0] != exp {
			t.Fatalf("walFiles[0] = %q, want %q", walFiles[0], exp)
		}
	})

	t.Run("full then multiple incrementals", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)
		mustCreateSnapshotInc(t, rootDir, "snap-2", 2, 1)
		mustCreateSnapshotInc(t, rootDir, "snap-3", 3, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		// Resolve middle incremental — one WAL.
		dbFile, walFiles, err := ss.ResolveFiles("snap-2")
		if err != nil {
			t.Fatalf("ResolveFiles(snap-2) returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 1 {
			t.Fatalf("expected 1 WAL file, got %d", len(walFiles))
		}

		// Resolve latest incremental — two WALs in order.
		dbFile, walFiles, err = ss.ResolveFiles("snap-3")
		if err != nil {
			t.Fatalf("ResolveFiles(snap-3) returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 2 {
			t.Fatalf("expected 2 WAL files, got %d", len(walFiles))
		}
		if exp := filepath.Join(rootDir, "snap-2", walFileName(1)); walFiles[0] != exp {
			t.Fatalf("walFiles[0] = %q, want %q", walFiles[0], exp)
		}
		if exp := filepath.Join(rootDir, "snap-3", walFileName(1)); walFiles[1] != exp {
			t.Fatalf("walFiles[1] = %q, want %q", walFiles[1], exp)
		}
	})

	t.Run("two fulls with incrementals uses nearest full", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)
		mustCreateSnapshotInc(t, rootDir, "snap-2", 2, 1)
		mustCreateSnapshotFull(t, rootDir, "snap-3", 3, 1)
		mustCreateSnapshotInc(t, rootDir, "snap-4", 4, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		// snap-4 should resolve against snap-3 (nearest full), not snap-1.
		dbFile, walFiles, err := ss.ResolveFiles("snap-4")
		if err != nil {
			t.Fatalf("ResolveFiles(snap-4) returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-3", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 1 {
			t.Fatalf("expected 1 WAL file, got %d", len(walFiles))
		}
		if exp := filepath.Join(rootDir, "snap-4", walFileName(1)); walFiles[0] != exp {
			t.Fatalf("walFiles[0] = %q, want %q", walFiles[0], exp)
		}
	})

	t.Run("incremental with no preceding full", func(t *testing.T) {
		// Construct a SnapshotSet directly with only an incremental snapshot.
		ss := SnapshotSet{
			dir: "/test",
			items: []*Snapshot{
				{id: "snap-1", typ: SnapshotTypeIncremental, path: "/test/snap-1",
					raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			},
		}

		_, _, err := ss.ResolveFiles("snap-1")
		if err == nil {
			t.Fatal("expected error when no full snapshot precedes incremental, got nil")
		}
	})

	t.Run("empty set", func(t *testing.T) {
		ss := SnapshotSet{}
		_, _, err := ss.ResolveFiles("anything")
		if err != ErrSnapshotNotFound {
			t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
		}
	})
}

// Test SnapshotCatalog.Scan recognizes noop snapshots
func Test_SnapshotCatalog_Scan_Noop(t *testing.T) {
	rootDir := t.TempDir()
	catalog := &SnapshotCatalog{}
	mustCreateSnapshotNoop(t, rootDir, "snapshot-1", 1, 1)

	ss, err := catalog.Scan(rootDir)
	if err != nil {
		t.Fatalf("Scan() returned error: %v", err)
	}
	if ss.Len() != 1 {
		t.Fatalf("Scan() returned %d snapshots, want 1", ss.Len())
	}

	snap := ss.All()[0]
	if snap.typ != SnapshotTypeNoop {
		t.Fatalf("snapshot type = %v, want %v", snap.typ, SnapshotTypeNoop)
	}
}

// Test ValidateIncrementalChain allows noop snapshots after full
func Test_SnapshotSet_ValidateIncrementalChain_WithNoop(t *testing.T) {
	t.Run("full then noop", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeNoop, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
		}
		ss := SnapshotSet{items: items}
		if err := ss.ValidateIncrementalChain(); err != nil {
			t.Fatalf("ValidateIncrementalChain() returned unexpected error: %v", err)
		}
	})

	t.Run("full then incremental then noop", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
			{id: "snapshot-3", typ: SnapshotTypeNoop, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		}
		ss := SnapshotSet{items: items}
		if err := ss.ValidateIncrementalChain(); err != nil {
			t.Fatalf("ValidateIncrementalChain() returned unexpected error: %v", err)
		}
	})

	t.Run("full then noop then incremental", func(t *testing.T) {
		items := []*Snapshot{
			{id: "snapshot-1", typ: SnapshotTypeFull, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 1}},
			{id: "snapshot-2", typ: SnapshotTypeNoop, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 2}},
			{id: "snapshot-3", typ: SnapshotTypeIncremental, raftMeta: &raft.SnapshotMeta{Term: 1, Index: 3}},
		}
		ss := SnapshotSet{items: items}
		if err := ss.ValidateIncrementalChain(); err != nil {
			t.Fatalf("ValidateIncrementalChain() returned unexpected error: %v", err)
		}
	})
}

// Test ResolveFiles with noop snapshots
func Test_SnapshotSet_ResolveFiles_WithNoop(t *testing.T) {
	t.Run("full then noop resolves to just db", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)
		mustCreateSnapshotNoop(t, rootDir, "snap-2", 2, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		dbFile, walFiles, err := ss.ResolveFiles("snap-2")
		if err != nil {
			t.Fatalf("ResolveFiles() returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 0 {
			t.Fatalf("expected 0 WAL files for noop, got %d", len(walFiles))
		}
	})

	t.Run("full then incremental then noop", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)
		mustCreateSnapshotInc(t, rootDir, "snap-2", 2, 1)
		mustCreateSnapshotNoop(t, rootDir, "snap-3", 3, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		// Resolve the noop — should get DB from full and WAL from incremental.
		dbFile, walFiles, err := ss.ResolveFiles("snap-3")
		if err != nil {
			t.Fatalf("ResolveFiles() returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 1 {
			t.Fatalf("expected 1 WAL file, got %d", len(walFiles))
		}
		if exp := filepath.Join(rootDir, "snap-2", walFileName(1)); walFiles[0] != exp {
			t.Fatalf("walFiles[0] = %q, want %q", walFiles[0], exp)
		}
	})

	t.Run("full then noop then incremental", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)
		mustCreateSnapshotNoop(t, rootDir, "snap-2", 2, 1)
		mustCreateSnapshotInc(t, rootDir, "snap-3", 3, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		// Resolve the incremental — should skip noop, get DB from full and WAL from incremental.
		dbFile, walFiles, err := ss.ResolveFiles("snap-3")
		if err != nil {
			t.Fatalf("ResolveFiles() returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 1 {
			t.Fatalf("expected 1 WAL file, got %d", len(walFiles))
		}
		if exp := filepath.Join(rootDir, "snap-3", walFileName(1)); walFiles[0] != exp {
			t.Fatalf("walFiles[0] = %q, want %q", walFiles[0], exp)
		}
	})
}

func mustCopyFile(t *testing.T, src, dest string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("failed to read file %s: %v", src, err)
	}
	if err := os.WriteFile(dest, data, 0644); err != nil {
		t.Fatalf("failed to write file %s: %v", dest, err)
	}
}

func mustCreateSnapshotFull(t *testing.T, rootDir, snapshotID string, idx, term uint64) {
	mustCreateSnapshot(t, rootDir, snapshotID, "testdata/db-and-wals/full2.db", dbfileName, idx, term)
}

func mustCreateSnapshotInc(t *testing.T, rootDir, snapshotID string, idx, term uint64) {
	mustCreateSnapshot(t, rootDir, snapshotID, "testdata/db-and-wals/wal-00", walFileName(1), idx, term)
}

func mustCreateSnapshotNoop(t *testing.T, rootDir, snapshotID string, idx, term uint64) {
	t.Helper()
	snapshotDir := filepath.Join(rootDir, snapshotID)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatalf("failed to create snapshot dir: %v", err)
	}
	// Create the data.noop sentinel file.
	noopPath := filepath.Join(snapshotDir, noopfileName)
	f, err := os.Create(noopPath)
	if err != nil {
		t.Fatalf("failed to create data.noop: %v", err)
	}
	f.Close()

	meta := &raft.SnapshotMeta{
		ID:    snapshotID,
		Index: idx,
		Term:  term,
	}
	if err := writeMeta(snapshotDir, meta); err != nil {
		t.Fatalf("failed to write snapshot meta: %v", err)
	}
}

func mustCreateSnapshot(t *testing.T, rootDir string, snapshotID, srcName, dstName string, idx, term uint64) {
	snapshotDir := filepath.Join(rootDir, snapshotID)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatalf("failed to create snapshot dir: %v", err)
	}

	mustCopyFile(t, srcName, filepath.Join(snapshotDir, dstName))
	meta := &raft.SnapshotMeta{
		ID:    snapshotID,
		Index: idx,
		Term:  term,
	}
	if err := writeMeta(snapshotDir, meta); err != nil {
		t.Fatalf("failed to write snapshot meta: %v", err)
	}
}

// mustCreateSnapshotIncMulti creates an incremental snapshot directory with
// multiple WAL files named 00000001.wal, 00000002.wal, etc.
func mustCreateSnapshotIncMulti(t *testing.T, rootDir, snapshotID string, idx, term uint64, walSrcs ...string) {
	t.Helper()
	snapshotDir := filepath.Join(rootDir, snapshotID)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatalf("failed to create snapshot dir: %v", err)
	}
	for i, src := range walSrcs {
		mustCopyFile(t, src, filepath.Join(snapshotDir, walFileName(i+1)))
	}
	meta := &raft.SnapshotMeta{
		ID:    snapshotID,
		Index: idx,
		Term:  term,
	}
	if err := writeMeta(snapshotDir, meta); err != nil {
		t.Fatalf("failed to write snapshot meta: %v", err)
	}
}

// Test SnapshotCatalog.Scan with multi-WAL incremental snapshots
func Test_SnapshotCatalog_Scan_MultiWAL(t *testing.T) {
	t.Run("two WAL files", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotIncMulti(t, rootDir, "snap-1", 1, 1,
			"testdata/db-and-wals/wal-00", "testdata/db-and-wals/wal-01")

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 1 {
			t.Fatalf("Scan() returned %d snapshots, want 1", ss.Len())
		}

		snap := ss.All()[0]
		if snap.typ != SnapshotTypeIncremental {
			t.Fatalf("snapshot type = %v, want %v", snap.typ, SnapshotTypeIncremental)
		}
		if len(snap.walFiles) != 2 {
			t.Fatalf("expected 2 WAL files, got %d", len(snap.walFiles))
		}
	})

	t.Run("three WAL files", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotIncMulti(t, rootDir, "snap-1", 1, 1,
			"testdata/db-and-wals/wal-00", "testdata/db-and-wals/wal-01", "testdata/db-and-wals/wal-02")

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 1 {
			t.Fatalf("Scan() returned %d snapshots, want 1", ss.Len())
		}

		snap := ss.All()[0]
		if snap.typ != SnapshotTypeIncremental {
			t.Fatalf("snapshot type = %v, want %v", snap.typ, SnapshotTypeIncremental)
		}
		if len(snap.walFiles) != 3 {
			t.Fatalf("expected 3 WAL files, got %d", len(snap.walFiles))
		}
	})

	t.Run("WAL files with data.db is rejected", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}

		// Create a snapshot directory with both a DB and a WAL file.
		snapshotDir := filepath.Join(rootDir, "snap-1")
		if err := os.MkdirAll(snapshotDir, 0755); err != nil {
			t.Fatalf("failed to create snapshot dir: %v", err)
		}
		mustCopyFile(t, "testdata/db-and-wals/full2.db", filepath.Join(snapshotDir, dbfileName))
		mustCopyFile(t, "testdata/db-and-wals/wal-00", filepath.Join(snapshotDir, walFileName(1)))
		meta := &raft.SnapshotMeta{ID: "snap-1", Index: 1, Term: 1}
		if err := writeMeta(snapshotDir, meta); err != nil {
			t.Fatalf("failed to write meta: %v", err)
		}

		_, err := catalog.Scan(rootDir)
		if err == nil {
			t.Fatal("expected error when snapshot has both WAL and DB files, got nil")
		}
	})
}

// Test ResolveFiles with multi-WAL incremental snapshots
func Test_SnapshotSet_ResolveFiles_MultiWAL(t *testing.T) {
	t.Run("full then two-WAL incremental", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)
		mustCreateSnapshotIncMulti(t, rootDir, "snap-2", 2, 1,
			"testdata/db-and-wals/wal-00", "testdata/db-and-wals/wal-01")

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		dbFile, walFiles, err := ss.ResolveFiles("snap-2")
		if err != nil {
			t.Fatalf("ResolveFiles() returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 2 {
			t.Fatalf("expected 2 WAL files, got %d", len(walFiles))
		}
		if exp := filepath.Join(rootDir, "snap-2", walFileName(1)); walFiles[0] != exp {
			t.Fatalf("walFiles[0] = %q, want %q", walFiles[0], exp)
		}
		if exp := filepath.Join(rootDir, "snap-2", walFileName(2)); walFiles[1] != exp {
			t.Fatalf("walFiles[1] = %q, want %q", walFiles[1], exp)
		}
	})

	t.Run("full then three-WAL incremental", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)
		mustCreateSnapshotIncMulti(t, rootDir, "snap-2", 2, 1,
			"testdata/db-and-wals/wal-00", "testdata/db-and-wals/wal-01", "testdata/db-and-wals/wal-02")

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		dbFile, walFiles, err := ss.ResolveFiles("snap-2")
		if err != nil {
			t.Fatalf("ResolveFiles() returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 3 {
			t.Fatalf("expected 3 WAL files, got %d", len(walFiles))
		}
		for i := 0; i < 3; i++ {
			if exp := filepath.Join(rootDir, "snap-2", walFileName(i+1)); walFiles[i] != exp {
				t.Fatalf("walFiles[%d] = %q, want %q", i, walFiles[i], exp)
			}
		}
	})

	t.Run("full then single-WAL inc then two-WAL inc", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)
		mustCreateSnapshotInc(t, rootDir, "snap-2", 2, 1)
		mustCreateSnapshotIncMulti(t, rootDir, "snap-3", 3, 1,
			"testdata/db-and-wals/wal-01", "testdata/db-and-wals/wal-02")

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		// Resolve snap-3: should get DB from snap-1, WAL from snap-2 (1 file),
		// and WALs from snap-3 (2 files) = 3 total WAL files.
		dbFile, walFiles, err := ss.ResolveFiles("snap-3")
		if err != nil {
			t.Fatalf("ResolveFiles() returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 3 {
			t.Fatalf("expected 3 WAL files, got %d", len(walFiles))
		}
		// First WAL from snap-2 (single-WAL incremental).
		if exp := filepath.Join(rootDir, "snap-2", walFileName(1)); walFiles[0] != exp {
			t.Fatalf("walFiles[0] = %q, want %q", walFiles[0], exp)
		}
		// Next two WALs from snap-3 (multi-WAL incremental).
		if exp := filepath.Join(rootDir, "snap-3", walFileName(1)); walFiles[1] != exp {
			t.Fatalf("walFiles[1] = %q, want %q", walFiles[1], exp)
		}
		if exp := filepath.Join(rootDir, "snap-3", walFileName(2)); walFiles[2] != exp {
			t.Fatalf("walFiles[2] = %q, want %q", walFiles[2], exp)
		}
	})

	t.Run("full then two-WAL inc then noop", func(t *testing.T) {
		rootDir := t.TempDir()
		catalog := &SnapshotCatalog{}
		mustCreateSnapshotFull(t, rootDir, "snap-1", 1, 1)
		mustCreateSnapshotIncMulti(t, rootDir, "snap-2", 2, 1,
			"testdata/db-and-wals/wal-00", "testdata/db-and-wals/wal-01")
		mustCreateSnapshotNoop(t, rootDir, "snap-3", 3, 1)

		ss, err := catalog.Scan(rootDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}

		// Resolve the noop — should get DB from full, 2 WALs from incremental.
		dbFile, walFiles, err := ss.ResolveFiles("snap-3")
		if err != nil {
			t.Fatalf("ResolveFiles() returned error: %v", err)
		}
		if exp := filepath.Join(rootDir, "snap-1", dbfileName); dbFile != exp {
			t.Fatalf("dbFile = %q, want %q", dbFile, exp)
		}
		if len(walFiles) != 2 {
			t.Fatalf("expected 2 WAL files, got %d", len(walFiles))
		}
	})
}
