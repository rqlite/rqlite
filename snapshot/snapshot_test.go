package snapshot

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

func createTestSnapshot(t *testing.T, baseDir, snapshotID string, term uint64, index uint64) {
	t.Helper()
	snapshotDir := filepath.Join(baseDir, snapshotID)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatalf("failed to create snapshot directory: %v", err)
	}

	meta := &raft.SnapshotMeta{
		ID:    snapshotID,
		Term:  term,
		Index: index,
	}

	metaPath := filepath.Join(snapshotDir, "meta.json")
	fh, err := os.Create(metaPath)
	if err != nil {
		t.Fatalf("failed to create meta.json: %v", err)
	}
	defer fh.Close()

	enc := json.NewEncoder(fh)
	if err := enc.Encode(meta); err != nil {
		t.Fatalf("failed to encode meta.json: %v", err)
	}
}

func createTestSnapshotDir(t *testing.T, baseDir, snapshotID string) {
	t.Helper()
	snapshotDir := filepath.Join(baseDir, snapshotID)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatalf("failed to create snapshot directory: %v", err)
	}
}

func createTestFile(t *testing.T, baseDir, filename, content string) {
	t.Helper()
	filePath := filepath.Join(baseDir, filename)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		t.Fatalf("failed to create parent directory: %v", err)
	}
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
}

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
					ID:    tt.s1ID,
					Term:  tt.s1Term,
					Index: tt.s1Index,
				},
			}
			s2 := &Snapshot{
				id: tt.s2ID,
				raftMeta: &raft.SnapshotMeta{
					ID:    tt.s2ID,
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
					ID:    tt.s1ID,
					Term:  tt.s1Term,
					Index: tt.s1Index,
				},
			}
			s2 := &Snapshot{
				id: tt.s2ID,
				raftMeta: &raft.SnapshotMeta{
					ID:    tt.s2ID,
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

// Test Snapshot.LessThanMeta method
func TestSnapshot_LessThanMeta(t *testing.T) {
	tests := []struct {
		name     string
		snapTerm uint64
		snapIdx  uint64
		snapID   string
		metaTerm uint64
		metaIdx  uint64
		metaID   string
		expected bool
	}{
		{
			name:     "less by term",
			snapTerm: 1,
			snapIdx:  10,
			snapID:   "snap-a",
			metaTerm: 2,
			metaIdx:  5,
			metaID:   "snap-b",
			expected: true,
		},
		{
			name:     "greater by term",
			snapTerm: 2,
			snapIdx:  10,
			snapID:   "snap-a",
			metaTerm: 1,
			metaIdx:  20,
			metaID:   "snap-b",
			expected: false,
		},
		{
			name:     "less by index",
			snapTerm: 1,
			snapIdx:  5,
			snapID:   "snap-a",
			metaTerm: 1,
			metaIdx:  10,
			metaID:   "snap-b",
			expected: true,
		},
		{
			name:     "greater by index",
			snapTerm: 1,
			snapIdx:  15,
			snapID:   "snap-a",
			metaTerm: 1,
			metaIdx:  10,
			metaID:   "snap-b",
			expected: false,
		},
		{
			name:     "less by id",
			snapTerm: 1,
			snapIdx:  10,
			snapID:   "snap-a",
			metaTerm: 1,
			metaIdx:  10,
			metaID:   "snap-b",
			expected: true,
		},
		{
			name:     "greater by id",
			snapTerm: 1,
			snapIdx:  10,
			snapID:   "snap-z",
			metaTerm: 1,
			metaIdx:  10,
			metaID:   "snap-b",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := &Snapshot{
				id: tt.snapID,
				raftMeta: &raft.SnapshotMeta{
					Term:  tt.snapTerm,
					Index: tt.snapIdx,
				},
			}
			meta := &raft.SnapshotMeta{
				Term:  tt.metaTerm,
				Index: tt.metaIdx,
				ID:    tt.metaID,
			}

			result := snap.LessThanMeta(meta)
			if result != tt.expected {
				t.Errorf("LessThanMeta() = %v, want %v", result, tt.expected)
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

// Test SnapshotCatalog.Scan method
func TestSnapshotCatalog_Scan(t *testing.T) {
	t.Run("empty directory", func(t *testing.T) {
		tmpDir := t.TempDir()

		catalog := &SnapshotCatalog{}
		ss, err := catalog.Scan(tmpDir)
		if err != nil {
			t.Fatalf("Scan() returned error for empty directory: %v", err)
		}
		if ss.Len() != 0 {
			t.Errorf("Scan() returned %d snapshots, want 0", ss.Len())
		}
	})

	t.Run("single snapshot", func(t *testing.T) {
		tmpDir := t.TempDir()
		createTestSnapshot(t, tmpDir, "snapshot-1-2-100", 1, 2)

		catalog := &SnapshotCatalog{}
		ss, err := catalog.Scan(tmpDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 1 {
			t.Fatalf("Scan() returned %d snapshots, want 1", ss.Len())
		}

		oldest, ok := ss.Oldest()
		if !ok {
			t.Fatal("Oldest() returned false")
		}
		if oldest.id != "snapshot-1-2-100" {
			t.Errorf("snapshot id = %q, want %q", oldest.id, "snapshot-1-2-100")
		}
		if oldest.raftMeta.Term != 1 {
			t.Errorf("snapshot term = %d, want 1", oldest.raftMeta.Term)
		}
		if oldest.raftMeta.Index != 2 {
			t.Errorf("snapshot index = %d, want 2", oldest.raftMeta.Index)
		}

		snewest, ok := ss.Newest()
		if !ok {
			t.Fatal("Newest() returned false")
		}
		if snewest != oldest {
			t.Error("Newest() snapshot does not match Oldest() snapshot in single-snapshot set")
		}
	})

	t.Run("multiple snapshots ordered correctly", func(t *testing.T) {
		tmpDir := t.TempDir()
		createTestSnapshot(t, tmpDir, "snapshot-1-10-100", 1, 10)
		createTestSnapshot(t, tmpDir, "snapshot-2-5-200", 2, 5)
		createTestSnapshot(t, tmpDir, "snapshot-1-5-300", 1, 5)

		catalog := &SnapshotCatalog{}
		ss, err := catalog.Scan(tmpDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 3 {
			t.Fatalf("Scan() returned %d snapshots, want 3", ss.Len())
		}

		items := ss.All()
		if items[0].raftMeta.Term != 1 || items[0].raftMeta.Index != 5 {
			t.Errorf("first snapshot = term %d, index %d, want term 1, index 5",
				items[0].raftMeta.Term, items[0].raftMeta.Index)
		}
		if items[1].raftMeta.Term != 1 || items[1].raftMeta.Index != 10 {
			t.Errorf("second snapshot = term %d, index %d, want term 1, index 10",
				items[1].raftMeta.Term, items[1].raftMeta.Index)
		}
		if items[2].raftMeta.Term != 2 || items[2].raftMeta.Index != 5 {
			t.Errorf("third snapshot = term %d, index %d, want term 2, index 5",
				items[2].raftMeta.Term, items[2].raftMeta.Index)
		}

		// test oldest and newest
		oldest, ok := ss.Oldest()
		if !ok {
			t.Fatal("Oldest() returned false")
		}
		if oldest.raftMeta.Term != 1 || oldest.raftMeta.Index != 5 {
			t.Errorf("Oldest() = term %d, index %d, want term 1, index 5",
				oldest.raftMeta.Term, oldest.raftMeta.Index)
		}

		newest, ok := ss.Newest()
		if !ok {
			t.Fatal("Newest() returned false")
		}
		if newest.raftMeta.Term != 2 || newest.raftMeta.Index != 5 {
			t.Errorf("Newest() = term %d, index %d, want term 2, index 5",
				newest.raftMeta.Term, newest.raftMeta.Index)
		}
	})

	t.Run("ignores regular files", func(t *testing.T) {
		tmpDir := t.TempDir()
		createTestSnapshot(t, tmpDir, "snapshot-1-2-100", 1, 2)
		createTestFile(t, tmpDir, "regular-file.txt", "some content")

		catalog := &SnapshotCatalog{}
		ss, err := catalog.Scan(tmpDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 1 {
			t.Errorf("Scan() returned %d snapshots, want 1", ss.Len())
		}
	})

	t.Run("ignores tmp directories", func(t *testing.T) {
		tmpDir := t.TempDir()
		createTestSnapshot(t, tmpDir, "snapshot-1-2-100", 1, 2)
		createTestSnapshot(t, tmpDir, "snapshot-temp.tmp", 1, 3)

		catalog := &SnapshotCatalog{}
		ss, err := catalog.Scan(tmpDir)
		if err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		if ss.Len() != 1 {
			t.Errorf("Scan() returned %d snapshots, want 1 (tmp dir should be ignored)", ss.Len())
		}
	})

	t.Run("error on missing meta.json", func(t *testing.T) {
		tmpDir := t.TempDir()
		createTestSnapshotDir(t, tmpDir, "snapshot-1-2-100")

		catalog := &SnapshotCatalog{}
		_, err := catalog.Scan(tmpDir)
		if err == nil {
			t.Fatal("Scan() should return error for missing meta.json")
		}
	})

	t.Run("error on malformed meta.json", func(t *testing.T) {
		tmpDir := t.TempDir()
		createTestSnapshotDir(t, tmpDir, "snapshot-1-2-100")
		createTestFile(t, tmpDir, "snapshot-1-2-100/meta.json", "invalid json {")

		catalog := &SnapshotCatalog{}
		_, err := catalog.Scan(tmpDir)
		if err == nil {
			t.Fatal("Scan() should return error for malformed meta.json")
		}
	})

	t.Run("error on non-existent directory", func(t *testing.T) {
		catalog := &SnapshotCatalog{}
		_, err := catalog.Scan("/non/existent/directory")
		if err == nil {
			t.Fatal("Scan() should return error for non-existent directory")
		}
	})
}
