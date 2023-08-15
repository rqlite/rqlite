package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

func Test_CreateNewStoreWithValidDirectory(t *testing.T) {
	tempDir := t.TempDir()

	s, err := NewStore(tempDir)
	if err != nil {
		t.Fatalf("Failed to initialize Store: %v", err)
	}
	if s.Dir() != tempDir {
		t.Fatalf("Store directory does not match expected value")
	}

	// Check if the generation directory has been created.
	firstGenPath := filepath.Join(s.Dir(), "generations")
	if _, err := os.Stat(firstGenPath); os.IsNotExist(err) {
		t.Fatalf("generations directory was not created")
	}

	if !s.FullNeeded() {
		t.Fatalf("expected full snapshot to be needed")
	}

	// Check generations
	generations, err := s.GetGenerations()
	if err != nil {
		t.Fatalf("failed to get generations: %v", err)
	}
	if !compareStringSlices(generations, []string{"0000000001"}) {
		t.Fatalf("unexpected generations, got: %v, want: %v", generations, []string{"0000000001"})
	}

	if currDir, err := s.GetCurrentGenerationDir(); err != nil {
		t.Fatalf("failed to get current generation dir: %v", err)
	} else if currDir != filepath.Join(s.Dir(), "generations", "0000000001") {
		t.Fatalf("unexpected current generation dir, got: %s, want: %s", currDir, filepath.Join(s.Dir(), "generations", "0000000001"))
	}

	if ng, err := s.GetNextGeneration(); err != nil {
		t.Fatalf("failed to get next generation: %v", err)
	} else if ng != "0000000002" {
		t.Fatalf("unexpected next generation, got: %s, want: %s", ng, "0000000002")
	}

	if ngd, err := s.GetNextGenerationDir(); err != nil {
		t.Fatalf("failed to get next generation dir: %v", err)
	} else if ngd != filepath.Join(s.Dir(), "generations", "0000000002") {
		t.Fatalf("unexpected next generation dir, got: %s, want: %s", ngd, filepath.Join(s.Dir(), "generations", "0000000002"))
	}

	// Check snapshots
	if snaps, err := s.List(); err != nil {
		t.Fatalf("failed to list snapshots: %v", err)
	} else if len(snaps) != 0 {
		t.Fatalf("unexpected number of snapshots, got: %d, want: %d", len(snaps), 0)
	}
}

func Test_StoreCreateSinkOK(t *testing.T) {
	tempDir := t.TempDir()

	s, err := NewStore(tempDir)
	if err != nil {
		t.Fatalf("Failed to initialize Store: %v", err)
	}
	sink, err := s.Create(1, 2, 3, makeTestConfiguration("id1", "localhost"), 4, nil)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	if sink == nil {
		t.Fatalf("Snapshot sink is nil")
	}
}

// write a function to compare two string slices
func compareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, val := range a {
		if val != b[i] {
			return false
		}
	}
	return true
}

func makeTestConfiguration(i, a string) raft.Configuration {
	return raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(i),
				Address: raft.ServerAddress(a),
			},
		},
	}
}
