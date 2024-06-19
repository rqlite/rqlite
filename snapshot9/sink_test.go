package snapshot9

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func Test_NewSinkCancel(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if sink.ID() != "snap-1234" {
		t.Fatalf("Unexpected ID: %s", sink.ID())
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Failed to cancel unopened sink: %v", err)
	}
}

func Test_NewSinkClose(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if sink.ID() != "snap-1234" {
		t.Fatalf("Unexpected ID: %s", sink.ID())
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to cancel unopened sink: %v", err)
	}
}

func Test_NewSinkOpenCancel(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Failed to cancel opened sink: %v", err)
	}
}

// Test_SinkWriteReferentialSnapshot tests that writing a referential snapshot
// -- writing a Proof object instead of an actual SQLite object -- works. Since
// Snapshotting is a critical operation, this test does more than just test the
// behaviour, it looks inside the sink.
func Test_SinkWriteReferentialSnapshot(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}

	now := time.Now()
	proof := NewProof(100, now, 1234)
	pb, err := proof.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal proof: %v", err)
	}
	if _, err := sink.Write(pb); err != nil {
		t.Fatalf("Failed to write proof: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Test that the Proof file was written correctly.
	pb = mustReadFile(t, filepath.Join(sink.str.Dir(), "snap-1234", stateFileName))
	proof2, err := UnmarshalProof(pb)
	if err != nil {
		t.Fatalf("Failed to unmarshal proof: %v", err)
	}
	if !proof.Equals(proof2) {
		t.Fatalf("Proofs do not match: %v != %v", proof, proof2)
	}

	// Validate the meta file.
	meta, err := readMeta(filepath.Join(sink.str.Dir(), "snap-1234"))
	if err != nil {
		t.Fatalf("Failed to read meta: %v", err)
	}
	if meta.ID != "snap-1234" || meta.Index != 3 || meta.Term != 2 || meta.ConfigurationIndex != 1 {
		t.Fatalf("Meta does not match: %v", meta)
	}
}

// Test_SinkWriteDataSnapshot tests that writing a Snapshot that is just data
// (and not an actual Proof) works fine. In practise this covers the case where
// a SQLite file is written to the sink.
func Test_SinkWriteDataSnapshot(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}

	// Write some data to the sink, ensure it is stored correctly.
	data := []byte("Hello, world!")
	if _, err := sink.Write(data); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// There should be a data file since we wrote a Full snapshot.
	data2 := mustReadFile(t, filepath.Join(sink.str.Dir(), "snap-1234", stateFileName))
	if !bytes.Equal(data, data2) {
		t.Fatalf("Data does not match: %s != %s", data, data2)
	}
}

func mustStore(t *testing.T) *ReferentialStore {
	t.Helper()
	return NewReferentialStore(t.TempDir(), nil)
}

func makeRaftMeta(id string, index, term, cfgIndex uint64) *raft.SnapshotMeta {
	return &raft.SnapshotMeta{
		ID:                 id,
		Index:              index,
		Term:               term,
		Configuration:      makeTestConfiguration("1", "localhost:1"),
		ConfigurationIndex: cfgIndex,
		Version:            1,
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	return data
}

func mustFileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}
	return fi.Size()
}
