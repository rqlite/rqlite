package snapshot9

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

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

// func Test_NewSinkOpenCloseFail(t *testing.T) {
// 	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
// 	if sink == nil {
// 		t.Fatalf("Failed to create new sink")
// 	}
// 	if err := sink.Open(); err != nil {
// 		t.Fatalf("Failed to open sink: %v", err)
// 	}
// 	if err := sink.Close(); err == nil {
// 		t.Fatalf("Expected error closing opened sink without data")
// 	}
// }

// Test_SinkWriteReferentialSnapshot tests that writing a referential snapshot
// -- a snapshot without any accompanying SQLite data -- works as expected. Since
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

	proof := NewProof(100, 1000, 1234)
	_, err := proof.Write(sink)
	if err != nil {
		t.Fatalf("Failed to write proof: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Test that the Proof file was written correctly.
	proof2, err := readProof(filepath.Join(sink.str.Dir(), "snap-1234"))
	if err != nil {
		t.Fatalf("Failed to read proof: %v", err)
	}
	if proof.SizeBytes != proof2.SizeBytes || proof.UnixMilli != proof2.UnixMilli || proof.CRC32 != proof2.CRC32 {
		t.Fatalf("Proofs do not match: %v != %v", proof, proof2)
	}

	// There should be no data file since we wrote a Referential snapshot.
	if fileExists(filepath.Join(sink.str.Dir(), "snap-1234", dataFileName)) {
		t.Fatalf("Unexpected data file")
	}
}

// Test_SinkWriteReferentialSnapshot_Incremental writes a referential snapshot
// in bits and pieces, to check the partial read logic inside the Sink.
func Test_SinkWriteReferentialSnapshot_Incremental(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}

	proof := NewProof(100, 1000, 1234)
	pb, err := json.Marshal(proof)
	if err != nil {
		t.Fatalf("Failed to marshal proof: %v", err)
	}
	if err := binary.Write(sink, binary.LittleEndian, uint64(len(pb))); err != nil {
		t.Fatalf("Failed to write proof length: %v", err)
	}
	if _, err := sink.Write(pb[:2]); err != nil {
		t.Fatalf("Failed to write proof: %v", err)
	}
	if _, err := sink.Write(pb[2:]); err != nil {
		t.Fatalf("Failed to write proof: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Test that the Proof file was written correctly.
	proof2, err := readProof(filepath.Join(sink.str.Dir(), "snap-1234"))
	if err != nil {
		t.Fatalf("Failed to read proof: %v", err)
	}
	if proof.SizeBytes != proof2.SizeBytes || proof.UnixMilli != proof2.UnixMilli || proof.CRC32 != proof2.CRC32 {
		t.Fatalf("Proofs do not match: %v != %v", proof, proof2)
	}

	// There should be no data file since we wrote a Referential snapshot.
	if fileExists(filepath.Join(sink.str.Dir(), "snap-1234", dataFileName)) {
		t.Fatalf("Unexpected data file")
	}
}

// Test_SinkWriteFullSnapshot tests that writing a full snapshot
// -- a snapshot with any accompanying SQLite data -- works as expected. Since
// Snapshotting is a critical operation, this test does more than just test the
// behaviour, it looks inside the sink.
func Test_SinkWriteFullSnapshot(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}

	proof := NewProof(100, 1000, 1234)
	_, err := proof.Write(sink)
	if err != nil {
		t.Fatalf("Failed to write proof: %v", err)
	}

	// Write some data to the sink, ensure it is stored correct.
	data := []byte("Hello, world!")
	if _, err := sink.Write(data); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Test that the Proof file was written correctly.
	proof2, err := readProof(filepath.Join(sink.str.Dir(), "snap-1234"))
	if err != nil {
		t.Fatalf("Failed to read proof: %v", err)
	}
	if proof.SizeBytes != proof2.SizeBytes || proof.UnixMilli != proof2.UnixMilli || proof.CRC32 != proof2.CRC32 {
		t.Fatalf("Proofs do not match: %v != %v", proof, proof2)
	}

	// There should be a data file since we wrote a Full snapshot.
	data2 := mustReadFile(t, filepath.Join(sink.str.Dir(), "snap-1234", dataFileName))
	if string(data) != string(data2) {
		t.Fatalf("Data does not match: %s != %s", data, data2)
	}
}

func Test_SinkWriteFullSnapshot_Incremental(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}

	proof := NewProof(100, 1000, 1234)
	pb, err := json.Marshal(proof)
	if err != nil {
		t.Fatalf("Failed to marshal proof: %v", err)
	}
	if err := binary.Write(sink, binary.LittleEndian, uint64(len(pb))); err != nil {
		t.Fatalf("Failed to write proof length: %v", err)
	}
	if _, err := sink.Write(pb[:2]); err != nil {
		t.Fatalf("Failed to write proof: %v", err)
	}
	if _, err := sink.Write(pb[2:]); err != nil {
		t.Fatalf("Failed to write proof: %v", err)
	}

	// Write some data to the sink in bits and pieces, ensure it is stored correctly.
	data := []byte("Hello, world!")
	if _, err := sink.Write(data[:1]); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if _, err := sink.Write(data[1:4]); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if _, err := sink.Write(data[4:]); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Test that the Proof file was written correctly.
	proof2, err := readProof(filepath.Join(sink.str.Dir(), "snap-1234"))
	if err != nil {
		t.Fatalf("Failed to read proof: %v", err)
	}
	if proof.SizeBytes != proof2.SizeBytes || proof.UnixMilli != proof2.UnixMilli || proof.CRC32 != proof2.CRC32 {
		t.Fatalf("Proofs do not match: %v != %v", proof, proof2)
	}

	// There should be a data file since we wrote a Full snapshot.
	data2 := mustReadFile(t, filepath.Join(sink.str.Dir(), "snap-1234", dataFileName))
	if string(data) != string(data2) {
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
