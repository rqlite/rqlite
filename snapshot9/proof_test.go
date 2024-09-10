package snapshot9

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func Test_NewProof(t *testing.T) {
	size := int64(1024)
	lastModifiedTime := time.Now()

	proof := NewProof(size, lastModifiedTime)

	if proof.SizeBytes != size {
		t.Errorf("expected SizeBytes to be %d, got %d", size, proof.SizeBytes)
	}

	if !proof.LastModifiedTime.Equal(lastModifiedTime) {
		t.Errorf("expected LastModifiedTime to be %v, got %v", lastModifiedTime, proof.LastModifiedTime)
	}
}

func Test_NewProofFromFile(t *testing.T) {
	content := []byte("test data")
	tmpfile, err := os.CreateTemp("", "testfile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // clean up

	if _, err := tmpfile.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	proof, err := NewProofFromFile(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}

	expectedSize := int64(len(content))
	fileInfo, _ := os.Stat(tmpfile.Name())

	if proof.SizeBytes != expectedSize {
		t.Errorf("expected SizeBytes to be %d, got %d", expectedSize, proof.SizeBytes)
	}

	if !proof.LastModifiedTime.Equal(fileInfo.ModTime()) {
		t.Errorf("expected LastModifiedTime to be %v, got %v", fileInfo.ModTime(), proof.LastModifiedTime)
	}
}

func Test_ProofEquals(t *testing.T) {
	size := int64(1024)
	lastModifiedTime := time.Now()

	proof1 := NewProof(size, lastModifiedTime)
	proof2 := NewProof(size, lastModifiedTime)

	if !proof1.Equals(proof2) {
		t.Error("expected proofs to be equal")
	}

	proof3 := NewProof(size, lastModifiedTime.Add(time.Second))
	if proof1.Equals(proof3) {
		t.Error("expected proofs to be not equal due to different LastModifiedTime")
	}
}

func Test_ProofMarshal(t *testing.T) {
	size := int64(1024)
	lastModifiedTime := time.Now()

	proof := NewProof(size, lastModifiedTime)
	data, err := proof.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	var unmarshaledProof Proof
	err = json.Unmarshal(data, &unmarshaledProof)
	if err != nil {
		t.Fatal(err)
	}

	if !proof.Equals(&unmarshaledProof) {
		t.Error("expected unmarshaled proof to be equal to the original")
	}
}

func Test_UnmarshalProof(t *testing.T) {
	size := int64(1024)
	lastModifiedTime := time.Now()

	proof := NewProof(size, lastModifiedTime)
	data, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}

	unmarshaledProof, err := UnmarshalProof(data)
	if err != nil {
		t.Fatal(err)
	}

	if !proof.Equals(unmarshaledProof) {
		t.Error("expected unmarshaled proof to be equal to the original")
	}
}
