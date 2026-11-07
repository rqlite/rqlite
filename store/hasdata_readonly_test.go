package store

import "testing"

func Test_HasDataPreservesNewNode(t *testing.T) {
	dir := t.TempDir()
	if !IsNewNode(dir) {
		t.Fatal("empty directory should be a new node")
	}
	hasData, err := HasData(dir)
	if err != nil {
		t.Fatal(err)
	}
	if hasData {
		t.Fatal("empty directory should have no data")
	}
	if !IsNewNode(dir) {
		t.Fatal("checking for data created Raft state and changed the new-node status")
	}
}
