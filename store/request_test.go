package store

import (
	"testing"
)

func Test_RequestNonCompressedSingle(t *testing.T) {
	r := NewRequest2()

	r.SetTimings(true)
	r.SetTransaction(true)
	if err := r.SetSQL([]string{"SELECT * FROM foo"}); err != nil {
		t.Fatalf("Failed to set SQL: %s", err)
	}
	if r.Compressed() {
		t.Fatalf("Request was unexpectedly compressed")
	}

	_, err := r.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal request")
	}
}

func Test_RequestNonCompressedMulti(t *testing.T) {
	r := NewRequest2()
	sql := make([]string, 0)
	for i := 0; i < batchCompressSize-1; i++ {
		sql = append(sql, "SELECT * FROM foo")
	}

	if err := r.SetSQL(sql); err != nil {
		t.Fatalf("Failed to set SQL: %s", err)
	}
	if r.Compressed() {
		t.Fatalf("Request was unexpectedly compressed")
	}

	_, err := r.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal request")
	}
}

func Test_RequestCompressed(t *testing.T) {
	r := NewRequest2()
	sql := make([]string, 0)
	for i := 0; i < batchCompressSize; i++ {
		sql = append(sql, "SELECT * FROM foo")
	}

	if err := r.SetSQL(sql); err != nil {
		t.Fatalf("Failed to set SQL: %s", err)
	}
	if !r.Compressed() {
		t.Fatalf("Request was unexpectedly not compressed")
	}

	_, err := r.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal request")
	}
}
