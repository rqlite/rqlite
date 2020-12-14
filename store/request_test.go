package store

import (
	"reflect"
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

	b, err := r.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal request")
	}

	r, err = UnmarshalRequest(b)
	if err != nil {
		t.Fatalf("failed to unmarshal request")
	}
	if r.GetTimings() != true {
		t.Fatalf("timing not preserved")
	}
	if r.GetTransaction() != true {
		t.Fatalf("transaction not preserved")
	}
	if sql, err := r.GetSQL(); err != nil {
		t.Fatalf("failed to get SQL: %s", err)
	} else if !reflect.DeepEqual(sql, []string{"SELECT * FROM foo"}) {
		t.Fatalf("SQL not preserved")
	}
}

func Test_RequestCompressedSingleLargeSQL(t *testing.T) {
	r := NewRequest2()
	var sql string

	for i := 0; i < sqlCompressSize; i++ {
		sql = sql + "S"
	}

	if err := r.SetSQL([]string{sql}); err != nil {
		t.Fatalf("Failed to set SQL: %s", err)
	}
	if !r.Compressed() {
		t.Fatalf("Request was unexpectedly not compressed")
	}

	b, err := r.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal request")
	}

	r, err = UnmarshalRequest(b)
	if err != nil {
		t.Fatalf("failed to unmarshal request")
	}

	if rsql, err := r.GetSQL(); err != nil {
		t.Fatalf("failed to get SQL: %s", err)
	} else if !reflect.DeepEqual([]string{sql}, rsql) {
		t.Fatalf("SQL not preserved")
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

func Test_RequestCompressedBatch(t *testing.T) {
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
