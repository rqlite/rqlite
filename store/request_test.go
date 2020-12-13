package store

import (
	"fmt"
	"testing"
)

func Test_NewQueryNonCompressed(t *testing.T) {
	qr := NewQueryRequest2()

	qr.SetTimings(true)
	qr.SetTransaction(true)
	if c, err := qr.SetSQL([]string{"SELECT * FROM foo"}); err != nil {
		t.Fatalf("Failed to set SQL: %s", err)
	} else if c {
		t.Fatalf("Request was unexpectedly compressed")
	}
}

func Test_NewQueryCompressed(t *testing.T) {
	qr := NewQueryRequest2()

	sql := []string{
		"SELECT * FROM foo WHERE x=y",
		"SELECT * FROM foo WHERE h-j",
		"SELECT * FROM foo",
		"SELECT * FROM bar",
		"SELECT * FROM foo",
	}

	qr.SetTimings(true)
	qr.SetTransaction(true)
	if c, err := qr.SetSQL(sql); err != nil {
		t.Fatalf("Failed to set SQL: %s", err)
	} else if !c {
		t.Fatalf("Request was unexpectedly not compressed")
	}

	b, err := qr.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal compressed request")
	}
	fmt.Println(len(b))
}
