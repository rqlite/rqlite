package store

import (
	"testing"
)

func Test_NewTransport(t *testing.T) {
	if NewTransport(nil) == nil {
		t.Fatal("failed to create new Transport")
	}
}

func Test_NewNodeTransport(t *testing.T) {
	nt := NewNodeTransport(nil)
	if nt == nil {
		t.Fatal("failed to create new NodeTransport")
	}
	if err := nt.Close(); err != nil {
		t.Fatalf("failed to close NodeTransport: %s", err.Error())
	}
}
