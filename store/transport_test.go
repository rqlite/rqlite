package store

import (
	"testing"
)

func Test_NewTransport(t *testing.T) {
	if NewTransport(nil) == nil {
		t.Fatal("failed to create new Transport")
	}
}
