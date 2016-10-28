package sql

import (
	"testing"
)

func Test_ScannerNew(t *testing.T) {
	s := NewScanner(nil)
	if s == nil {
		t.Fatalf("failed to create basic Scanner")
	}
}
