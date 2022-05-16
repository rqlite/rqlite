package queue

import (
	"testing"
	"time"
)

func Test_NewQueue(t *testing.T) {
	q := New(1, 1, 100*time.Millisecond, nil)
	if q == nil {
		t.Fatalf("failed to create new Queue")
	}
}
