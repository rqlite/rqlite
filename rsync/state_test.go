package rsync

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func Test_CloseOrTimeout_Closed(t *testing.T) {
	ch := make(chan struct{})
	close(ch)
	if err := CloseOrTimeout(ch, time.Second); err != nil {
		t.Fatal(err)
	}
}

func Test_CloseOrTimeout_Close(t *testing.T) {
	ch := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := CloseOrTimeout(ch, time.Second); err != nil {
			t.Fatal(err)
		}
	}()
	close(ch)
	wg.Wait()
}

func Test_CloseOrTimeout_Timeout(t *testing.T) {
	ch := make(chan struct{})
	if err := CloseOrTimeout(ch, 100*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected %v, got %v", ErrTimeout, err)
	}
}
